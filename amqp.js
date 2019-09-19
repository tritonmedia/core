/**
 * AMQP Library
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1.0
 */

'use strict'

const amqp = require('amqp-connection-manager')
const path = require('path')
const logger = require('pino')({
  name: path.basename(__filename)
})


const client = require('prom-client')

const required = () => {
  throw new Error('Missing required parameter to AMQP.')
}

const metrics = {
  up: new client.Gauge({
    name: 'rabbitmq_up',
    help: '1 if RabbitMQ connection is up, 0 if down'
  }),
  messages_published: new client.Counter({
    name: 'rabbitmq_messages_published',
    help: 'Total number of rabbitmq messages published in this processes lifetime',
    labelNames: ['queue', 'exchange']
  }),
  messages_published_errored: new client.Counter({
    name: 'rabbitmq_messages_published_errored',
    help: 'Total number of rabbitmq messages that failed to be published in this processes lifetime',
    labelNames: ['queue', 'exchange']
  }),
  messages_unacked_ram: new client.Gauge({
    name: 'rabbitmq_messages_unacked_ram',
    help: 'Current number of unacked messages',
    labelNames: ['queue', 'exchange']
  }),
  messages_consumed: new client.Counter({
    name: 'rabbitmq_messages_consumed',
    help: 'Total number of rabbitmq messages consumed in this processes lifetime',
    labelNames: ['queue', 'exchange']
  })
}

class AMQP {
  /**
   * 
   * @param {String} host host to connect too
   * @param {Number} [prefetch=1000] global prefetch
   * @param {Number} [numConsumerQueues=2] number of consumer queues to listen / publish on
   * @param {client} prom DEPRECATED: prometheus client
   */
  constructor (host = required(), prefetch = 1000, numConsumerQueues = 2, prom) {
    this.host = host
    this.mode = null

    // queue creation options
    // TODO: make configurable
    this.numConsumerQueues = numConsumerQueues
    this.prefetch = prefetch

    /**
     * @type {Map<string,Number>}
     */
    this._topicState = new Map()

    // connection and channels in use
    this.connection = null

    // publisher channel
    this.pchannel = null

    // generic channel for anything but publishing
    this.channel = null


    this.prom = prom
    if (!this.prom) throw new Error('Missing prometheus client')
  }

  async connect () {
    return new Promise(async (resolve, reject) => {
      this.connection = await amqp.connect({
        protocol: 'amqp',
        hostname: this.host,
        username: process.env.RABBITMQ_USERNAME,
        password: process.env.RABBITMQ_PASSWORD,
      })
      this.connection.on('disconnect', err => {
        metrics.up.set(0)
        logger.warn('disconnected to rabbitmq', err)
      })
      this.connection.on('connect', () => {
        metrics.up.set(1)
        logger.info('connected to rabbitmq')
        resolve()
      })
    })
  }

  /**
   * Ensure that an exchange exists.
   *
   * @param {amqp.ChannelWrapper} channel amqplib channel (std)
   * @param {String} name name of the exchange
   * @param {Object} options options for the exchange, see amqp.assertExchange
   * @see http://www.squaremobius.net/amqp.node/channel_api.html#channelassertexchange
   */
  async _ensureExchange (channel = required(), name = required(), options = {}) {
    logger.info('ensuring exchange', name)
    return channel.assertExchange(name, 'direct', options)
  }

  /**
   * Ensure that consumer queues exist.
   *
   * @param {amqp.ChannelWrapper} channel amqplib channel (std)
   * @param {String} exchangeName exchange to bind too
   * @see http://www.squaremobius.net/amqp.node/channel_api.html#channelassertqueue
   * @see http://www.squaremobius.net/amqp.node/channel_api.html#channelbindqueue
   */
  async _ensureConsumerQueues (channel = required(), exchangeName = required()) {
    for (let i = 0; i !== this.numConsumerQueues; i++) {
      const queueName = `${exchangeName}-${i}`
      logger.info('creating consumer queue', queueName)

      try {
        await channel.assertQueue(queueName)
      } catch (err) {
        logger.error('failed to create queue', queueName, err.stack)
      }

      // bind the queue to the exchange based on the queueName
      logger.info(`binding queue '${queueName}' to exchange '${exchangeName}'`)
      try {
        await channel.bindQueue(queueName, exchangeName, queueName)
      } catch (err) {
        logger.error('failed to bind:', err.stack)
      }
    }
  }

  /**
   * Listen on for a routing key
   *
   * @param {String} topic topic of the message
   * @param {Function} processor function that processes new jobs
   * @returns {Promise} never RESOLVES
   * @see http://www.squaremobius.net/amqp.node/channel_api.html#channelconsume
   */
  async listen (topic = required(), processor = required()) {
    if (this.mode && this.mode === 'publisher') throw new Error('Already marked as a publisher.')

    const channelWrapper = this.connection.createChannel({
      json: false,
      setup: async channel => {
        logger.info('executing channel setup')
        await this._ensureExchange(channel, topic, {})
        await this._ensureConsumerQueues(channel, topic)

        // TODO: make configurable
        channel.prefetch(this.prefetch, true)

        for (let i = 0; i !== this.numConsumerQueues; i++) {
          const queueName = `${topic}-${i}`

          logger.info('subscribing to queue', queueName)
          channel.consume(queueName, msg => {
            const labels = {
              queue: queueName,
              exchange: topic,
            }

            try {
              metrics.messages_consumed.inc(labels)
              metrics.messages_unacked_ram.inc(labels)
              processor({
                message: msg,
                ack: () => {
                  metrics.messages_unacked_ram.dec(labels)
                  channelWrapper.ack(msg)
                },
                nack: () => {
                  metrics.messages_unacked_ram.dec(labels)
                  channelWrapper.nack(msg)
                },
              })
            } catch (err) {
              logger.error('processor failed', err.message)
            }
          })
        }
      }
    })
  }

  /**
   * Publish a message. Marks this class as being in publish mode.
   *
   * @param {String} topic topic of the message
   * @param {Buffer} body body of the message
   */
  async publish (topic = required(), body = required()) {
    if (this.mode && this.mode === 'consumer') throw new Error('Already marked as a consumer.')

    const generateChannel = async () => {
      if (!this.pchannel) {
        logger.warn('generating channel')
        this.pchannel = await this.connection.createChannel({
          json: false,
          setup: async channel => {
            this._ensureConsumerQueues(channel, topic)
            return this._ensureExchange(channel, topic, {})
          }
        })

        await this.pchannel.waitForConnect()
        logger.info('channel is ready')
      }

      return this.pchannel
    }
    const channel = await generateChannel()

    // get the last routing key index we used for this topic
    let rkIndex = this._topicState.get(topic)
    if (rkIndex === undefined) {
      this._topicState.set(topic, 0)
      rkIndex = 0
    }

    // format the routin key
    const rk = `${topic}-${rkIndex}`

    logger.info('publishing to exchange', topic, 'using rk', rk)

    // for metrics
    const labels = {
      queue: rk,
      exchange: topic,
    }

    try {
      await channel.publish(topic, rk, body, {
        persistent: true
      })
    } catch (err) {
      logger.error('failed to publish', err)
      metrics.messages_published_errored.inc(labels)
    }

    metrics.messages_published.inc(labels)

    // reset if we're at the max num of consumer queues
    if (rkIndex === this.numConsumerQueues - 1) {
      rkIndex = 0
    } else {
      rkIndex++
    }

    this._topicState.set(topic, rkIndex)
  }

  async cancel () {
    if (this.mode !== 'consumer') return
    this.connection._channel.cancel()
  }

  async close () {
    this.connection.close()
    if (this.pchannel) this.pchannel.close()
  }
}

module.exports = AMQP

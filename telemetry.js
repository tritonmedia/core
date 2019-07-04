/**
 * Telemetry Library (basically a wrapper around amqp.js)
 * 
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

'use strict'

const AMQP = require('./amqp')
const proto = require('./proto')
const uuid = require('uuid/v4')

const required = () => {
  throw new Error('Missing required argument.')
}

module.exports = class Telemetry {
  constructor (amqpHost) {
    this.publisher = new AMQP(amqpHost)
  }

  async connect () {
    this.telemetryStatusProto = await proto.load('api.TelemetryStatus')
    await this.publisher.connect()
  }

  /**
   * Emit a Status Update
   *
   * @param {String} mediaId id of the media
   * @param {Number} status status number
   * @returns {Object} telemetry status update
   */
  async emitStatus (mediaId = required(), status = required()) {
    const payload = {
      id: uuid(),
      mediaId,
      status,
    }

    let encoded;
    try {
      encoded = proto.encode(this.telemetryStatusProto, payload)
    } catch (err) {
      logger.warn('Failed to serialize telemetry status update:', err.message || err)
    }

    try {
      await this.publisher.publish('v1.telemetry.status', encoded)
    } catch (err) {
      logger.warn('failed to publish telemetry status:', err.message || err)
    }

    return payload
  }
}
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

    try {
      const encoded = proto.encode(this.telemetryStatusProto, payload)
    } catch (err) {
      throw new Error('Failed to serialize telemetry status update:', err.message || err)
    }

    await this.publisher.publish('v1.telemetry.status', encoded)
    return payload
  }
}
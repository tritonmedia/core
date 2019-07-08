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
const path = require('path')
const os = require('os')
const logger = require('pino')({
  name: path.basename(__filename)
})

const required = () => {
  throw new Error('Missing required argument.')
}

module.exports = class Telemetry {
  constructor (amqpHost) {
    this.statusPublisher = new AMQP(amqpHost)
    this.progressPublisher = new AMQP(amqpHost)
  }

  async connect () {
    this.telemetryStatusProto = await proto.load('api.TelemetryStatus')
    this.telemetryProgressProto = await proto.load('api.TelemetryProgress')

    await this.statusPublisher.connect()
    await this.progressPublisher.connect()
  }

  /**
   * Emit a status update
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
      host: os.hostname() || 'unknown'
    }

    let encoded;
    try {
      encoded = proto.encode(this.telemetryStatusProto, payload)
    } catch (err) {
      logger.warn('Failed to serialize telemetry status update:', err.message || err)
    }

    try {
      await this.statusPublisher.publish('v1.telemetry.status', encoded)
    } catch (err) {
      logger.warn('failed to publish telemetry status:', err.message || err)
    }

    return payload
  }

  /**
   * Emit a progress update
   *
   * @param {String} mediaId id of the media
   * @param {Number} status status number
   * @param {Number} progress 0 - 100 progress status
   */
  async emitProgress (mediaId = required(), status = required(), progress = required()) {
    const payload = {
      id: uuid(),
      mediaId,
      status,
      progress,
      host: os.hostname() || 'unknown',
    }

    let encoded;
    try {
      encoded = proto.encode(this.telemetryProgressProto, payload)
    } catch (err) {
      logger.warn('Failed to serialize telemetry progress update:', err.message || err)
    }

    try {
      await this.progressPublisher.publish('v1.telemetry.progress', encoded)
    } catch (err) {
      logger.warn('failed to publish telemetry progress:', err.message || err)
    }

    return payload
  }
}
/**
 * Tracing Helper
 * 
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const tracer = require('jaeger-client').initTracer
const opentracing = require('opentracing')

const initTracer = function(serviceName, logger) {
  const config = {
    serviceName: serviceName,
    sampler: { 'type': 'const', 'param': 1 },
    reporter: { 'logSpans': true }
  };
  const options = {
    logger,
    tags: {
      version: '1.0.0'
    }
  };

  // save for later
  this._tracer = tracer(config, options)
  return this._tracer
}

module.exports = {
  /**
   * @type {opentracing.Tracer}
   */
  _tracer: null,
  initTracer: initTracer,
  /**
   * @type opentracing
   */
  opentracing,

  /**
   * Tags contains a object mapping for common terminology to their
   * relevant opentracing tags.
   */
  Tags: {
    CARD_ID: 'card.id',
    LIST_ID: 'list.id',
    MEDIA_TYPE: 'media.type',
    /**
     * Download protocol is the download protocol, http, torrent, etc
     * @type string
     */
    DOWNLOAD_PROTOCOL: 'download.protocol',

  },

  /**
   * Error handles a generic Error object and attaches it to a span.
   * @param {opentracing.Span} span - span object
   * @param {Error} err - error object
   */
  error: (span, err) => {
    span.setTag(opentracing.Tags.ERROR, true)
    span.log({'event': 'error', 'error.object': err, 'message': err.message, 'stack': err.stack})
    span.finish()
  },

  /**
   * Serialize dumps opentracing context into an object
   * @param {opentracing.Span} span - span to serialize
   * @returns {Object} kv
   */
  serialize: function(span) {
    const context = {}
    this._tracer.inject(span, opentracing.FORMAT_TEXT_MAP, context)
    return context
  },

  /**
   * Unserialize deserializes trace data back into meaningful data
   * @param {String|Object} unknown - serialized map (text or object)
   */
  unserialize: function(unknown) {
    let data
    if (typeof unknown === 'string') {
      data = JSON.parse(unknown)
    } else {
      data = unknown
    }
    return this._tracer.extract(opentracing.FORMAT_TEXT_MAP, data)
  },

  /**
   * SerializeHTTP dumps opentracing context into an object 
   * suitable for use in HTTP headers
   * @param {opentracing.Span} span - span to serialize
   * @returns {Object} kv
   */
  serializeHTTP: function(span) {
    const context = {}
    this._tracer.inject(span, opentracing.FORMAT_HTTP_HEADERS, context)
    return context
  }
}

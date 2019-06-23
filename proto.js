/**
 * Load Protobufs
 * 
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version
 */

const protobuf = require('protobufjs')
const path = require('path')

module.exports = {
  /**
   * Load a protobuf package
   * @param {String} package name of the protobuf package to load
   * @param {Number} version version of the api to load
   * @returns {protobuf.Type}
   */
  load: async (package, version = 1) => {
    const root = await protobuf.load(path.join(__dirname, `./protos/api/v${version}.proto`))
    const convert = root.lookupType(package)
    return convert
  },

  /**
   * Encode a object payload into a form suitable for sending
   * over the wire.
   * 
   * @param {protobuf.Type} type protobuf type
   * @param {Object} payload payload to send
   * @returns {Uint8Array} protobuf format
   */
  encode: (type, payload) => {
    const err = type.verify(payload)
    if (err) throw err

    const msg = type.create(payload)
    return type.encode(msg).finish()
  },

  /**
   * Decode a protobuf from over the wire
   * 
   * @param {protobuf.Type} type protobuf type
   * @param {Buffer} buf buf to decode
   * @returns {Object} type object
   */
  decode: (type, buf) => {
    const encMsg = type.decode(buf)
    return type.toObject(encMsg, {
      enums: Number,
      bytes: String,
      longs: String,
    })
  }
}
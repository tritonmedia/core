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
   * @param {protobuf.IConversionOptions} opts conversion options
   * @returns {Object} type object
   */
  decode: (type, buf, opts) => {
    const encMsg = type.decode(buf)
    return type.toObject(encMsg, opts || {
      enums: Number,
      bytes: String,
      longs: String,
    })
  },

  /**
   * Convert a string to the equiv enum
   * @param {protobuf.Type} type protobuf type
   * @param {String} en enum to look at
   * @param {String} str string name
   * @returns {Number} numeric representation of the enum
   */
  stringToEnum: (type, en, str) => {
    const e = type.lookupEnum(en)
    const v = e.values[str]
    if (v === undefined) {
      throw new Error(`Failed to find string '${str}'`)
    }
    return v
  },

  /**
   * Convert a number to the equiv enum string
   * @param {protobuf.Type} type protobuf type
   * @param {String} key key of the protobuf
   * @param {Number} value value of it
   * @returns {String} string representation of the enum
   */
  enumToString: (type, key, value) => {
    const e = type.lookupEnum(key)
    const v = e.valuesById[value]
    if (v === undefined) {
      throw new Error(`Key '${key}' not found.`)
    }
    return v
  },

  /**
   * Return the possible string values of an enum
   * @param {protobuf.Type} type protobuf type
   * @param {String} key key of the protobuf
   * @returns {String[]}
   */
  enumValues: (type, key) => {
    const e = type.lookupEnum(key)
    return Object.keys(e.values)
  }
}
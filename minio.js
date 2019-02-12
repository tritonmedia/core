/**
 * Minio helper functions
 * 
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const Minio = require('minio')
const path = require('path')
const url = require('url')
const dyn = require('./dynamics')
const logger = require('pino')({
  name: "triton-core/"+path.basename(__filename)
})

module.exports = {
  /**
   * NewClient creates a new Minio client
   * 
   * @param {Object} config config object
   * @returns {Minio.Client}
   */
  newClient: async config => {
    const minioEndpoint = url.parse(dyn('minio'))
    logger.info(`minio client is using protocol='${minioEndpoint.protocol}',endpoint='${minioEndpoint.hostname}',port='${parseInt(minioEndpoint.port, 10)}',ssl='${minioEndpoint.protocol === 'https:'}'`)
    return new Minio.Client({
      endPoint: minioEndpoint.hostname,
      port: parseInt(minioEndpoint.port, 10),
      useSSL: minioEndpoint.protocol === 'https:',
      accessKey: config.keys.minio.accessKey,
      secretKey: config.keys.minio.secretKey
    })
  },

  /**
   * List all the objects in a bucket
   * @param {Minio.Client} s3Client s3client to use
   * @param {String} bucketId bucket id
   * @param {String} prefix optional prefix for bucket
   * @returns {Minio.BucketItem[]} list of objects
   */
  getObjects: async (s3Client, bucketId, prefix = '') => {
    return new Promise((resolve, reject) => {
      logger.info(`getObjects(): listing bucket '${bucketId}'`)
      let errored = false
      const objects = []
      const stream = s3Client.listObjects(bucketId, '', true)
      stream.on('data', obj => {
        console.log(obj)
        if (!obj.name) return // skip dir
        const p = path.parse(obj.name)
        if (p.dir !== prefix) {
          logger.info(`skipping '${obj.name}' due to incorrect dir (${p.dir} != ${prefix})`)
          return
        }

        logger.info(`getObjects(): push '${obj.name}'`)
        objects.push(obj)
      })
      stream.on('end', () => {
        logger.info('getObjects(): end')
        if (errored) return
        return resolve(objects)
      })
      stream.on('error', err => {
        errored = true
        console.log(err)
        return reject(err)
      })
    })
  },

  /**
   * Cleanup recursively cleans all items in a bucket
   * @param {Minio.Client} s3Client the s3client to use
   * @param {String} bucketId - the bucket to clean
   * @param {prefix} prefix - prefix to use
   */
  cleanupBucket: async (s3Client, bucketId, prefix = '') => {
    logger.info('cleaning up bucket', bucketId)
    return new Promise((resolve, reject) => {
      const objectNames = []
      const stream = s3Client.listObjects(bucketId, prefix, true)
      stream.on('data', async obj => {
        objectNames.push(obj.name)
      })
      stream.on('error', err => {
        return reject(err)
      })
      stream.on('end', async () => {
        try {
          await s3Client.removeObjects(bucketId, objectNames)
        } catch (err) {
          return reject(err)
        }

        return resolve()
      })
    })
  }
}
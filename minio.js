/**
 * Minio helper functions
 * 
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const logger = require('pino')({
  name: "triton-core/"+path.basename(__filename)
})

 module.exports = {
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
      const stream = s3Client.listObjectsV2(bucketId, '', true)
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
        return reject(err)
      })
    })
  }
}
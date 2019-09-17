/**
 * Dynamic Environment Variables
 * 
 * @todo This is meant to be replaced by service discovery or something
 * in the future.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

module.exports = prop => {
  let response;
  switch(prop) {
    case 'redis':
      response = process.env.REDIS || 'redis://127.0.0.1:6379'
    break;

    case 'media':
      response = process.env.MEDIA || 'http://127.0.0.1:8001'
    break;

    case 'minio':
      response = process.env.S3_ENDPOINT || 'http://127.0.0.1:9000'
    break;

    case 'rabbitmq':
      response = process.env.RABBITMQ || 'amqp://user:bitnami@localhost'
    break;

    case 'postgres':
      response = process.env.POSTGRES || 'localhost'
    break;

    default:
      response = null;
    break;
  }

  return response
}

/**
 * Prometheus Wrapper
 * 
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const client = require('prom-client')
const http = require('http')
const path = require('path')
const logger = require('pino')({
  name: path.basename(__filename)
})

const Prom = {
  /**
   * Return the global prometheus object
   * @param {string} serviceName name of the service
   * @returns {client}
   */
  new: serviceName => {
    client.register.setDefaultLabels({
      serviceName
    })
    return client
  },

  /**
   * Expose exposes the prometheus metrics on 8000, unless
   * process.env.PROMETHEUS_PORT is set.
   */
  expose: () => {
    const requestHandler = (req, res) => {
      res.end(client.register.metrics())
    }
    
    const server = http.createServer(requestHandler)
    const port = process.env.PROMETHEUS_PORT || 8000
    server.listen(port, err => {
      if (err) return logger.error('failed to listen to expose prometheus metrics:', err.message || err)  
      logger.info('prometheus is listening on port', port)
    })
  }
}

module.exports = Prom
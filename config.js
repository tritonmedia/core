/**
 * Loads a config relevant to a specific service with the defaults.
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

'use strict';

const _    = require('lodash')
const fs   = require('fs-extra')
const path = require('path')
const yaml = require('js-yaml')
const logger = require('pino')({
  name: path.basename(__filename)
})

module.exports = async service => {
  const CONFIG_PATH   = process.env.CONFIG_PATH || path.join('/config', 'config.yaml')
  const config_exists = await fs.exists(CONFIG_PATH)
  const ENV           = process.env.NODE_ENV || process.env.CONFIG_ENV || 'debug'

  if(!config_exists) throw new Error('Config not found')

  // load the config
  const configContents = await fs.readFile(CONFIG_PATH)
  const configObject   = yaml.safeLoad(configContents)

  logger.info('environment', ENV)
  const config         = configObject[ENV]
  if(!config) throw new Error(`Config enviroment '${ENV}' not found.`)

  const service_config = {
    keys: config.keys,
    instance: _.merge(config.instances.default, config.instances[service] || {})
  }

  return service_config
}

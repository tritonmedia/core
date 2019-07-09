/**
 * Database Interface
 *
 * @author Jared Allard <jaredallard@outlook.com>
 * @license MIT
 * @version 1
 */

const { Pool } = require('pg')
const proto = require('./proto')
const dyn = require('./dynamics')
const uuid = require('uuid/v4')
const _ = require('lodash')
const path = require('path')
const logger = require('pino')({
  name: path.basename(__filename)
})

const initStatement = `
CREATE TABLE media (
  id character varying(36) PRIMARY KEY,
  media_name text NOT NULL,
  creator smallint NOT NULL,
  creator_id text,
  type smallint NOT NULL,
  source smallint NOT NULL,
  source_uri text NOT NULL,
  metadata_id text NOT NULL,
  metadata smallint NOT NULL DEFAULT '0'::smallint,
  status smallint DEFAULT '0'::smallint,
  converter_status integer DEFAULT 0
);
CREATE TABLE tokens (
  token character varying(128) PRIMARY KEY,
  created_at timestamp with time zone DEFAULT now()
);
COMMENT ON COLUMN tokens.token IS 'API Token';
COMMENT ON COLUMN tokens.created_at IS 'When this token was created';
COMMENT ON TABLE media IS 'This table holds media information';
COMMENT ON COLUMN media.id IS 'ID of the media';
COMMENT ON COLUMN media.media_name IS 'Media name';
COMMENT ON COLUMN media.creator IS 'Creator Type, see protobuf for int to string';
COMMENT ON COLUMN media.creator_id IS 'Creator ID (for mapping back to creator), if needed';
COMMENT ON COLUMN media.type IS 'Media Type, see protobuf for int to string';
COMMENT ON COLUMN media.source IS 'Source Type, see protobuf for int to string ';
COMMENT ON COLUMN media.source_uri IS 'Source URL';
COMMENT ON COLUMN media.metadata_id IS 'Metadata ID';
COMMENT ON COLUMN media.metadata IS 'Metadata Type: 0 MAL, 1 IMDB';
COMMENT ON COLUMN media.status IS 'Status of the media file';
COMMENT ON COLUMN media.converter_status IS 'Converter Status is used by the converter to determine it''s position. This could be used for progress calculation.';
`

/**
 * Storage Adapter
 * @class Storage
 */
class Storage {
  constructor () {
    this.adapter = new Pool({
      host: dyn('postgres'),
      user: 'postgres',
      database: 'media'
    })
  }

  /**
   * Connects to the storage and initializes it if needed
   * @returns {Undefined}
   */
  async connect () {
    this.downloadProto = await proto.load('api.Download')

    try {
      await this.adapter.query('SELECT id FROM media LIMIT 1;')
    } catch (err) {
      logger.info('intializing postgres ...')
      try {
        await this.adapter.query(initStatement)
      } catch (err) {
        logger.error('failed to init postgress', err.message)
        // TODO: logging stuff for here
        throw err
      }
    }

    logger.info('postgres connected')
  }

  /**
   * Finds a media by it's metadata
   *
   * @param {String} id metadata id
   * @param {Number} type metadata type
   * @returns {String|Null} id of the item if found, otherwise null
   */
  async findByMetadata (id, type) {
    const res = await this.adapter.query(
      'SELECT id FROM media WHERE metadata = $1 AND metadata_id = $2;',
      [type, id])
    if (res.rows.length !== 1) return null

    return res.rows[0]
  }

  /**
   * Get the Status of a Media
   * @param {String} id id of the media
   * @returns {Number|null} status or null if not found
   */
  async getStatus (id) {
    const res = await this.adapter.query(
      'SELECT status FROM media WHERE id = $1',
      [id]
    )

    if (res.rows.length !== 1) return null
    return res.rows[0]
  }

  /**
   * Create a new media object, returning a protobuf for Download
   * and creating a DB object, ensuring no duplicates exist.
   *
   * @param {String} name name of the media
   * @param {Number} creator creator type
   * @param {String} creatorId creator id
   * @param {Number} type media type
   * @param {Number} source source type
   * @param {String} sourceURI source URL
   * @param {Number} metadata metadata type
   * @param {String} metadataId Metadata ID
   */
  async new (name, creator, creatorId, type, source, sourceURI, metadata, metadataId, ignoreExisting = false) {
    const id = uuid()
    const payload = {
      createdAt: new Date().toISOString(),
      media: {
        id,
        name,
        creator,
        creatorId,
        type,
        source,
        sourceURI,
        metadata,
        metadataId,

        // initial status is set to 0 (QUEUED)
        status: 0
      }
    }

    const existingId = await this.findByMetadata(metadataId, metadata)
    if (existingId !== null && !ignoreExisting) {
      throw new Error('Media already exists.')
    }

    if (existingId === null && ignoreExisting) { // update an existing one
      logger.info('marking id', existingId, 'as QUEUED status')
      // set to queued
      this.updateStatus(existingId, 0)
      payload.id = existingId
    } else { // create a new one
      try {
        logger.info('creating id', id)
        await this.adapter.query(
          'INSERT INTO "public"."media"("id", "media_name", "creator", "creator_id", "type", "source", "source_uri", "metadata_id", "metadata") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)',
          [
            id,
            name,
            creator,
            creatorId,
            type,
            source,
            sourceURI,
            metadataId,
            metadata
          ]
        )
      } catch (err) {
        logger.error('failed to insert', err.message)
        throw err
      }
    }

    return proto.encode(this.downloadProto, payload)
  }

  /**
   * Update converter Status
   * @param {String} mediaId id of the media to update
   * @param {Number} i new int value
   */
  async setConverterStatus (mediaId, i) {
    return this.adapter.query(
      'UPDATE "public"."media" SET "converter_status"=$1 WHERE "id"=$2',
      [
        i,
        mediaId
      ]
    )
  }

  /**
   * Get converter status
   * @param {String} mediaId id of the media
   * @returns {Number} position to start at
   */
  async getConverterStatus (mediaId) {
    const res = await this.adapter.query('SELECT converter_status FROM media where id = $1', [mediaId])
    if (res.rows.length === 0){
      const err = new Error('ID not found')
      err.code = 'ERRNOTFOUND'
      throw err
    }

    return res.rows[0].converter_status
  }

  /**
   * Insert a token
   *
   * @param {String} token token to insert
   */
  async insertToken (token) {
    return this.adapter.query(
      'INSERT INTO tokens("token") VALUES ($1)', [token]
    )
  }

  /**
   * Check if a token exists
   *
   * @param {String} token token value
   */
  async tokenExists (token) {
    const res = await this.adapter.query('SELECT token FROM tokens WHERE token = $1', [token])
    if (res.rows.length === 1) {
      return true
    }

    return false
  }

  /**
   * List all Tokens
   *
   * @returns {String[]} tokens
   */
  async listTokens () {
    const res = await this.adapter.query('SELECT token FROM tokens')
    return _.map(res.rows, item => {
      return item.id
    })
  }

  /**
   * Get a media object by ID (returns a valid v1.media object)
   * @param {String} id id of the media
   */
  async getByID (id) {
    const res = await this.adapter.query('SELECT * FROM media WHERE id = $1', [id])
    if (res.rows.length === 0) {
      const err = new Error('ID not found')
      err.code = 'ERRNOTFOUND'
      throw err
    }
    if (res.rows.length !== 1) throw new Error('Found multiple rows on single op')

    const row = res.rows[0]
    return {
      id: row.id,
      name: row.media_name,
      creator: row.creator,
      creatorId: row.creator_id,
      type: row.type,
      source: row.source,
      sourceURI: row.source_uri,
      metadata: row.metadata,
      metadataId: row.metadata_id,
      status: row.status
    }
  }

  /**
   * List media
   * @todo paginate
   * @returns {Array} media
   */
  async list () {
    const res = await this.adapter.query('SELECT * FROM media')
    if (res.rows.length === 0) return []

    return res.rows.map(row => {
      return {
        id: row.id,
        name: row.media_name,
        creator: row.creator,
        creatorId: row.creator_id,
        type: row.type,
        source: row.source,
        sourceURI: row.source_uri,
        metadata: row.metadata,
        metadataId: row.metadata_id,
        status: row.status
      }
    })
  }
  /**
   * Update the status of a media
   *
   * @param {String} id id of the media
   * @param {Number} status status to set media too
   * @returns {undefined} not used
   */
  async updateStatus (id, status) {
    try {
      await this.adapter.query(
        'UPDATE "public"."media" SET "status"=$1 WHERE "id"=$2',
        [
          status,
          id
        ]
      )
    } catch (err) {
      logger.error('failed to update media', err.message)
      throw err
    }
  }
}

module.exports = Storage

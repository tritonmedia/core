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
-- For fuzzy search
CREATE EXTENSION fuzzystrmatch;

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
      password: process.env.POSTGRES_PASSWORD,
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
        console.log(err)
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
   * List media
   * @todo paginate
   * @param {String} id of the media
   * @returns {Array} media
   */
  async listEpisodes (id) {
    const res = await this.adapter.query(`
    SELECT e.id, e.description, e.title, e.media_id, e.air_date, e.season, e.season_number, e.created_at, i.id AS thumbnail_image_id FROM episodes_v1 e 
    LEFT JOIN episode_images_v1 i ON e.id = i.episode_id WHERE media_id = $1;
    `, [id])
    if (res.rows.length === 0) return []

    return res.rows.map(row => {
      return {
        id: row.id,
        media_id: row.media_id,
        absolute_number: row.absolute_number,
        description: row.description,
        title: row.title,
        season: row.season,
        season_number: row.season_number,
        air_date: row.air_date,
        created_at: row.created_at,
        thumbnail_image_id: row.thumbnail_image_id
      }
    })
  }

  /**
   * Get Series by ID
   * @todo paginate
   * @param {String} id of the media
   * @returns {Array} media
   */
  async getSeries (id) {
    const res = await this.adapter.query('SELECT * FROM series_v1 WHERE id = $1', [id])
    if (res.rows.length === 0) return {}
    
    const row = res.rows[0]
    const images = await this.getSeriesImages(row.id)
    return {
      id: row.id,
      title: row.title,
      type: row.type,
      rating: row.rating,
      overview: row.overview,
      network: row.network,
      first_aired: row.first_aired,
      status: row.status,
      images,
      genres: row.genres,
      airs: row.airs,
      air_day_of_week: row.air_day_of_week,
      runtime: row.runtime,
      created_at: row.created_at
    }
  }

  /**
   * List Series
   * @todo paginate
   * @param {Number} type of the media,
   * @returns {Array} media
   */
  async listSeries (type) {
    const args = []

    // TODO(jaredallard): this is just sad
    let query = 'SELECT * FROM series_v1'
    if (type) {
      args.push(type)
      query += ' WHERE type = $1'
    }

    const res = await this.adapter.query(query, args)
    if (res.rows.length === 0) return []


    const promises = res.rows.map(async row => {
      const images = await this.getSeriesImages(row.id)
      return {
        id: row.id,
        title: row.title,
        type: row.type,
        rating: row.rating,
        overview: row.overview,
        network: row.network,
        first_aired: row.first_aired,
        status: row.status,
        images,
        genres: row.genres,
        airs: row.airs,
        air_day_of_week: row.air_day_of_week,
        runtime: row.runtime,
        created_at: row.created_at
      }
    })

    return Promise.all(promises)
  }

  /**
   * Get Series by ID
   * @todo paginate
   * @param {String} id of the media
   * @returns {Array} media
   */
  async getSeriesImages (id) {
    const res = await this.adapter.query('SELECT * FROM images_v1 WHERE media_id = $1', [id])
    if (res.rows.length === 0) return []

    return res.rows.map(row => {
      return {
        id: row.id,
        media_id: row.media_id,
        checksum: row.checksum,
        image_type: row.image_type,
        resolution: row.resolution,
        rating: row.rating,
        created_at: row.created_at
      }
    })
  }

  /**
   * Get Episode Files
   * @todo paginate
   * @param {String} id of the episode
   * @returns {Array} media
   */
  async getEpisodeFiles (id) {
    const res = await this.adapter.query('SELECT * FROM episode_files_v1 WHERE episode_id = $1', [id])
    if (res.rows.length === 0) return []

    return res.rows.map(row => {
      return {
        id: row.id,
        episode_id: row.episode_id,
        key: row.key,
        quality: row.quality,
        created_at: row.created_at
      }
    })
  }

  /**
   * Get Subtitles Files
   * @todo paginate
   * @param {String} id of the episode
   * @returns {Array} subtitles
   */
  async getSubtitles (id) {
    const res = await this.adapter.query('SELECT * FROM subtitles_v1 WHERE episode_id = $1', [id])
    if (res.rows.length === 0) return []

    return res.rows.map(row => {
      return {
        id: row.id,
        episode_id: row.episode_id,
        key: row.key,
        language: row.language,
        created_at: row.created_at
      }
    })
  }

  /**
   * Get a subtitle by episode ID 
   * @todo paginate
   * @param {String} episodeID of the episode
   * @returns {Object} subtitle
   */
  async getSubtitle (episodeID, subtitleID) {
    const res = await this.adapter.query('SELECT * FROM subtitles_v1 WHERE episode_id = $1 AND id = $2', [episodeID, subtitleID])
    if (res.rows.length === 0) return {}

    const row = res.rows[0]
    return {
      id: row.id,
      episode_id: row.episode_id,
      key: row.key,
      language: row.language,
      created_at: row.created_at
    }
  }

  /**
   * Fuzzy search for a series by name
   *
   * @param {String} name name of the media, fuzzy
   */
  async searchSeries (name) {
    const res = await this.adapter.query(`
      SELECT * 
      FROM series_v1
      WHERE levenshtein(title, $1) <= 3
      ORDER BY levenshtein(title, $1)
      LIMIT 10
    `, [name])
    if (res.rows.length === 0) return []

    return res.rows.map(row => {
      return {
        id: row.id,
        title: row.title,
        type: row.type,
        rating: row.rating,
        overview: row.overview,
        network: row.network,
        first_aired: row.first_aired,
        status: row.status,
        genres: row.genres,
        airs: row.airs,
        air_day_of_week: row.air_day_of_week,
        runtime: row.runtime,
        created_at: row.created_at
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

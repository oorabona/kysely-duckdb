import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'
import { SpatialFunctions } from '../../src/extensions/spatial.js'

describe('SpatialFunctions Extended', () => {
  let database: DuckDBInstance
  let db: Kysely<any>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    const dialect = new DuckDbDialect({ database })
    db = new Kysely({ dialect })

    // Install and load spatial extension - fail hard if not available
    await sql`INSTALL spatial`.execute(db)
    await sql`LOAD spatial`.execute(db)

    // Create test table with spatial data using raw SQL since geometry is a DuckDB-specific type
    await sql`
      CREATE TABLE spatial_test (
        id INTEGER PRIMARY KEY,
        location GEOMETRY,
        name VARCHAR(255)
      )
    `.execute(db)

    // Insert test data with various geometries
    await db
      .insertInto('spatial_test')
      .values([
        { id: 1, location: sql`ST_Point(0, 0)`, name: 'Origin' },
        { id: 2, location: sql`ST_Point(1, 1)`, name: 'Point A' },
        { id: 3, location: sql`ST_Point(3, 4)`, name: 'Point B' },
        { id: 4, location: sql`ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')`, name: 'Line' },
        {
          id: 5,
          location: sql`ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))')`,
          name: 'Square',
        },
      ])
      .execute()
  })

  afterEach(async () => {
    if (db) {
      await db.destroy()
    }
  })

  describe('Geometry Creation Functions', () => {
    it('should create points using stPoint', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([SpatialFunctions.stAsText(SpatialFunctions.stPoint(2, 3)).as('point_wkt')])
        .limit(1)
        .execute()

      const r0 = result[0]
      expect(r0?.point_wkt).toContain('POINT')
      expect(r0?.point_wkt).toContain('2')
      expect(r0?.point_wkt).toContain('3')
    })

    it('should create points with expression parameters', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          SpatialFunctions.stAsText(SpatialFunctions.stPoint(sql`id + 1`, sql`id + 2`)).as(
            'dynamic_point',
          ),
        ])
        .where('id', '=', 1)
        .execute()

      const r0 = result[0]
      expect(r0?.dynamic_point).toContain('POINT')
      expect(r0?.dynamic_point).toContain('2') // id + 1 = 2
      expect(r0?.dynamic_point).toContain('3') // id + 2 = 3
    })

    it('should create geometry from WKT using stGeomFromText', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          SpatialFunctions.stAsText(
            SpatialFunctions.stGeomFromText(sql.lit('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
          ).as('polygon_wkt'),
          SpatialFunctions.stGeometryType(
            SpatialFunctions.stGeomFromText(sql.lit('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
          ).as('geom_type'),
        ])
        .limit(1)
        .execute()

      const r0 = result[0]
      expect(r0?.polygon_wkt).toContain('POLYGON')
      expect(r0?.geom_type).toContain('POLYGON')
    })

    it('should create geometry from GeoJSON using stGeomFromGeoJSON', async () => {
      const geoJsonPoint = '{"type":"Point","coordinates":[1,2]}'

      const result = await db
        .selectFrom('spatial_test')
        .select([
          SpatialFunctions.stAsText(SpatialFunctions.stGeomFromGeoJSON(geoJsonPoint)).as(
            'point_from_geojson',
          ),
        ])
        .limit(1)
        .execute()

      const r0 = result[0]
      expect(r0?.point_from_geojson).toContain('POINT')
      expect(r0?.point_from_geojson).toContain('1')
      expect(r0?.point_from_geojson).toContain('2')
    })
  })

  describe('Geometry Property Functions', () => {
    it('should get geometry type using stGeometryType', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stGeometryType(sql.ref('location')).as('geom_type')])
        .execute()

      expect(result).toHaveLength(5)
      expect(result.find(r => r.name === 'Origin')?.geom_type).toContain('POINT')
      expect(result.find(r => r.name === 'Line')?.geom_type).toContain('LINESTRING')
      expect(result.find(r => r.name === 'Square')?.geom_type).toContain('POLYGON')
    })

    it('should validate geometry using stIsValid', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stIsValid(sql.ref('location')).as('is_valid')])
        .execute()

      expect(result).toHaveLength(5)
      expect(result.every(r => r.is_valid === true)).toBe(true)
    })

    it('should calculate area using stArea', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stArea(sql.ref('location')).as('area')])
        .where('name', '=', 'Square')
        .execute()

      expect(result).toHaveLength(1)
      const r0 = result[0]
      expect(r0?.area).toBe(16) // 4x4 square
    })

    it('should calculate length using stLength', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stLength(sql.ref('location')).as('length')])
        .where('name', '=', 'Line')
        .execute()

      expect(result).toHaveLength(1)
      const r0 = result[0]
      expect(r0?.length).toBeCloseTo(2.828, 2) // √2 + √2 ≈ 2.828
    })
  })

  describe('Spatial Relationship Functions', () => {
    it('should test intersection using stIntersects', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stIntersects(
            sql.ref('location'),
            SpatialFunctions.stGeomFromText(
              sql.lit('POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))'),
            ),
          ).as('intersects'),
        ])
        .execute()

      expect(result).toHaveLength(5)
      expect(result.find(r => r.name === 'Point A')?.intersects).toBe(true)
    })

    it('should test containment using stWithin', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stWithin(
            sql.ref('location'),
            SpatialFunctions.stGeomFromText(sql.lit('POLYGON((-1 -1, 5 -1, 5 5, -1 5, -1 -1))')),
          ).as('within'),
        ])
        .execute()

      expect(result).toHaveLength(5)
      expect(result.every(r => r.within === true)).toBe(true)
    })

    it('should calculate distance using stDistance', async () => {
      const referencePoint = SpatialFunctions.stPoint(0, 0)

      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stDistance(sql.ref('location'), referencePoint).as('distance'),
        ])
        .where('name', '=', 'Point B')
        .execute()

      expect(result).toHaveLength(1)
      const r0 = result[0]
      expect(r0?.distance).toBeCloseTo(5, 5) // √(3² + 4²) = 5
    })
  })

  describe('Geometry Processing Functions', () => {
    it('should create buffer using stBuffer', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stArea(SpatialFunctions.stBuffer(sql.ref('location'), 1)).as(
            'buffer_area',
          ),
        ])
        .where('name', '=', 'Origin')
        .execute()

      expect(result).toHaveLength(1)
      const r0 = result[0]
      expect(r0?.buffer_area).toBeCloseTo(Math.PI, 1) // π for radius 1
    })

    it('should calculate centroid using stCentroid', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stAsText(SpatialFunctions.stCentroid(sql.ref('location'))).as(
            'centroid_wkt',
          ),
        ])
        .where('name', '=', 'Square')
        .execute()

      expect(result).toHaveLength(1)
      const r0 = result[0]
      expect(r0?.centroid_wkt).toContain('POINT')
      expect(r0?.centroid_wkt).toContain('2') // Center at (2,2)
    })

    it('should calculate envelope using stEnvelope', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stAsText(SpatialFunctions.stEnvelope(sql.ref('location'))).as(
            'envelope_wkt',
          ),
        ])
        .where('name', '=', 'Line')
        .execute()

      expect(result).toHaveLength(1)
      const r0 = result[0]
      expect(r0?.envelope_wkt).toContain('POLYGON')
    })
  })

  describe('Format Conversion Functions', () => {
    it('should convert to WKT using stAsText', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stAsText(sql.ref('location')).as('wkt')])
        .execute()

      expect(result).toHaveLength(5)
      expect(result.every(r => typeof r.wkt === 'string')).toBe(true)
      expect(result.find(r => r.name === 'Origin')?.wkt).toContain('POINT')
    })

    it('should convert to GeoJSON using stAsGeoJSON', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stAsGeoJSON(sql.ref('location')).as('geojson')])
        .where('name', '=', 'Origin')
        .execute()

      expect(result).toHaveLength(1)
      // DuckDB returns GeoJSON as a JavaScript object, not a JSON string
      const r0 = result[0]
      expect(typeof r0?.geojson).toBe('object')
      expect(r0?.geojson).toHaveProperty('type', 'Point')
      expect(r0?.geojson).toHaveProperty('coordinates')
      expect(Array.isArray((r0 as any).geojson.coordinates)).toBe(true)
      expect((r0 as any).geojson.coordinates).toEqual([0, 0])
    })
  })

  describe('Complex Spatial Operations', () => {
    it('should perform complex spatial queries with multiple conditions', async () => {
      const result = await db
        .selectFrom('spatial_test')
        .select([
          'name',
          SpatialFunctions.stDistance(sql.ref('location'), SpatialFunctions.stPoint(0, 0)).as(
            'distance_from_origin',
          ),
        ])
        .where(eb =>
          SpatialFunctions.stWithin(
            eb.ref('location'),
            SpatialFunctions.stGeomFromText(
              sql.lit('POLYGON((-0.5 -0.5, 2.5 -0.5, 2.5 2.5, -0.5 2.5, -0.5 -0.5))'),
            ),
          ),
        )
        .orderBy('distance_from_origin')
        .execute()

      expect(result.length).toBeGreaterThan(0)
      expect(result[0]?.name).toBe('Origin') // Closest to origin
    })

    it('should handle null geometries gracefully', async () => {
      await sql`INSERT INTO spatial_test (id, location, name) VALUES (99, NULL, 'Null Geometry')`.execute(
        db,
      )

      const result = await db
        .selectFrom('spatial_test')
        .select(['name', SpatialFunctions.stIsValid(sql.ref('location')).as('is_valid')])
        .where('name', '=', 'Null Geometry')
        .execute()

      expect(result).toHaveLength(1)
      expect(result[0]?.is_valid).toBe(null)
    })
  })
})

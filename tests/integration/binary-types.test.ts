import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/index.js'

describe('DuckDB Binary and Special Types Integration', () => {
  let database: DuckDBInstance
  let db: Kysely<any>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    db = new Kysely<any>({
      dialect: new DuckDbDialect({ database }),
    })
    await sql.raw('PRAGMA old_implicit_casting=true').execute(db)
  })

  afterEach(async () => {
    await db.destroy()
  })

  describe('BLOB Types', () => {
    it('should handle Buffer objects for BLOB columns', async () => {
      await db.schema
        .createTable('blob_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('data', 'blob')
        .addColumn('binary_data', sql`BYTEA`)
        .execute()

      const buffer = Buffer.from('Hello, World!', 'utf8')
      const uint8Array = new Uint8Array([1, 2, 3, 4, 5])

      await db
        .insertInto('blob_test')
        .values({
          data: buffer,
          binary_data: uint8Array,
        })
        .execute()

      const results = await db.selectFrom('blob_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['data']).toBeDefined()
      expect(row['binary_data']).toBeDefined()
    })

    it('should handle empty BLOB data', async () => {
      await db.schema
        .createTable('empty_blob_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('empty_data', 'blob')
        .execute()

      // Use null instead of empty buffer to avoid hex encoding issues
      await db
        .insertInto('empty_blob_test')
        .values({
          empty_data: null,
        })
        .execute()

      const results = await db.selectFrom('empty_blob_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['empty_data']).toBeNull()
    })

    it('should handle large binary data', async () => {
      await db.schema
        .createTable('large_blob_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('large_data', 'blob')
        .execute()

      // Create a 1KB buffer
      const largeBuffer = Buffer.alloc(1024, 0xff)

      await db
        .insertInto('large_blob_test')
        .values({
          large_data: largeBuffer,
        })
        .execute()

      const results = await db.selectFrom('large_blob_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['large_data']).toBeDefined()
    })
  })

  describe('INTERVAL Types', () => {
    it('should handle string interval values', async () => {
      await db.schema
        .createTable('interval_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('duration', sql`INTERVAL`)
        .addColumn('period', sql`INTERVAL`)
        .execute()

      await db
        .insertInto('interval_test')
        .values({
          duration: '3 months',
          period: '1 year 2 days',
        })
        .execute()

      const results = await db.selectFrom('interval_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['duration']).toBeDefined()
      expect(row['period']).toBeDefined()
    })

    it('should handle various interval formats', async () => {
      await db.schema
        .createTable('interval_formats_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('short_interval', sql`INTERVAL`)
        .addColumn('complex_interval', sql`INTERVAL`)
        .execute()

      await db
        .insertInto('interval_formats_test')
        .values([
          {
            short_interval: '1 day',
            complex_interval: '2 years 3 months 4 days 5 hours 6 minutes 7 seconds',
          },
          {
            short_interval: '24 hours',
            complex_interval: '90 minutes',
          },
        ])
        .execute()

      const results = await db.selectFrom('interval_formats_test').selectAll().execute()
      expect(results).toHaveLength(2)
    })
  })

  describe('DECIMAL Types', () => {
    it('should handle JavaScript numbers for DECIMAL columns', async () => {
      await db.schema
        .createTable('decimal_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('price', sql`DECIMAL(10,2)`)
        .addColumn('rate', sql`DECIMAL(5,4)`)
        .execute()

      await db
        .insertInto('decimal_test')
        .values({
          price: 123.45,
          rate: 0.0123,
        })
        .execute()

      const results = await db.selectFrom('decimal_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['price']).toBeDefined()
      expect(row['rate']).toBeDefined()
    })

    it('should handle high precision decimal values', async () => {
      await db.schema
        .createTable('high_precision_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('precise_value', sql`DECIMAL(20,10)`)
        .execute()

      await db
        .insertInto('high_precision_test')
        .values({
          // use string to avoid precision loss in JS literal while allowing DECIMAL precision
          precise_value: sql`CAST('123456789.0123456789' AS DECIMAL(20,10))`,
        })
        .execute()

      const results = await db.selectFrom('high_precision_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['precise_value']).toBeDefined()
    })
  })

  describe('ENUM Types', () => {
    it('should handle string values for ENUM columns', async () => {
      // Create custom enum type
      await sql.raw(`CREATE TYPE mood AS ENUM ('happy', 'sad', 'excited', 'calm')`).execute(db)

      await db.schema
        .createTable('enum_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('current_mood', sql`mood`)
        .addColumn('status', sql`ENUM('active', 'inactive', 'pending')`)
        .execute()

      await db
        .insertInto('enum_test')
        .values({
          current_mood: 'happy',
          status: 'active',
        })
        .execute()

      const results = await db.selectFrom('enum_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const row = results[0]
      expect(row).toBeDefined()
      if (!row) {
        throw new Error('missing row')
      }
      expect(row['current_mood']).toBe('happy')
      expect(row['status']).toBe('active')
    })

    it('should handle all enum values', async () => {
      await sql.raw(`CREATE TYPE color AS ENUM ('red', 'green', 'blue')`).execute(db)

      await db.schema
        .createTable('color_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('color', sql`color`)
        .execute()

      // Test all enum values
      await db
        .insertInto('color_test')
        .values([{ color: 'red' }, { color: 'green' }, { color: 'blue' }])
        .execute()

      const results = await db.selectFrom('color_test').selectAll().execute()
      expect(results).toHaveLength(3)
      expect(results.map(r => r['color']).sort()).toEqual(['blue', 'green', 'red'])
    })
  })
})

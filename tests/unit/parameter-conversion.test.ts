import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/index.js'

describe('Parameter Conversion Unit Tests', () => {
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

  describe('Date Parameter Conversion', () => {
    it('should convert Date objects to ISO strings', async () => {
      await db.schema
        .createTable('date_conversion_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('timestamp_col', 'timestamp')
        .addColumn('date_col', 'date')
        .execute()

      const testDate = new Date('2025-08-27T14:30:00.123Z')

      await db
        .insertInto('date_conversion_test')
        .values({
          timestamp_col: testDate,
          date_col: testDate,
        })
        .execute()

      const results = await db.selectFrom('date_conversion_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['timestamp_col']).toBeDefined()
      expect(r0?.['date_col']).toBeDefined()
    })

    it('should handle Date objects in WHERE clauses', async () => {
      await db.schema
        .createTable('date_where_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('created_at', 'timestamp')
        .execute()

      const date1 = new Date('2025-08-27T10:00:00Z')
      const date2 = new Date('2025-08-27T14:00:00Z')
      const date3 = new Date('2025-08-27T18:00:00Z')

      await db
        .insertInto('date_where_test')
        .values([{ created_at: date1 }, { created_at: date2 }, { created_at: date3 }])
        .execute()

      const results = await db
        .selectFrom('date_where_test')
        .selectAll()
        .where('created_at', '>=', date2)
        .execute()

      expect(results.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Array Parameter Conversion', () => {
    it('should convert JavaScript arrays to DuckDB LIST format', async () => {
      await db.schema
        .createTable('array_conversion_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('numbers', sql`INTEGER[]`)
        .addColumn('strings', sql`VARCHAR[]`)
        .execute()

      await db
        .insertInto('array_conversion_test')
        .values({
          numbers: [1, 2, 3, 4, 5],
          strings: ['hello', 'world', 'test'],
        })
        .execute()

      const results = await db.selectFrom('array_conversion_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0] as any
      expect(Array.isArray(r0?.['numbers'])).toBe(true)
      expect(Array.isArray(r0?.['strings'])).toBe(true)
    })

    it('should handle arrays with null values', async () => {
      await db.schema
        .createTable('array_null_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('mixed_array', sql`VARCHAR[]`)
        .execute()

      await db
        .insertInto('array_null_test')
        .values({
          mixed_array: ['value1', null, 'value3', null],
        })
        .execute()

      const results = await db.selectFrom('array_null_test').selectAll().execute()
      expect(results).toHaveLength(1)
    })

    it('should handle arrays with special characters', async () => {
      await db.schema
        .createTable('array_special_chars_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('special_strings', sql`VARCHAR[]`)
        .execute()

      await db
        .insertInto('array_special_chars_test')
        .values({
          special_strings: ["it's", 'qu"ote', 'back\\slash', 'new\nline'],
        })
        .execute()

      const results = await db.selectFrom('array_special_chars_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0] as any
      expect(Array.isArray(r0?.['special_strings'])).toBe(true)
    })
  })

  describe('Object Parameter Conversion', () => {
    it('should convert JavaScript objects to JSON strings', async () => {
      await db.schema
        .createTable('object_conversion_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('config', 'json')
        .addColumn('metadata', 'json')
        .execute()

      const config = { timeout: 5000, retries: 3, enabled: true }
      const metadata = { version: '1.0', tags: ['test', 'production'] }

      await db
        .insertInto('object_conversion_test')
        .values({
          config: config,
          metadata: metadata,
        })
        .execute()

      const results = await db.selectFrom('object_conversion_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0] as any
      expect(typeof r0?.['config']).toBe('object')
      expect(typeof r0?.['metadata']).toBe('object')
      expect(r0?.['config']?.timeout).toBe(5000)
      expect(r0?.['metadata']?.version).toBe('1.0')
    })

    it('should handle nested objects', async () => {
      await db.schema
        .createTable('nested_object_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('nested_config', 'json')
        .execute()

      const nestedConfig = {
        database: {
          host: 'localhost',
          port: 5432,
          credentials: {
            username: 'user',
            password: 'secret',
          },
        },
        features: {
          caching: { enabled: true, ttl: 3600 },
          logging: { level: 'info', format: 'json' },
        },
      }

      await db
        .insertInto('nested_object_test')
        .values({
          nested_config: nestedConfig,
        })
        .execute()

      const results = await db.selectFrom('nested_object_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0] as any
      expect(r0?.['nested_config']?.database?.host).toBe('localhost')
      expect(r0?.['nested_config']?.features?.caching?.enabled).toBe(true)
    })
  })

  describe('Buffer Parameter Conversion', () => {
    it('should convert Buffer objects to hex strings for BLOB columns', async () => {
      await db.schema
        .createTable('buffer_conversion_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('binary_data', 'blob')
        .execute()

      const buffer = Buffer.from('Hello, Binary World!', 'utf8')

      await db
        .insertInto('buffer_conversion_test')
        .values({
          binary_data: buffer,
        })
        .execute()

      const results = await db.selectFrom('buffer_conversion_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['binary_data']).toBeDefined()
    })

    it('should convert Uint8Array to hex strings for BLOB columns', async () => {
      await db.schema
        .createTable('uint8array_conversion_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('binary_data', 'blob')
        .execute()

      const uint8Array = new Uint8Array([0x48, 0x65, 0x6c, 0x6c, 0x6f]) // "Hello"

      await db
        .insertInto('uint8array_conversion_test')
        .values({
          binary_data: uint8Array,
        })
        .execute()

      const results = await db.selectFrom('uint8array_conversion_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['binary_data']).toBeDefined()
    })
  })

  describe('BigInt Parameter Conversion', () => {
    it('should handle BigInt values directly', async () => {
      await db.schema
        .createTable('bigint_conversion_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('large_number', sql`HUGEINT`)
        .execute()

      const largeBigInt = BigInt('12345678901234567890')

      await db
        .insertInto('bigint_conversion_test')
        .values({
          large_number: largeBigInt,
        })
        .execute()

      const results = await db.selectFrom('bigint_conversion_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(typeof r0?.['large_number']).toBe('bigint')
    })
  })

  describe('Parameter Type Edge Cases', () => {
    it('should handle null and undefined parameters', async () => {
      await db.schema
        .createTable('null_param_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('nullable_text', sql`VARCHAR`)
        .addColumn('nullable_number', sql`INTEGER`)
        .addColumn('nullable_json', 'json')
        .execute()

      await db
        .insertInto('null_param_test')
        .values([
          {
            nullable_text: null,
            nullable_number: null,
            nullable_json: null,
          },
          {
            nullable_text: 'not null',
            nullable_number: 42,
            nullable_json: { key: 'value' },
          },
        ])
        .execute()

      const results = await db.selectFrom('null_param_test').selectAll().execute()
      expect(results).toHaveLength(2)
      const r0 = results[0]
      const r1 = results[1]
      expect(r0?.['nullable_text']).toBeNull()
      expect(r1?.['nullable_text']).toBe('not null')
    })

    it('should handle boolean parameters', async () => {
      await db.schema
        .createTable('boolean_param_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('is_active', 'boolean')
        .addColumn('is_verified', 'boolean')
        .execute()

      await db
        .insertInto('boolean_param_test')
        .values([
          { is_active: true, is_verified: false },
          { is_active: false, is_verified: true },
        ])
        .execute()

      const results = await db.selectFrom('boolean_param_test').selectAll().execute()
      expect(results).toHaveLength(2)
      const r0 = results[0]
      const r1 = results[1]
      expect(r0?.['is_active']).toBe(true)
      expect(r0?.['is_verified']).toBe(false)
      expect(r1?.['is_active']).toBe(false)
      expect(r1?.['is_verified']).toBe(true)
    })
  })
})

import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/index.js'

describe('DuckDB Integration - Complex Queries', () => {
  let database: DuckDBInstance
  let db: Kysely<any>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    db = new Kysely<any>({
      dialect: new DuckDbDialect({
        database,
      }),
    })
  })

  afterEach(async () => {
    if (db) {
      await db.destroy()
    }
  })

  describe('JSON Column Type Detection and Parsing', () => {
    it('should parse JSON columns but leave TEXT columns as strings', async () => {
      // Create table with both JSON and TEXT columns
      await db.schema
        .createTable('json_vs_text_test')
        .addColumn('id', 'integer')
        .addColumn('json_data', 'json')
        .addColumn('text_data', 'varchar')
        .execute()

      // Insert identical JSON strings in both columns
      await db
        .insertInto('json_vs_text_test')
        .values([
          {
            id: 1,
            json_data: '{"name": "Alice", "age": 30, "active": true}',
            text_data: '{"name": "Alice", "age": 30, "active": true}',
          },
          {
            id: 2,
            json_data: '[1, 2, 3, "test"]',
            text_data: '[1, 2, 3, "test"]',
          },
        ])
        .execute()

      const results = await db.selectFrom('json_vs_text_test').selectAll().execute()

      expect(results).toHaveLength(2)

      // JSON column should be parsed into objects/arrays
      const r0: any = results[0]
      const r1: any = results[1]
      expect(typeof r0.json_data).toBe('object')
      expect(r0.json_data).toEqual({ name: 'Alice', age: 30, active: true })
      expect(Array.isArray(r1.json_data)).toBe(true)
      expect(r1.json_data).toEqual([1, 2, 3, 'test'])

      // TEXT column should remain as strings
      expect(typeof r0.text_data).toBe('string')
      expect(r0.text_data).toBe('{"name": "Alice", "age": 30, "active": true}')
      expect(typeof r1.text_data).toBe('string')
      expect(r1.text_data).toBe('[1, 2, 3, "test"]')
    })

    it('should handle malformed JSON strings in TEXT columns without errors', async () => {
      await db.schema
        .createTable('malformed_json_test')
        .addColumn('id', 'integer')
        .addColumn('text_data', 'varchar')
        .execute()

      // Insert invalid JSON strings - should work in TEXT columns
      await db
        .insertInto('malformed_json_test')
        .values([
          { id: 1, text_data: '{invalid: json}' },
          { id: 2, text_data: '[unclosed array' },
          { id: 3, text_data: 'not json at all' },
        ])
        .execute()

      const results = await db.selectFrom('malformed_json_test').selectAll().execute()

      expect(results).toHaveLength(3)
      // All should remain as strings since they're in TEXT columns
      const m0: any = results[0]
      const m1: any = results[1]
      const m2: any = results[2]
      expect(m0.text_data).toBe('{invalid: json}')
      expect(m1.text_data).toBe('[unclosed array')
      expect(m2.text_data).toBe('not json at all')
    })

    it('should handle null values consistently across JSON and TEXT columns', async () => {
      await db.schema
        .createTable('null_handling_test')
        .addColumn('id', 'integer')
        .addColumn('json_col', 'json')
        .addColumn('text_col', 'varchar')
        .execute()

      await db
        .insertInto('null_handling_test')
        .values([
          { id: 1, json_col: null, text_col: null },
          { id: 2, json_col: '{"value": null}', text_col: 'null' },
          { id: 3, json_col: 'null', text_col: 'null' },
        ])
        .execute()

      const results = await db.selectFrom('null_handling_test').selectAll().execute()

      expect(results).toHaveLength(3)

      const n0: any = results[0]
      const n1: any = results[1]
      const n2: any = results[2]
      // First row: actual null values
      expect(n0.json_col).toBe(null)
      expect(n0.text_col).toBe(null)

      // Second row: JSON with null property vs string "null"
      expect(n1.json_col).toEqual({ value: null })
      expect(n1.text_col).toBe('null')

      // Third row: JSON null vs string "null"
      expect(n2.json_col).toBe(null) // JSON 'null' becomes actual null
      expect(n2.text_col).toBe('null') // TEXT remains string
    })
  })

  describe('Complex JSON Operations', () => {
    beforeEach(async () => {
      await db.schema
        .createTable('complex_json_test')
        .addColumn('id', 'integer')
        .addColumn('user_data', 'json')
        .addColumn('metadata', 'json')
        .execute()

      // Insert complex nested JSON data
      await db
        .insertInto('complex_json_test')
        .values([
          {
            id: 1,
            user_data: JSON.stringify({
              name: 'Alice',
              profile: {
                age: 30,
                preferences: {
                  theme: 'dark',
                  notifications: true,
                  languages: ['en', 'fr'],
                },
              },
              history: [
                { action: 'login', timestamp: '2023-01-01T10:00:00Z' },
                { action: 'update_profile', timestamp: '2023-01-02T15:30:00Z' },
              ],
            }),
            metadata: JSON.stringify({
              created_at: '2023-01-01',
              tags: ['premium', 'active'],
              scores: [95, 87, 92],
            }),
          },
          {
            id: 2,
            user_data: JSON.stringify({
              name: 'Bob',
              profile: {
                age: 25,
                preferences: {
                  theme: 'light',
                  notifications: false,
                  languages: ['en'],
                },
              },
              history: [],
            }),
            metadata: JSON.stringify({
              created_at: '2023-02-01',
              tags: ['basic'],
              scores: [78, 82],
            }),
          },
        ])
        .execute()
    })

    it('should correctly parse deeply nested JSON structures', async () => {
      const results = await db.selectFrom('complex_json_test').selectAll().execute()

      expect(results).toHaveLength(2)

      // Verify deep nesting is preserved
      const c0: any = results[0]
      const c1: any = results[1]
      expect(c0.user_data.profile.preferences.theme).toBe('dark')
      expect(c0.user_data.profile.preferences.languages).toEqual(['en', 'fr'])
      expect(Array.isArray(c0.user_data.history)).toBe(true)
      expect(c0.user_data.history).toHaveLength(2)

      expect(c1.user_data.profile.preferences.theme).toBe('light')
      expect(c1.user_data.history).toEqual([])
    })

    it('should handle arrays within JSON properly', async () => {
      const results = await db.selectFrom('complex_json_test').selectAll().execute()

      // Check metadata arrays
      const a0: any = results[0]
      const a1: any = results[1]
      expect(Array.isArray(a0.metadata.tags)).toBe(true)
      expect(a0.metadata.tags).toEqual(['premium', 'active'])
      expect(Array.isArray(a0.metadata.scores)).toBe(true)
      expect(a0.metadata.scores).toEqual([95, 87, 92])

      expect(a1.metadata.tags).toEqual(['basic'])
      expect(a1.metadata.scores).toEqual([78, 82])
    })

    it('should maintain data type integrity in JSON parsing', async () => {
      const results = await db.selectFrom('complex_json_test').selectAll().execute()

      // Verify types are preserved
      const t0: any = results[0]
      expect(typeof t0.user_data.profile.age).toBe('number')
      expect(typeof t0.user_data.profile.preferences.notifications).toBe('boolean')
      expect(typeof t0.user_data.name).toBe('string')

      // Verify numbers in arrays
      t0.metadata.scores.forEach((score: any) => {
        expect(typeof score).toBe('number')
      })
    })
  })

  describe('Array Data Handling in JSON', () => {
    it('should handle arrays within JSON columns properly', async () => {
      await db.schema
        .createTable('json_arrays_test')
        .addColumn('id', 'integer')
        .addColumn('data', 'json')
        .execute()

      await db
        .insertInto('json_arrays_test')
        .values({
          id: 1,
          data: JSON.stringify({
            numbers: [1, 2, 3, 4, 5],
            words: ['apple', 'banana', 'cherry'],
            mixed: [1, 'two', true, null, ['nested', 'array']],
            empty: [],
          }),
        })
        .execute()

      const results = await db.selectFrom('json_arrays_test').selectAll().execute()

      expect(results).toHaveLength(1)
      const row: any = results[0]

      // JSON arrays should be properly converted to JavaScript arrays
      expect(Array.isArray(row.data.numbers)).toBe(true)
      expect(row.data.numbers).toEqual([1, 2, 3, 4, 5])

      expect(Array.isArray(row.data.words)).toBe(true)
      expect(row.data.words).toEqual(['apple', 'banana', 'cherry'])

      expect(Array.isArray(row.data.mixed)).toBe(true)
      expect(row.data.mixed).toEqual([1, 'two', true, null, ['nested', 'array']])

      expect(Array.isArray(row.data.empty)).toBe(true)
      expect(row.data.empty).toHaveLength(0)
    })
  })

  describe('Large Dataset Operations', () => {
    it('should handle bulk operations efficiently', async () => {
      await db.schema
        .createTable('bulk_test')
        .addColumn('id', 'integer')
        .addColumn('data', 'json')
        .addColumn('timestamp', 'timestamp')
        .execute()

      // Generate bulk data
      const bulkData = Array.from({ length: 1000 }, (_, i) => ({
        id: i + 1,
        data: JSON.stringify({
          user_id: i + 1,
          score: Math.floor(Math.random() * 100),
          tags: [`tag_${i % 10}`, `category_${i % 5}`],
        }),
        timestamp: new Date(Date.now() + i * 1000).toISOString(),
      }))

      const startTime = Date.now()

      // Insert in chunks for better performance
      const chunkSize = 100
      for (let i = 0; i < bulkData.length; i += chunkSize) {
        const chunk = bulkData.slice(i, i + chunkSize)
        await db.insertInto('bulk_test').values(chunk).execute()
      }

      const insertDuration = Date.now() - startTime

      // Query back the data
      const queryStartTime = Date.now()
      const results = await db.selectFrom('bulk_test').selectAll().execute()
      const queryDuration = Date.now() - queryStartTime

      expect(results).toHaveLength(1000)
      expect(insertDuration).toBeLessThan(10000) // Should complete in less than 10 seconds
      expect(queryDuration).toBeLessThan(5000) // Query should be fast

      // Verify JSON parsing worked correctly for all rows
      results.forEach((row: any, index) => {
        expect(typeof row.data).toBe('object')
        expect(row.data.user_id).toBe(index + 1)
        expect(Array.isArray(row.data.tags)).toBe(true)
        expect(row.data.tags).toHaveLength(2)
      })
    })
  })

  describe('Error Handling with Real Database', () => {
    it('should handle JSON validation errors in JSON columns', async () => {
      await db.schema
        .createTable('json_validation_test')
        .addColumn('id', 'integer')
        .addColumn('strict_json', 'json')
        .execute()

      // DuckDB should reject invalid JSON in JSON columns
      await expect(
        db
          .insertInto('json_validation_test')
          .values({ id: 1, strict_json: '{invalid json}' as any })
          .execute(),
      ).rejects.toThrow()

      // But valid JSON should work
      await expect(
        db
          .insertInto('json_validation_test')
          .values({ id: 1, strict_json: '{"valid": "json"}' as any })
          .execute(),
      ).resolves.not.toThrow()
    })

    it('should handle connection cleanup on errors', async () => {
      // Create a table
      await db.schema
        .createTable('error_test')
        .addColumn('id', 'integer', col => col.primaryKey())
        .execute()

      // Insert initial data
      await db.insertInto('error_test').values({ id: 1 }).execute()

      // Try to insert duplicate key - should fail
      await expect(db.insertInto('error_test').values({ id: 1 }).execute()).rejects.toThrow()

      // Connection should still work after error
      const result = await db.selectFrom('error_test').selectAll().execute()
      expect(result).toHaveLength(1)
      const first = result[0]
      expect(first?.['id']).toBe(1)
    })
  })
})

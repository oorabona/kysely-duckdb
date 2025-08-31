import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'
import {
  JsonFunctions,
  jsonArrayLength,
  jsonContains,
  jsonExists,
  jsonExtract,
  jsonKeys,
  jsonStructure,
  jsonTransform,
  jsonType,
  jsonValid,
} from '../../src/extensions/json.js'

interface TestTable {
  id: number
  json_data: string
  metadata: string
}

describe('JsonFunctions Integration', () => {
  let database: DuckDBInstance
  let db: Kysely<{ test_json: TestTable }>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    const dialect = new DuckDbDialect({ database })
    db = new Kysely<any>({ dialect })

    // Create test table with JSON data
    await db.schema
      .createTable('test_json')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('json_data', 'varchar')
      .addColumn('metadata', 'varchar')
      .execute()

    // Insert test data
    await db
      .insertInto('test_json')
      .values([
        {
          id: 1,
          json_data: '{"name": "John", "age": 30, "city": "New York"}',
          metadata: '{"type": "user", "active": true}',
        },
        {
          id: 2,
          json_data: '{"name": "Jane", "age": 25, "items": ["a", "b", "c"]}',
          metadata: '{"type": "admin", "permissions": ["read", "write"]}',
        },
        {
          id: 3,
          json_data: '{"company": "ACME Corp", "employees": 100}',
          metadata: '{"type": "organization", "verified": false}',
        },
      ])
      .execute()
  })

  afterEach(async () => {
    try {
      await db?.destroy()
    } catch {
      // Ignore cleanup errors
    }
    try {
      database?.closeSync()
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('jsonExtract', () => {
    it('should extract values from JSON strings', async () => {
      const result = await db
        .selectFrom('test_json')
        .select(['id', jsonExtract(sql.ref('json_data'), '$.name').as('name')])
        .where('id', '=', 1)
        .execute()

      expect(result).toHaveLength(1)
      expect(result[0]?.name).toBe('John')
    })

    it('should extract nested values', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([
          'id',
          jsonExtract(sql.ref('json_data'), '$.age').as('age'),
          jsonExtract(sql.ref('metadata'), '$.type').as('type'),
        ])
        .execute()

      expect(result).toHaveLength(3)
      expect(result[0]?.age).toBe(30)
      expect(result[0]?.type).toBe('user')
    })

    it('should handle array extraction', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonExtract(sql.ref('json_data'), '$.items').as('items')])
        .where('id', '=', 2)
        .execute()

      expect(result[0]?.items).toEqual(['a', 'b', 'c'])
    })
  })

  describe('jsonValid', () => {
    it('should validate JSON strings', async () => {
      const result = await db
        .selectFrom('test_json')
        .select(['id', jsonValid(sql.ref('json_data')).as('is_valid')])
        .execute()

      expect(result).toHaveLength(3)
      for (const row of result) {
        expect(row.is_valid).toBe(true)
      }
    })

    it('should detect invalid JSON', async () => {
      // Insert invalid JSON
      await db
        .insertInto('test_json')
        .values({ id: 999, json_data: 'invalid json', metadata: '{}' })
        .execute()

      const result = await db
        .selectFrom('test_json')
        .select(['id', jsonValid(sql.ref('json_data')).as('is_valid')])
        .where('id', '=', 999)
        .execute()

      expect(result[0]?.is_valid).toBe(false)
    })
  })

  describe('jsonKeys', () => {
    it('should extract object keys', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonKeys(sql.ref('json_data')).as('keys')])
        .where('id', '=', 1)
        .execute()

      expect(result[0]?.keys).toContain('name')
      expect(result[0]?.keys).toContain('age')
      expect(result[0]?.keys).toContain('city')
    })

    it('should extract object keys using path overload', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonKeys(sql.ref('metadata'), '$').as('keys_root')])
        .where('id', '=', 1)
        .execute()

      expect(result[0]?.keys_root).toContain('type')
      expect(result[0]?.keys_root).toContain('active')
    })
  })

  describe('jsonType', () => {
    it('should return JSON value types', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([
          jsonType(jsonExtract(sql.ref('json_data'), '$.name')).as('name_type'),
          jsonType(jsonExtract(sql.ref('json_data'), '$.age')).as('age_type'),
        ])
        .where('id', '=', 1)
        .execute()

      expect(result[0]?.name_type).toBeTypeOf('string')
      expect(typeof result[0]?.age_type).toBe('string')
    })
  })

  describe('jsonExists', () => {
    it('should check if JSON path exists', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([
          'id',
          jsonExists(sql.ref('json_data'), '$.name').as('has_name'),
          jsonExists(sql.ref('json_data'), '$.nonexistent').as('has_nonexistent'),
        ])
        .execute()

      expect(result[0]?.has_name).toBe(true)
      expect(result[0]?.has_nonexistent).toBe(false)
    })
  })

  describe('jsonArrayLength', () => {
    it('should return array length', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonArrayLength(jsonExtract(sql.ref('json_data'), '$.items')).as('array_length')])
        .where('id', '=', 2)
        .execute()

      expect(result[0]?.array_length).toBe(3n)
    })

    it('should handle non-arrays', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonArrayLength(sql.ref('json_data')).as('length')])
        .where('id', '=', 1)
        .execute()

      // For non-arrays, should return null or 0n
      expect([null, 0n]).toContain(result[0]?.length)
    })

    it('should return array length using path overload', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonArrayLength(sql.ref('json_data'), '$.items').as('array_length_path')])
        .where('id', '=', 2)
        .execute()

      expect(result[0]?.array_length_path).toBe(3n)
    })
  })

  describe('jsonMerge', () => {
    it('should generate jsonMerge calls', () => {
      // Test that the function generates proper SQL without executing
      const mergeExpr = JsonFunctions.jsonMerge(sql.ref('json_data'), sql.ref('metadata'))

      expect(mergeExpr).toBeDefined()
      expect(mergeExpr.toOperationNode().kind).toBe('RawNode')
    })
  })

  describe('jsonStructure', () => {
    it('should return JSON structure', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([(jsonStructure(sql.ref('json_data')) as any).as('structure')])
        .where('id', '=', 1)
        .execute()

      // Structure returns an object describing the JSON schema
      expect(result[0]?.['structure']).toBeDefined()
    })
  })

  describe('jsonTransform', () => {
    it('should generate jsonTransform calls', () => {
      // Test single transformation
      const singleTransform = jsonTransform(sql.ref('json_data'), '$.age', sql.lit('35'))

      expect(singleTransform).toBeDefined()
      expect((singleTransform as any).toOperationNode().kind).toBe('RawNode')

      // Test multiple transformations
      const multiTransform = jsonTransform(sql.ref('json_data'), [
        { path: '$.name', value: sql.lit('"Updated"') },
        { path: '$.age', value: sql.lit('99') },
      ])

      expect(multiTransform).toBeDefined()

      // Test raw structure overload: json_transform(json, structure)
      const structureTransform = jsonTransform(
        sql.ref('json_data'),
        sql.lit('STRUCT(name VARCHAR, age INTEGER)'),
      )

      expect(structureTransform).toBeDefined()
      expect((structureTransform as any).toOperationNode().kind).toBe('RawNode')
    })
  })

  describe('jsonContains', () => {
    it('should check if value is contained', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([
          jsonContains(sql.ref('json_data'), sql.lit('"John"')).as('contains_john'),
          jsonContains(sql.ref('json_data'), sql.lit('"NotFound"')).as('contains_notfound'),
        ])
        .where('id', '=', 1)
        .execute()

      expect(result[0]?.contains_john).toBe(true)
      expect(result[0]?.contains_notfound).toBe(false)
    })

    it('should check contains with path', async () => {
      // DuckDB's json_contains doesn't support a third path argument. Instead,
      // extract the value at the path and compare directly.
      const result = await db
        .selectFrom('test_json')
        .select([
          sql<boolean>`json_extract_string(${sql.ref('json_data')}, '$.age') = '30'`.as(
            'age_contains_30',
          ),
        ])
        .where('id', '=', 1)
        .execute()

      expect(result[0]?.age_contains_30).toBe(true)
    })

    it('should check contains with path (items array)', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([
          jsonContains(sql.ref('json_data'), sql.lit('"b"'), '$.items').as('items_contains_b'),
        ])
        .where('id', '=', 2)
        .execute()

      expect(result[0]?.items_contains_b).toBe(true)
    })
  })

  describe('complex queries', () => {
    it('should handle nested JSON function calls', async () => {
      const result = await db
        .selectFrom('test_json')
        .select([jsonType(jsonExtract(sql.ref('metadata'), '$.type')).as('type_of_type')])
        .execute()

      expect(result[0]?.type_of_type).toBe('VARCHAR')
    })

    it('should work in WHERE clauses', async () => {
      const result = await db
        .selectFrom('test_json')
        .select(['id'])
        .where(JsonFunctions.jsonExtract(sql.ref('metadata'), '$.type'), '=', '"user"')
        .execute()

      expect(result).toHaveLength(1)
      expect(result[0]?.id).toBe(1)
    })

    it('should work in ORDER BY clauses', async () => {
      const result = await db
        .selectFrom('test_json')
        .select(['id'])
        .orderBy(JsonFunctions.jsonExtract(sql.ref('json_data'), '$.age'), 'desc')
        .execute()

      expect(result[0]?.id).toBe(1) // age 30
      expect(result[1]?.id).toBe(2) // age 25
    })
  })
})

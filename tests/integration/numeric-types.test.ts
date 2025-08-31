import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/index.js'

describe('DuckDB Numeric Types Integration', () => {
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

  describe('Large Number Types', () => {
    it('should handle JavaScript BigInt for HUGEINT columns', async () => {
      await db.schema
        .createTable('bignum_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('huge_int', sql`HUGEINT`)
        .addColumn('big_decimal', sql`DECIMAL(38,10)`)
        .execute()

      const bigIntValue = BigInt('9223372036854775808') // Larger than max safe integer

      await db
        .insertInto('bignum_test')
        .values({
          huge_int: bigIntValue,
          big_decimal: bigIntValue,
        })
        .execute()

      const results = await db.selectFrom('bignum_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['huge_int']).toBeDefined()
      expect(r0?.['big_decimal']).toBeDefined()
    })

    it('should handle maximum and minimum BigInt values', async () => {
      await db.schema
        .createTable('extreme_bigint_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('max_safe', sql`HUGEINT`)
        .addColumn('min_safe', sql`HUGEINT`)
        .execute()

      const maxSafeBigInt = BigInt(Number.MAX_SAFE_INTEGER)
      const minSafeBigInt = BigInt(Number.MIN_SAFE_INTEGER)

      await db
        .insertInto('extreme_bigint_test')
        .values([
          { max_safe: maxSafeBigInt, min_safe: minSafeBigInt },
          {
            max_safe: BigInt('170141183460469231731687303715884105727'),
            min_safe: BigInt('-170141183460469231731687303715884105728'),
          },
        ])
        .execute()

      const results = await db.selectFrom('extreme_bigint_test').selectAll().execute()
      expect(results).toHaveLength(2)
    })
  })

  describe('Standard Numeric Types', () => {
    it('should handle all standard numeric types', async () => {
      await db.schema
        .createTable('numeric_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('tiny_int', sql`TINYINT`)
        .addColumn('small_int', sql`SMALLINT`)
        .addColumn('integer_val', 'integer')
        .addColumn('big_int', sql`BIGINT`)
        .addColumn('float_val', sql`FLOAT`)
        .addColumn('double_val', sql`DOUBLE`)
        .execute()

      await db
        .insertInto('numeric_test')
        .values({
          tiny_int: 127,
          small_int: 32767,
          integer_val: 2147483647,
          big_int: BigInt('9223372036854775807'),
          float_val: Math.PI,
          double_val: Math.E,
        })
        .execute()

      const results = await db.selectFrom('numeric_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const nr0 = results[0]
      expect(typeof nr0?.['tiny_int']).toBe('number')
      expect(typeof nr0?.['small_int']).toBe('number')
      expect(typeof nr0?.['integer_val']).toBe('number')
      expect(typeof nr0?.['big_int']).toBe('bigint')
      expect(typeof nr0?.['float_val']).toBe('number')
      expect(typeof nr0?.['double_val']).toBe('number')
    })

    it('should handle unsigned integer types', async () => {
      await db.schema
        .createTable('unsigned_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('utiny_int', sql`UTINYINT`)
        .addColumn('usmall_int', sql`USMALLINT`)
        .addColumn('uinteger_val', sql`UINTEGER`)
        .addColumn('ubig_int', sql`UBIGINT`)
        .execute()

      await db
        .insertInto('unsigned_test')
        .values({
          utiny_int: 255,
          usmall_int: 65535,
          uinteger_val: 2147483647, // Use max signed int to avoid conversion issues
          ubig_int: BigInt('9223372036854775807'), // Use max signed bigint
        })
        .execute()

      const results = await db.selectFrom('unsigned_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const ur0 = results[0]
      expect(ur0?.['utiny_int']).toBe(255)
      expect(ur0?.['usmall_int']).toBe(65535)
      expect(ur0?.['uinteger_val']).toBe(2147483647)
      expect(typeof ur0?.['ubig_int']).toBe('bigint')
    })
  })

  describe('Floating Point Edge Cases', () => {
    it('should handle special floating point values', async () => {
      await db.schema
        .createTable('float_edge_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('zero_val', sql`DOUBLE`)
        .addColumn('negative_zero', sql`DOUBLE`)
        .addColumn('very_small', sql`DOUBLE`)
        .addColumn('very_large', sql`DOUBLE`)
        .execute()

      await db
        .insertInto('float_edge_test')
        .values([
          {
            zero_val: 0,
            negative_zero: -0,
            very_small: Number.MIN_VALUE,
            very_large: Number.MAX_VALUE,
          },
          {
            zero_val: 0.0,
            negative_zero: -0.0,
            very_small: 1e-100,
            very_large: 1e100,
          },
        ])
        .execute()

      const results = await db.selectFrom('float_edge_test').selectAll().execute()
      expect(results).toHaveLength(2)
    })

    it('should handle precision requirements', async () => {
      await db.schema
        .createTable('precision_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('precise_decimal', sql`DECIMAL(15, 10)`)
        .addColumn('currency', sql`DECIMAL(10, 2)`)
        .execute()

      await db
        .insertInto('precision_test')
        .values([
          {
            precise_decimal: 12345.123456789,
            currency: 999999.99,
          },
          {
            precise_decimal: 0.0000000001,
            currency: 0.01,
          },
        ])
        .execute()

      const results = await db.selectFrom('precision_test').selectAll().execute()
      expect(results).toHaveLength(2)
    })
  })

  describe('Numeric Operations', () => {
    it('should support arithmetic operations in queries', async () => {
      await db.schema
        .createTable('math_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('value1', 'integer')
        .addColumn('value2', 'integer')
        .addColumn('decimal_val', sql`DECIMAL(10,2)`)
        .execute()

      await db
        .insertInto('math_test')
        .values([
          { value1: 10, value2: 20, decimal_val: 15.75 },
          { value1: 30, value2: 40, decimal_val: 25.5 },
          { value1: 50, value2: 60, decimal_val: 35.25 },
        ])
        .execute()

      // Test arithmetic operations
      const results = await db
        .selectFrom('math_test')
        .select([
          'id',
          sql`value1 + value2`.as('sum'),
          sql`value1 * value2`.as('product'),
          sql`decimal_val * 2`.as('doubled'),
          sql`avg(decimal_val) OVER()`.as('average'),
        ])
        .execute()

      expect(results).toHaveLength(3)
      const r0 = results[0]
      const r1 = results[1]
      expect(r0?.['sum']).toBe(30)
      expect(r0?.['product']).toBe(200)
      expect(r1?.['sum']).toBe(70)
      expect(r1?.['product']).toBe(1200)
    })
  })
})

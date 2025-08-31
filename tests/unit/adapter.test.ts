import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely } from 'kysely'
import { beforeEach, describe, expect, it } from 'vitest'
import { DuckDbAdapter } from '../../src/dialect/duckdb-adapter.js'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'
import { DuckDbIntrospector } from '../../src/dialect/duckdb-introspector.js'
import { DuckDbQueryCompiler } from '../../src/dialect/duckdb-query-compiler.js'

describe('DuckDbAdapter', () => {
  let adapter: DuckDbAdapter
  let database: DuckDBInstance
  let db: Kysely<any>

  beforeEach(async () => {
    adapter = new DuckDbAdapter()
    database = await DuckDBInstance.create(':memory:')

    const dialect = new DuckDbDialect({ database })
    db = new Kysely<any>({ dialect })
  })

  describe('Feature Support Properties', () => {
    it('should support transactional DDL', () => {
      expect(adapter.supportsTransactionalDdl).toBe(true)
    })

    it('should support RETURNING clause', () => {
      expect(adapter.supportsReturning).toBe(true)
    })

    it('should support CREATE IF NOT EXISTS', () => {
      expect(adapter.supportsCreateIfNotExists).toBe(true)
    })

    it('should support DROP IF EXISTS', () => {
      expect(adapter.supportsDropIfExists).toBe(true)
    })

    it('should support Common Table Expressions (CTE)', () => {
      expect(adapter.supportsCte).toBe(true)
    })

    it('should support recursive CTE', () => {
      expect(adapter.supportsRecursiveCte).toBe(true)
    })

    it('should support JSON aggregation functions', () => {
      expect(adapter.supportsJsonAgg).toBe(true)
    })

    it('should support JSON_ARRAY_FROM function', () => {
      expect(adapter.supportsJsonArrayFrom).toBe(true)
    })

    it('should support JSON_OBJECT_FROM function', () => {
      expect(adapter.supportsJsonObjectFrom).toBe(true)
    })

    it('should support window functions', () => {
      expect(adapter.supportsWindowFunctions).toBe(true)
    })

    it('should support ORDER BY NULLS FIRST/LAST', () => {
      expect(adapter.supportsOrderByNullsFirstLast).toBe(true)
    })

    it('should NOT support multi-table UPDATE', () => {
      expect(adapter.supportsUpdateMultitable).toBe(false)
    })

    it('should support DELETE with USING clause', () => {
      expect(adapter.supportsDeleteUsing).toBe(true)
    })

    it('should support INSERT ON CONFLICT', () => {
      expect(adapter.supportsInsertOnConflict).toBe(true)
    })

    it('should support INSERT OR IGNORE', () => {
      expect(adapter.supportsInsertOrIgnore).toBe(true)
    })

    it('should support INSERT OR REPLACE', () => {
      expect(adapter.supportsInsertOrReplace).toBe(true)
    })

    it('should support UPSERT operations', () => {
      expect(adapter.supportsUpsert).toBe(true)
    })

    it('should support ARRAY_AGG function', () => {
      expect(adapter.supportsArrayAgg).toBe(true)
    })

    it('should support STRING_AGG function', () => {
      expect(adapter.supportsStringAgg).toBe(true)
    })

    it('should support BOOL_AND function', () => {
      expect(adapter.supportsBoolAnd).toBe(true)
    })

    it('should support BOOL_OR function', () => {
      expect(adapter.supportsBoolOr).toBe(true)
    })

    it('should support EVERY function', () => {
      expect(adapter.supportsEvery).toBe(true)
    })
  })

  describe('Factory Methods', () => {
    it('should throw error when createDriver is called directly', () => {
      expect(() => adapter.createDriver()).toThrow(
        'DuckDbAdapter.createDriver() should not be called directly',
      )
    })

    it('should create DuckDbQueryCompiler instance', () => {
      const compiler = adapter.createQueryCompiler()
      expect(compiler).toBeInstanceOf(DuckDbQueryCompiler)
    })

    it('should create DuckDbIntrospector instance with database', () => {
      const introspector = adapter.createIntrospector(db)
      expect(introspector).toBeInstanceOf(DuckDbIntrospector)
    })

    it('should handle any type for createIntrospector parameter', () => {
      // Test that the method accepts any Kysely instance type
      const mockDb = {} as Kysely<{ users: { id: number; name: string } }>
      const introspector = adapter.createIntrospector(mockDb)
      expect(introspector).toBeInstanceOf(DuckDbIntrospector)
    })
  })

  describe('Migration Lock Management', () => {
    it('should acquire migration lock without error', async () => {
      await expect(adapter.acquireMigrationLock()).resolves.toBeUndefined()
    })

    it('should release migration lock without error', async () => {
      await expect(adapter.releaseMigrationLock()).resolves.toBeUndefined()
    })

    it('should handle multiple lock acquisitions gracefully', async () => {
      await expect(
        Promise.all([
          adapter.acquireMigrationLock(),
          adapter.acquireMigrationLock(),
          adapter.acquireMigrationLock(),
        ]),
      ).resolves.toEqual([undefined, undefined, undefined])
    })

    it('should handle lock release without prior acquisition', async () => {
      await expect(adapter.releaseMigrationLock()).resolves.toBeUndefined()
    })

    it('should handle rapid acquire/release cycles', async () => {
      for (let i = 0; i < 10; i++) {
        await adapter.acquireMigrationLock()
        await adapter.releaseMigrationLock()
      }
      // Should not throw or hang
    })
  })

  describe('Error Handling and Edge Cases', () => {
    it('should handle createIntrospector with null database gracefully', () => {
      const nullDb = null as any
      expect(() => adapter.createIntrospector(nullDb)).not.toThrow()
    })

    it('should maintain consistent behavior across multiple instances', () => {
      const adapter2 = new DuckDbAdapter()

      // All instances should have identical feature support
      expect(adapter.supportsTransactionalDdl).toBe(adapter2.supportsTransactionalDdl)
      expect(adapter.supportsReturning).toBe(adapter2.supportsReturning)
      expect(adapter.supportsCte).toBe(adapter2.supportsCte)
      expect(adapter.supportsUpdateMultitable).toBe(adapter2.supportsUpdateMultitable)
    })

    it('should create distinct introspector instances', () => {
      const introspector1 = adapter.createIntrospector(db)
      const introspector2 = adapter.createIntrospector(db)

      expect(introspector1).not.toBe(introspector2)
      expect(introspector1).toBeInstanceOf(DuckDbIntrospector)
      expect(introspector2).toBeInstanceOf(DuckDbIntrospector)
    })

    it('should create distinct query compiler instances', () => {
      const compiler1 = adapter.createQueryCompiler()
      const compiler2 = adapter.createQueryCompiler()

      expect(compiler1).not.toBe(compiler2)
      expect(compiler1).toBeInstanceOf(DuckDbQueryCompiler)
      expect(compiler2).toBeInstanceOf(DuckDbQueryCompiler)
    })
  })

  describe('Feature Support Validation', () => {
    it('should have consistent feature support for related features', () => {
      // JSON-related features should all be supported together
      expect(adapter.supportsJsonAgg).toBe(true)
      expect(adapter.supportsJsonArrayFrom).toBe(true)
      expect(adapter.supportsJsonObjectFrom).toBe(true)

      // Aggregation features should be consistent
      expect(adapter.supportsArrayAgg).toBe(true)
      expect(adapter.supportsStringAgg).toBe(true)
      expect(adapter.supportsBoolAnd).toBe(true)
      expect(adapter.supportsBoolOr).toBe(true)
      expect(adapter.supportsEvery).toBe(true)

      // INSERT variations should be consistent
      expect(adapter.supportsInsertOnConflict).toBe(true)
      expect(adapter.supportsInsertOrIgnore).toBe(true)
      expect(adapter.supportsInsertOrReplace).toBe(true)
      expect(adapter.supportsUpsert).toBe(true)
    })

    it('should correctly identify limitations', () => {
      // DuckDB-specific limitations
      expect(adapter.supportsUpdateMultitable).toBe(false)
    })
  })

  describe('Performance and Memory', () => {
    it('should not leak memory with repeated method calls', () => {
      const initialMemory = process.memoryUsage().heapUsed

      // Create many instances to test for memory leaks
      for (let i = 0; i < 1000; i++) {
        adapter.createQueryCompiler()
        adapter.createIntrospector(db)
      }

      const finalMemory = process.memoryUsage().heapUsed
      const memoryIncrease = finalMemory - initialMemory

      // Memory increase should be reasonable (less than 10MB)
      expect(memoryIncrease).toBeLessThan(10 * 1024 * 1024)
    })

    it('should execute lock operations quickly', async () => {
      const startTime = Date.now()

      await adapter.acquireMigrationLock()
      await adapter.releaseMigrationLock()

      const duration = Date.now() - startTime
      expect(duration).toBeLessThan(100) // Should complete in less than 100ms
    })
  })
})

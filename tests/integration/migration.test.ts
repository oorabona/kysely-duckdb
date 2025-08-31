import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, Migrator } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'
import { InMemoryMigrationProvider } from '../../src/migrations/provider.js'

describe('DuckDB Migrations', () => {
  let database: DuckDBInstance
  let db: Kysely<any>
  let migrator: Migrator

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')

    const dialect = new DuckDbDialect({
      database,
    })

    db = new Kysely({
      dialect,
    })

    const provider = new InMemoryMigrationProvider({
      '001_initial': {
        async up(db) {
          await db.schema
            .createTable('users')
            .addColumn('id', 'integer', col => col.primaryKey())
            .addColumn('name', 'varchar(255)')
            .execute()
        },
        async down(db) {
          await db.schema.dropTable('users').execute()
        },
      },
      '002_posts': {
        async up(db) {
          await db.schema
            .createTable('posts')
            .addColumn('id', 'integer', col => col.primaryKey())
            .addColumn('title', 'varchar(255)')
            .addColumn('user_id', 'integer')
            .execute()
        },
        async down(db) {
          await db.schema.dropTable('posts').execute()
        },
      },
    })

    migrator = new Migrator({ db, provider })
  })

  afterEach(async () => {
    try {
      if (db) {
        await db.destroy()
      }
    } catch {
      // Ignore cleanup errors
    }
    try {
      if (database) {
        database.closeSync()
      }
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('Basic Migration Operations', () => {
    it('should migrate to latest', async () => {
      const result = await migrator.migrateToLatest()

      expect(result.error).toBeUndefined()
      expect(result.results).toHaveLength(2)

      // Verify tables were created
      const tables = await db.introspection.getTables()
      const tableNames = tables.map(t => t.name)

      expect(tableNames).toContain('users')
      expect(tableNames).toContain('posts')
    })

    it('should migrate down', async () => {
      // First migrate up
      await migrator.migrateToLatest()

      // Verify tables exist
      const tablesAfterMigration = await db.introspection.getTables()
      expect(tablesAfterMigration.map(t => t.name)).toContain('users')

      const result = await migrator.migrateDown()
      expect(result.error).toBeUndefined()

      // Should have rolled back one migration
      const tablesAfterRollback = await db.introspection.getTables()
      const tableNames = tablesAfterRollback.map(t => t.name)
      expect(tableNames).not.toContain('posts')
      expect(tableNames).toContain('users')
    })

    it('should handle migration errors', async () => {
      const providerWithError = new InMemoryMigrationProvider({
        '001_error': {
          async up() {
            throw new Error('Intentional migration error')
          },
          async down() {
            // No-op
          },
        },
      })

      const errorMigrator = new Migrator({ db, provider: providerWithError })
      const result = await errorMigrator.migrateToLatest()

      expect(result.error).toBeDefined()
      if (result.error) {
        const msg =
          typeof (result.error as any).message === 'string'
            ? (result.error as any).message
            : String(result.error)
        expect(msg).toContain('Intentional migration error')
      }
    })
  })
})

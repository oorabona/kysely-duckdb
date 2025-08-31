import { describe, expect, it } from 'vitest'
import { InMemoryMigrationProvider } from '../../src/migrations/provider.js'
import {
  createDuckDbMigrator,
  createExternalTable,
  createIndex,
  createOptimizedTable,
  createSequence,
  DuckDbMigrationUtils,
  dropSequence,
  enableExtension,
} from '../../src/migrations/utils.js'

describe('Migration Utils', () => {
  describe('createDuckDbMigrator', () => {
    it('should create migrator with default provider', () => {
      const mockDb = {
        executeQuery: () => Promise.resolve({ rows: [] }),
      } as any

      const migrator = createDuckDbMigrator({
        db: mockDb,
      })

      expect(migrator).toBeDefined()
      expect(typeof migrator.migrateToLatest).toBe('function')
    })

    it('should create migrator with custom provider', () => {
      const mockDb = {
        executeQuery: () => Promise.resolve({ rows: [] }),
      } as any

      const customProvider = new InMemoryMigrationProvider({})

      const migrator = createDuckDbMigrator({
        db: mockDb,
        provider: customProvider,
        migrationTableName: 'custom_migrations',
        migrationTableSchema: 'custom_schema',
        lockTableName: 'custom_lock',
        allowUnorderedMigrations: true,
      })

      expect(migrator).toBeDefined()
    })

    it('should create migrator with custom migration folder', () => {
      const mockDb = {
        executeQuery: () => Promise.resolve({ rows: [] }),
      } as any

      const migrator = createDuckDbMigrator({
        db: mockDb,
        migrationFolder: './custom/migrations',
      })

      expect(migrator).toBeDefined()
    })
  })

  describe('createOptimizedTable', () => {
    it('should create optimized table SQL', () => {
      const sql = createOptimizedTable('users')
      expect(sql).toContain('CREATE TABLE IF NOT EXISTS users')
      expect(sql).toContain('id INTEGER PRIMARY KEY')
      expect(sql).toContain('created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
      expect(sql).toContain('updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
    })
  })

  describe('createIndex', () => {
    it('should create regular index', () => {
      const sql = createIndex('users', 'email')
      expect(sql).toBe('CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)')
    })

    it('should create unique index', () => {
      const sql = createIndex('users', 'email', true)
      expect(sql).toBe('CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email)')
    })
  })

  describe('createExternalTable', () => {
    it('should create CSV external table', () => {
      const sql = createExternalTable('users', 'users.csv', 'csv')
      expect(sql).toBe("CREATE OR REPLACE VIEW users AS SELECT * FROM read_csv('users.csv')")
    })

    it('should create JSON external table', () => {
      const sql = createExternalTable('data', 'data.json', 'json')
      expect(sql).toBe("CREATE OR REPLACE VIEW data AS SELECT * FROM read_json('data.json')")
    })

    it('should create Parquet external table', () => {
      const sql = createExternalTable('analytics', 'analytics.parquet', 'parquet')
      expect(sql).toBe(
        "CREATE OR REPLACE VIEW analytics AS SELECT * FROM read_parquet('analytics.parquet')",
      )
    })

    it('should default to CSV format', () => {
      const sql = createExternalTable('default_table', 'data.txt')
      expect(sql).toBe("CREATE OR REPLACE VIEW default_table AS SELECT * FROM read_csv('data.txt')")
    })
  })

  describe('enableExtension', () => {
    it('should create extension enable SQL', () => {
      const sql = enableExtension('spatial')
      expect(sql).toBe('INSTALL spatial; LOAD spatial;')
    })
  })

  describe('createSequence', () => {
    it('should create sequence with default start value', () => {
      const sql = createSequence('user_id_seq')
      expect(sql).toBe('CREATE SEQUENCE IF NOT EXISTS user_id_seq START 1')
    })

    it('should create sequence with custom start value', () => {
      const sql = createSequence('order_id_seq', 1000)
      expect(sql).toBe('CREATE SEQUENCE IF NOT EXISTS order_id_seq START 1000')
    })
  })

  describe('dropSequence', () => {
    it('should create drop sequence SQL', () => {
      const sql = dropSequence('user_id_seq')
      expect(sql).toBe('DROP SEQUENCE IF EXISTS user_id_seq')
    })
  })

  describe('DuckDbMigrationUtils object', () => {
    it('should export all utility functions', () => {
      expect(typeof DuckDbMigrationUtils.createOptimizedTable).toBe('function')
      expect(typeof DuckDbMigrationUtils.createIndex).toBe('function')
      expect(typeof DuckDbMigrationUtils.createExternalTable).toBe('function')
      expect(typeof DuckDbMigrationUtils.enableExtension).toBe('function')
      expect(typeof DuckDbMigrationUtils.createSequence).toBe('function')
      expect(typeof DuckDbMigrationUtils.dropSequence).toBe('function')
    })

    it('should work through the object interface', () => {
      const sql1 = DuckDbMigrationUtils.createOptimizedTable('test')
      expect(sql1).toContain('CREATE TABLE IF NOT EXISTS test')

      const sql2 = DuckDbMigrationUtils.createIndex('test', 'id', true)
      expect(sql2).toContain('UNIQUE INDEX')

      const sql3 = DuckDbMigrationUtils.enableExtension('json')
      expect(sql3).toBe('INSTALL json; LOAD json;')
    })
  })
})

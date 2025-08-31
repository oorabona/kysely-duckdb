/**
 * DuckDB migration utilities and helpers
 */

import type { Kysely, MigrationProvider, Migrator } from 'kysely'
import { FileMigrationProvider } from './provider.js'

/**
 * Create a Kysely migrator configured for DuckDB
 */
export function createDuckDbMigrator(config: {
  db: Kysely<any>
  migrationFolder?: string
  provider?: MigrationProvider
  migrationTableName?: string
  migrationTableSchema?: string
  lockTableName?: string
  allowUnorderedMigrations?: boolean
}): Migrator {
  return new (require('kysely').Migrator)({
    db: config.db,
    provider:
      config.provider || new FileMigrationProvider(config.migrationFolder || './migrations'),
    migrationTableName: config.migrationTableName,
    migrationTableSchema: config.migrationTableSchema,
    migrationLockTableName: config.lockTableName,
    allowUnorderedMigrations: config.allowUnorderedMigrations,
  })
}

// DuckDB Migration Utilities - Individual exports for better tree-shaking

/**
 * Create DuckDB-specific tables with optimizations
 */
export function createOptimizedTable(tableName: string): string {
  return `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id INTEGER PRIMARY KEY,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `
}

/**
 * Add DuckDB-specific indexes
 */
export function createIndex(
  tableName: string,
  columnName: string,
  unique: boolean = false,
): string {
  const uniqueStr = unique ? 'UNIQUE ' : ''
  return `CREATE ${uniqueStr}INDEX IF NOT EXISTS idx_${tableName}_${columnName} ON ${tableName} (${columnName})`
}

/**
 * Create external table mapping
 */
export function createExternalTable(
  tableName: string,
  filePath: string,
  format: 'csv' | 'json' | 'parquet' = 'csv',
): string {
  const readFunction =
    format === 'csv' ? 'read_csv' : format === 'json' ? 'read_json' : 'read_parquet'
  return `CREATE OR REPLACE VIEW ${tableName} AS SELECT * FROM ${readFunction}('${filePath}')`
}

/**
 * Enable DuckDB extension in migration
 */
export function enableExtension(extensionName: string): string {
  return `INSTALL ${extensionName}; LOAD ${extensionName};`
}

/**
 * Create DuckDB sequence (auto-increment alternative)
 */
export function createSequence(sequenceName: string, startValue: number = 1): string {
  return `CREATE SEQUENCE IF NOT EXISTS ${sequenceName} START ${startValue}`
}

/**
 * Drop sequence
 */
export function dropSequence(sequenceName: string): string {
  return `DROP SEQUENCE IF EXISTS ${sequenceName}`
}

// Backward compatibility - export object for existing code
export const DuckDbMigrationUtils = {
  createOptimizedTable,
  createIndex,
  createExternalTable,
  enableExtension,
  createSequence,
  dropSequence,
} as const

// import type Database from '@duckdb/node-api'
import {
  type DatabaseIntrospector,
  type Dialect,
  type DialectAdapter,
  type Driver,
  type Kysely,
  type QueryCompiler,
  sql,
} from 'kysely'
import { checkEnvironment } from '../internal/version-check.js'
import type { DuckDbDialectConfig, InternalDuckDbDialectConfig } from '../types/data-types.js'
import { DuckDbAdapter } from './duckdb-adapter.js'
import { DuckDbDriver } from './duckdb-driver.js'
import { DuckDbIntrospector } from './duckdb-introspector.js'
import { DuckDbQueryCompiler } from './duckdb-query-compiler.js'

/**
 * DuckDB dialect for Kysely
 */
export class DuckDbDialect implements Dialect {
  readonly #config: InternalDuckDbDialectConfig
  readonly #driver: DuckDbDriver
  readonly #adapter: DuckDbAdapter
  readonly #queryCompiler: DuckDbQueryCompiler

  constructor(config: DuckDbDialectConfig) {
    this.#config = {
      uuidAsString: false,
      tableMappings: {},
      config: {},
      ...config,
    }

    // Initialize driver with database instance and options
    this.#driver = new DuckDbDriver(this.#config)
    this.#adapter = new DuckDbAdapter()
    this.#queryCompiler = new DuckDbQueryCompiler()

    // Validate environment on construction
    this.#validateEnvironment()
  }

  /**
   * Validate environment compatibility (async, doesn't block construction)
   */
  /* c8 ignore start */
  /* v8 ignore start */
  // Disabled coverage as this is hard to test in CI
  async #validateEnvironment(): Promise<void> {
    try {
      const result = await checkEnvironment()
      if (!result.overall) {
        console.warn('kysely-duckdb compatibility warnings:')
        for (const warning of result.allWarnings) {
          console.warn(`  - ${warning}`)
        }
      }
    } catch (error) {
      console.warn('Failed to validate environment:', error)
    }
  }
  /* v8 ignore stop */
  /* c8 ignore stop */

  createDriver(): Driver {
    return this.#driver
  }

  createQueryCompiler(): QueryCompiler {
    return this.#queryCompiler
  }

  createAdapter(): DialectAdapter {
    return this.#adapter
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    // any required for generic database schema
    return new DuckDbIntrospector(db)
  }

  /**
   * Setup table mappings for external data sources
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async setupTableMappings(db: Kysely<any>): Promise<void> {
    for (const [tableName, mapping] of Object.entries(this.#config.tableMappings)) {
      if (typeof mapping === 'string') {
        // Simple string mapping
        await sql`CREATE OR REPLACE VIEW ${sql.ref(tableName)} AS SELECT * FROM ${sql.ref(mapping)}`.execute(
          db,
        )
      } else {
        // Complex mapping with options
        const { source, options = {} } = mapping
        const optionsStr = Object.entries(options)
          .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
          .join(', ')

        const readFunction = this.#getReadFunction(source)
        const sqlQuery = optionsStr
          ? `CREATE OR REPLACE VIEW ${tableName} AS SELECT * FROM ${readFunction}('${source}', ${optionsStr})`
          : `CREATE OR REPLACE VIEW ${tableName} AS SELECT * FROM ${readFunction}('${source}')`

        await sql.raw(sqlQuery).execute(db)
      }
    }
  }

  /**
   * Get the appropriate DuckDB read function based on file extension
   */
  #getReadFunction(source: string): string {
    const extension = source.toLowerCase().split('.').pop()

    switch (extension) {
      case 'json':
      case 'jsonl':
      case 'ndjson':
        return 'read_json'
      case 'csv':
        return 'read_csv'
      case 'parquet':
        return 'read_parquet'
      case 'arrow':
        return 'read_arrow'
      case 'excel':
      case 'xlsx':
        return 'read_excel'
      default:
        return 'read_csv' // Default fallback
    }
  }

  /**
   * Load DuckDB extension
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async loadExtension(db: Kysely<any>, extensionName: string): Promise<void> {
    // any required for generic database schema
    await sql`INSTALL ${sql.lit(extensionName)}`.execute(db)
    await sql`LOAD ${sql.lit(extensionName)}`.execute(db)
  }

  /**
   * Load commonly used extensions
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async loadCommonExtensions(db: Kysely<any>): Promise<void> {
    // any required for generic database schema
    const extensions = ['spatial', 'json', 'httpfs']

    for (const extension of extensions) {
      try {
        await this.loadExtension(db, extension)
      } catch (error) {
        // Silently ignore extension loading errors
        console.warn(`Failed to load DuckDB extension '${extension}':`, error)
      }
    }
  }
}

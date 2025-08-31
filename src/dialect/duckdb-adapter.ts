import type { DatabaseIntrospector, DialectAdapter, Driver, Kysely, QueryCompiler } from 'kysely'
import { DuckDbIntrospector } from './duckdb-introspector.js'
import { DuckDbQueryCompiler } from './duckdb-query-compiler.js'

/**
 * DuckDB dialect adapter that handles database-specific operations
 */
export class DuckDbAdapter implements DialectAdapter {
  get supportsTransactionalDdl(): boolean {
    return true
  }

  get supportsReturning(): boolean {
    return true
  }

  get supportsCreateIfNotExists(): boolean {
    return true
  }

  get supportsDropIfExists(): boolean {
    return true
  }

  get supportsCte(): boolean {
    return true
  }

  get supportsRecursiveCte(): boolean {
    return true
  }

  get supportsJsonAgg(): boolean {
    return true
  }

  get supportsJsonArrayFrom(): boolean {
    return true
  }

  get supportsJsonObjectFrom(): boolean {
    return true
  }

  get supportsWindowFunctions(): boolean {
    return true
  }

  get supportsOrderByNullsFirstLast(): boolean {
    return true
  }

  get supportsUpdateMultitable(): boolean {
    return false
  }

  get supportsDeleteUsing(): boolean {
    return true
  }

  get supportsInsertOnConflict(): boolean {
    return true
  }

  get supportsInsertOrIgnore(): boolean {
    return true
  }

  get supportsInsertOrReplace(): boolean {
    return true
  }

  get supportsUpsert(): boolean {
    return true
  }

  get supportsArrayAgg(): boolean {
    return true
  }

  get supportsStringAgg(): boolean {
    return true
  }

  get supportsBoolAnd(): boolean {
    return true
  }

  get supportsBoolOr(): boolean {
    return true
  }

  get supportsEvery(): boolean {
    return true
  }

  createDriver(): Driver {
    throw new Error('DuckDbAdapter.createDriver() should not be called directly')
  }

  createQueryCompiler(): QueryCompiler {
    return new DuckDbQueryCompiler()
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    // any required for generic database schema
    return new DuckDbIntrospector(db)
  }

  acquireMigrationLock(): Promise<void> {
    // DuckDB doesn't require explicit migration locks as it's single-writer
    return Promise.resolve()
  }

  releaseMigrationLock(): Promise<void> {
    // DuckDB doesn't require explicit migration locks as it's single-writer
    return Promise.resolve()
  }
}

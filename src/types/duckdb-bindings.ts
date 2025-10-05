/**
 * Strict type definitions for DuckDB native bindings
 * Replaces 'any' types with proper TypeScript types
 */

/**
 * DuckDB UUID object representation
 */
export interface DuckDBUUIDObject {
  hugeint?: {
    toString(radix?: number): string
  }
}

/**
 * DuckDB column type metadata
 */
export interface DuckDBColumnType {
  alias: string
  sql_type?: string
  type_id?: number
}

/**
 * Constructor type for DuckDB UUID values
 */
export type DuckDBUUIDConstructor = new (...args: unknown[]) => object

/**
 * DuckDB pending result state enum
 */
export enum DuckDBPendingResultState {
  RESULT_READY = 0,
  RESULT_NOT_READY = 1,
  RESULT_ERROR = 2,
}

/**
 * Logger arguments - supports any serializable type
 * Using unknown for maximum flexibility in logging
 */
export type LoggerArgs = readonly unknown[]

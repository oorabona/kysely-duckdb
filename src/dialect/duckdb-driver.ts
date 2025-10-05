import * as DuckDBAPI from '@duckdb/node-api'
import {
  type DuckDBConnection,
  type DuckDBInstance,
  DuckDBListValue,
  type DuckDBResultReader,
  type DuckDBValue,
} from '@duckdb/node-api'
import type { DatabaseConnection, Driver, TransactionSettings } from 'kysely'
import { CompiledQuery, type QueryResult } from 'kysely'
import { logger } from '../internal/logger.js'
import { globalPerformanceMonitor } from '../internal/performance-monitor.js'
import type { InternalDuckDbDialectConfig } from '../types/data-types.js'
import type {
  DuckDBColumnType,
  DuckDBUUIDConstructor,
  DuckDBUUIDObject,
} from '../types/duckdb-bindings.js'

// Precompute minimal UUID helpers at module scope to avoid per-call detection overhead
// Note: DuckDBUUIDValue class is not always exposed by the runtime API
//       so we need to do some duck typing here
/* c8 ignore start */
/* v8 ignore start */
// Disabled coverage as this is hard to test in CI
const UUIDCtor: DuckDBUUIDConstructor | undefined =
  typeof (DuckDBAPI as Record<string, unknown>)['DuckDBUUIDValue'] === 'function'
    ? ((DuckDBAPI as Record<string, unknown>)['DuckDBUUIDValue'] as DuckDBUUIDConstructor)
    : undefined
// Pending result ready state: try to read from runtime API, fallback to 0
const RESULT_READY: number =
  (
    (DuckDBAPI as Record<string, unknown>)['DuckDBPendingResultState'] as
      | Record<string, number>
      | undefined
  )?.['RESULT_READY'] ??
  (
    (DuckDBAPI as Record<string, unknown>)['PendingResultState'] as
      | Record<string, number>
      | undefined
  )?.['RESULT_READY'] ??
  0
/* c8 ignore end */
/* v8 ignore end */

/**
 * Check if a value is an instance of DuckDBUUIDValue
 */
function isDuckDBUUIDInstance(val: unknown): boolean {
  return Boolean(
    UUIDCtor && typeof val === 'object' && val !== null && (val as object) instanceof UUIDCtor,
  )
}

// Helper hoisted to module scope to avoid re-allocation per call.
function tryHugeint(val: unknown): string | null {
  try {
    if (val && typeof val === 'object' && 'hugeint' in val) {
      const hugeint = (val as DuckDBUUIDObject).hugeint
      if (hugeint && typeof hugeint.toString === 'function') {
        const hexString = hugeint.toString(16).padStart(32, '0')
        const formatted = hexString.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5')
        return formatted
      }
    }
  } catch (error) {
    logger.debug('UUID hugeint conversion failed:', error)
  }
  return null
}

/**
 * Convert DuckDB UUID object to string representation
 */
function convertUuidToString(uuidValue: unknown): string {
  // If it's already a string, return as-is
  if (typeof uuidValue === 'string') {
    return uuidValue
  }

  // Handle DuckDB UUID object types
  const s = String(uuidValue)

  // Prefer concrete class if exposed by the runtime API (cached at module scope)
  if (isDuckDBUUIDInstance(uuidValue)) {
    return s
  }

  if (uuidValue && typeof uuidValue === 'object') {
    // Graceful fallbacks for unknown wrappers
    const viaHugeint = tryHugeint(uuidValue)
    if (viaHugeint) {
      return viaHugeint
    }
  }

  // Last resort: convert to string or return as-is if it's already a string
  return s
}

/**
 * Prepare SQL query with named parameters for DuckDB
 */
function prepareSQLWithParams(compiledQuery: CompiledQuery): {
  sql: string
  namedParams: Record<string, DuckDBValue>
} {
  if (compiledQuery.parameters.length === 0) {
    return { sql: compiledQuery.sql, namedParams: {} }
  }

  // Convert positional parameters to named parameters for @duckdb/node-api
  const namedParams: Record<string, DuckDBValue> = {}
  compiledQuery.parameters.forEach((param, index) => {
    // Debug logging for parameters
    logger.debug(`Parameter ${index + 1}:`, typeof param, param, param?.constructor?.name)

    // Debug logging for UUID handling
    if (
      typeof param === 'string' &&
      param.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i)
    ) {
      logger.debug(`UUID parameter ${index + 1}:`, typeof param, param)
    }

    // Convert parameters to DuckDB-compatible types
    let duckDbParam: DuckDBValue

    if (param instanceof Date) {
      // Convert JavaScript Date objects to ISO string format for DuckDB
      duckDbParam = param.toISOString() as DuckDBValue
      logger.debug(`Converted Date to ISO string:`, param, '=>', duckDbParam)
    } else if (Array.isArray(param)) {
      // Convert JavaScript arrays to DuckDB LIST format: [1,2,3] -> "[1, 2, 3]"
      // This works for both ARRAY and LIST types in DuckDB
      const listString = `[${param
        .map(item => {
          if (typeof item === 'string') {
            return `'${item.replace(/'/g, "''")}'`
          } // Escape quotes
          if (item === null) {
            return 'NULL'
          }
          return String(item)
        })
        .join(', ')}]`
      duckDbParam = listString as DuckDBValue
      logger.debug(`Converted Array to LIST format:`, param, '=>', duckDbParam)
    } else if (param instanceof Map) {
      // Convert JavaScript Map to DuckDB MAP bracket syntax: {'key1': value1, 'key2': value2}
      const entries = Array.from(param.entries())
      const mapEntries = entries
        .map(([key, value]) => {
          const keyStr = typeof key === 'string' ? `'${key.replace(/'/g, "''")}'` : String(key)
          const valueStr =
            typeof value === 'string'
              ? `'${value.replace(/'/g, "''")}'`
              : value === null
                ? 'NULL'
                : String(value)
          return `${keyStr}: ${valueStr}`
        })
        .join(', ')
      const mapString = `{${mapEntries}}`
      duckDbParam = mapString as DuckDBValue
      logger.debug(`Converted Map to MAP bracket format:`, param, '=>', duckDbParam)
    } else if (param instanceof Buffer) {
      // Convert Node.js Buffer to DuckDB BLOB format
      // Use hex encoding for BLOB data
      const hexString = `'\\x${param.toString('hex')}'`
      duckDbParam = hexString as DuckDBValue
      logger.debug(`Converted Buffer to BLOB hex:`, param, '=>', duckDbParam)
    } else if (param instanceof Uint8Array) {
      // Convert Uint8Array to DuckDB BLOB format
      const buffer = Buffer.from(param)
      const hexString = `'\\x${buffer.toString('hex')}'`
      duckDbParam = hexString as DuckDBValue
      logger.debug(`Converted Uint8Array to BLOB hex:`, param, '=>', duckDbParam)
    } else if (typeof param === 'object' && param !== null && param.constructor === Object) {
      // Convert plain JavaScript objects to JSON strings for DuckDB JSON columns
      duckDbParam = JSON.stringify(param) as DuckDBValue
      logger.debug(`Converted Object to JSON string:`, param, '=>', duckDbParam)
    } else {
      // Basic types (null, boolean, number, bigint, string) are compatible
      duckDbParam = param as DuckDBValue
    }

    namedParams[`param${index + 1}`] = duckDbParam
  })

  // Build final SQL with parameter placeholders normalized to $paramN
  // Supported inputs:
  //  - Already-named params: $param1, $param2, ... (no replacement)
  //  - PostgreSQL-style: $1, $2, ... (rewrite to $param1, $param2, ...)
  //  - Positional: ? (rewrite sequentially to $param1, $param2, ...)
  let sql = compiledQuery.sql

  if (/\$param\d+/.test(sql)) {
    // Already named consistently, no changes needed
  } else if (/\$\d+/.test(sql)) {
    // Rewrite $1, $2, ... to $param1, $param2, ... without changing numbering
    sql = sql.replace(/\$(\d+)/g, (_m, n: string) => `$param${n}`)
  } else if (/[?]/.test(sql)) {
    let seq = 1
    sql = sql.replace(/\?/g, () => `$param${seq++}`)
  }

  return { sql, namedParams }
}

/**
 * Process rows by parsing JSON columns based on metadata and converting DuckDBListValue to arrays
 * Uses efficient caching to avoid repeated computations
 */
function processRows(
  rows: unknown[],
  result: DuckDBResultReader,
  uuidAsString: boolean,
): unknown[] {
  // Get column metadata to identify JSON columns (cached per result)
  const columnTypesRaw = result.columnTypesJson()
  const columnNames = result.deduplicatedColumnNames()

  // DuckDB columnTypesJson() always returns an array - if not, there's a serious bug
  const columnTypes = Array.isArray(columnTypesRaw)
    ? (columnTypesRaw as unknown as DuckDBColumnType[])
    : []

  // Create sets of column indices for efficient lookup
  const jsonColumnIndices = new Set(
    columnTypes
      .map((type: DuckDBColumnType, idx: number) => {
        // Type should be an object with alias property
        return type.alias === 'JSON' ? idx : null
      })
      .filter((idx: number | null) => idx !== null),
  )

  // Create a set of UUID column indices for efficient lookup (inlined in condition below)

  // Create column name to index mapping for efficient lookups
  const columnNameToIndex = new Map<string, number>()
  columnNames.forEach((name, index: number) => {
    columnNameToIndex.set(name, index)
  })

  return rows.map((row: unknown) => {
    // Assume DuckDB always returns valid objects, no need for defensive checks
    const rowObj = row as Record<string, unknown>
    const parsedRow = { ...rowObj }
    const keys = Object.keys(rowObj)

    keys.forEach(key => {
      const value = parsedRow[key]
      const columnIndex = columnNameToIndex.get(key)

      if (columnIndex !== undefined) {
        // Parse JSON columns (guaranteed valid by DuckDB)
        if (jsonColumnIndices.has(columnIndex) && typeof value === 'string') {
          parsedRow[key] = JSON.parse(value)
        }
        // Convert UUID objects to strings
        else if (isDuckDBUUIDInstance(value) && value != null && uuidAsString) {
          parsedRow[key] = convertUuidToString(value)
        }
        // Convert DuckDBListValue to plain arrays
        else if (value instanceof DuckDBListValue) {
          parsedRow[key] = value.items
        }
      }
    })

    return parsedRow
  })
}

/**
 * DuckDB driver for Kysely using @duckdb/node-api
 */
export class DuckDbDriver implements Driver {
  readonly #config: InternalDuckDbDialectConfig

  constructor(config: InternalDuckDbDialectConfig) {
    this.#config = config
  }

  async init(): Promise<void> {
    // DuckDB connection is already initialized
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    return new DuckDbConnection(this.#config.database, this.#config)
  }

  async beginTransaction(
    connection: DatabaseConnection,
    _settings: TransactionSettings,
  ): Promise<void> {
    const duckConnection = connection as DuckDbConnection
    await duckConnection.beginTransaction()

    // DuckDB doesn't support SET TRANSACTION ISOLATION LEVEL syntax
    // Transaction isolation is handled automatically by DuckDB

    await connection.executeQuery(CompiledQuery.raw('BEGIN'))
  }

  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw('COMMIT'))
    const duckConnection = connection as DuckDbConnection
    await duckConnection.commitTransaction()
  }

  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    try {
      await connection.executeQuery(CompiledQuery.raw('ROLLBACK'))
    } catch (error) {
      // Only ignore specific rollback errors when no transaction is active
      if (error instanceof Error && !error.message.includes('no transaction is active')) {
        // Log unexpected rollback errors
        logger.warn('Unexpected error during rollback:', error.message)
      }
    }
    const duckConnection = connection as DuckDbConnection
    await duckConnection.rollbackTransaction()
  }

  async releaseConnection(): Promise<void> {
    // Connection is released automatically
  }

  async destroy(): Promise<void> {
    this.#config.database.closeSync()
  }
}

/**
 * DuckDB database connection implementation
 */
class DuckDbConnection implements DatabaseConnection {
  readonly #database: DuckDBInstance // DuckDB Database instance
  #connection: DuckDBConnection | null = null // Active DuckDB connection for transactions
  readonly #uuidAsString: boolean

  constructor(database: DuckDBInstance, options: InternalDuckDbDialectConfig) {
    this.#database = database
    this.#uuidAsString = options.uuidAsString
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const startTime = performance.now()

    // Debug: Log the SQL being executed
    logger.debug(`Executing SQL:`, compiledQuery.sql)
    logger.debug(`Parameters (${compiledQuery.parameters.length}):`, compiledQuery.parameters)

    try {
      // Check if this is a data-returning query using robust regex patterns
      const isSelectQuery =
        /^\s*(SELECT|WITH|SHOW|DESCRIBE|EXPLAIN|PRAGMA|CALL)\b/i.test(compiledQuery.sql) ||
        /\bRETURNING\b/i.test(compiledQuery.sql)

      let result: QueryResult<O>

      if (isSelectQuery) {
        // For SELECT queries, use streamQuery and collect all results
        const allRows: O[] = []
        for await (const chunk of this.streamQuery<O>(compiledQuery)) {
          allRows.push(...chunk.rows)
        }

        result = {
          rows: allRows,
          numAffectedRows: BigInt(0),
          numChangedRows: BigInt(0),
        }
      } else {
        // For INSERT/UPDATE/DELETE, use the direct connection approach
        const connection = this.#connection || (await this.#database.connect())
        const shouldCloseConnection = !this.#connection

        try {
          const { sql, namedParams } = prepareSQLWithParams(compiledQuery)

          const runResult =
            Object.keys(namedParams).length > 0
              ? await connection.run(sql, namedParams)
              : await connection.run(sql)

          const numAffectedRows = BigInt(runResult.rowCount || 0)

          result = {
            rows: [] as O[],
            numAffectedRows,
            numChangedRows: numAffectedRows,
          }
        } finally {
          if (shouldCloseConnection) {
            connection.closeSync()
          }
        }
      }

      // Record performance metrics
      const endTime = performance.now()
      const duration = endTime - startTime
      globalPerformanceMonitor.recordQuery({
        sql: compiledQuery.sql,
        duration,
        rowCount: result.rows.length,
        parameters: compiledQuery.parameters.slice(),
      })

      return result
    } catch (error) {
      // Record error for monitoring
      globalPerformanceMonitor.recordError()

      const message = `DuckDB query failed: ${error instanceof Error ? error.message : String(error)}`
      // Preserve original error via cause for better debugging while keeping message stable for tests
      throw new Error(message, { cause: error instanceof Error ? error : new Error(String(error)) })
    }
  }

  async beginTransaction(): Promise<void> {
    if (!this.#connection) {
      this.#connection = await this.#database.connect()
    }
  }

  async commitTransaction(): Promise<void> {
    if (this.#connection) {
      this.#connection.closeSync()
      this.#connection = null
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (this.#connection) {
      this.#connection.closeSync()
      this.#connection = null
    }
  }

  async *streamQuery<O>(
    compiledQuery: CompiledQuery,
    chunkSize = 2048,
  ): AsyncIterableIterator<QueryResult<O>> {
    // Reuse the active transaction connection if present to ensure
    // transactional consistency. Otherwise, open a temporary connection.
    const connection = this.#connection || (await this.#database.connect())
    const shouldCloseConnection = !this.#connection

    try {
      const { sql, namedParams } = prepareSQLWithParams(compiledQuery)

      // Use DuckDB's proper streaming API with startStream
      const pending =
        Object.keys(namedParams).length > 0
          ? await connection.startStream(sql, namedParams)
          : await connection.startStream(sql)

      // Wait for the stream to be ready
      while (pending.runTask() !== RESULT_READY) {
        // RESULT_READY
        // Allow the event loop to process other tasks
        await new Promise(resolve => setImmediate(resolve))
      }

      // Get the result reader for streaming
      const reader = await pending.read()

      // Stream rows in chunks respecting the chunkSize parameter
      let processedRowCount = 0
      while (!reader.done) {
        // Read next chunk of rows from DuckDB
        await reader.readUntil(reader.currentRowCount + Math.max(chunkSize, 1))

        // Get all rows read so far
        const allRows = reader.getRowObjects()

        // Process only the new rows we haven't processed yet
        if (allRows.length > processedRowCount) {
          const newRows = allRows.slice(processedRowCount)

          // Process rows: parse JSON/UUID columns and convert DuckDBListValue to arrays
          const processedRows = processRows(newRows, reader, this.#uuidAsString)

          // Yield in chunks of the requested size
          for (let i = 0; i < processedRows.length; i += chunkSize) {
            const chunk = processedRows.slice(i, i + chunkSize)
            yield {
              rows: chunk as O[],
              numAffectedRows: BigInt(0),
              numChangedRows: BigInt(0),
            }
          }

          processedRowCount = allRows.length
        }
      }
    } finally {
      if (shouldCloseConnection) {
        connection.closeSync()
      }
    }
  }
}

// Internal test hooks (not part of public API surface, but exported for test coverage)
// eslint-disable-next-line @typescript-eslint/naming-convention
export const __test = {
  convertUuidToString,
  prepareSQLWithParams,
}

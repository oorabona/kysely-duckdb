import type { DuckDBInstance } from '@duckdb/node-api'

/**
 * DuckDB-specific data types
 */
export interface DuckDbDataTypeNode {
  readonly kind: 'DuckDbDataTypeNode'
  readonly dataType: string
}

/**
 * DuckDB array type
 */
export interface DuckDbArrayNode extends DuckDbDataTypeNode {
  readonly dataType: 'ARRAY'
  readonly itemType: DuckDbDataTypeNode
}

/**
 * DuckDB struct type
 */
export interface DuckDbStructNode extends DuckDbDataTypeNode {
  readonly dataType: 'STRUCT'
  readonly fields: Record<string, DuckDbDataTypeNode>
}

/**
 * DuckDB map type
 */
export interface DuckDbMapNode extends DuckDbDataTypeNode {
  readonly dataType: 'MAP'
  readonly keyType: DuckDbDataTypeNode
  readonly valueType: DuckDbDataTypeNode
}

/**
 * DuckDB union type
 */
export interface DuckDbUnionNode extends DuckDbDataTypeNode {
  readonly dataType: 'UNION'
  readonly types: DuckDbDataTypeNode[]
}

/**
 * DuckDB geometry type (from spatial extension)
 */
export interface DuckDbGeometryNode extends DuckDbDataTypeNode {
  readonly dataType: 'GEOMETRY'
}

/**
 * DuckDB vector type (for embeddings)
 */
export interface DuckDbVectorNode extends DuckDbDataTypeNode {
  readonly dataType: 'VECTOR'
  readonly dimensions?: number
}

/**
 * Common DuckDB data types
 */
export type DuckDbDataType =
  | 'BOOLEAN'
  | 'TINYINT'
  | 'SMALLINT'
  | 'INTEGER'
  | 'BIGINT'
  | 'UTINYINT'
  | 'USMALLINT'
  | 'UINTEGER'
  | 'UBIGINT'
  | 'REAL'
  | 'DOUBLE'
  | 'DECIMAL'
  | 'VARCHAR'
  | 'BLOB'
  | 'TIMESTAMP'
  | 'DATE'
  | 'TIME'
  | 'INTERVAL'
  | 'HUGEINT'
  | 'UHUGEINT'
  | 'UUID'
  | 'JSON'
  | 'ARRAY'
  | 'STRUCT'
  | 'MAP'
  | 'UNION'
  | 'GEOMETRY'
  | 'VECTOR'

/**
 * Configuration for table mappings (for external data sources)
 */
export interface TableMapping {
  /** Path to external file or data source */
  source: string
  /** DuckDB read function parameters */
  options?: Record<string, unknown>
}

/**
 * DuckDB dialect configuration
 */
export interface DuckDbDialectConfig {
  /** DuckDB database instance */
  database: DuckDBInstance
  /**
   * UUID conversion behavior for result rows.
   * When true, UUID-typed columns are converted to strings.
   * When false (default), raw DuckDB UUID runtime values are returned.
   */
  uuidAsString?: boolean
  /** Table mappings for external data sources */
  tableMappings?: Record<string, string | TableMapping>
  /** Additional DuckDB configuration */
  config?: Record<string, unknown>
}

/**
 * Legacy, loose configuration type used in tests/examples to describe various shapes.
 * Note: This is not used by the runtime dialect. Prefer DuckDbDialectConfig for real code.
 */
export interface DuckDbConfig {
  database: unknown
  tableMappings?: Record<string, string | TableMapping>
  config?: Record<string, unknown>
}

// Create a transform type that takes the interface and makes all properties required
// and non-nullable, recursively
type RequiredNonNullable<T> = {
  [P in keyof T]-?: T[P] extends object
    ? T[P] extends (...args: any[]) => any
      ? T[P]
      : RequiredNonNullable<T[P]>
    : NonNullable<T[P]>
}

/**
 * Internal fully resolved DuckDB dialect configuration
 */
export type InternalDuckDbDialectConfig = RequiredNonNullable<DuckDbDialectConfig> & {
  database: DuckDBInstance
}

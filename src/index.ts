/**
 * DuckDB dialect for Kysely
 *
 * This package provides a DuckDB dialect for the Kysely TypeScript SQL query builder.
 * It uses the modern @duckdb/node-api package for optimal performance and TypeScript support.
 */

export { DuckDbAdapter } from './dialect/duckdb-adapter.js'
// Main dialect exports
export { DuckDbDialect } from './dialect/duckdb-dialect.js'
// Driver and adapter exports
export { DuckDbDriver } from './dialect/duckdb-driver.js'
export { DuckDbIntrospector } from './dialect/duckdb-introspector.js'
export { DuckDbQueryCompiler } from './dialect/duckdb-query-compiler.js'
export * from './extensions/json.js'

// Extension helpers
export * from './extensions/spatial.js'
export * from './extensions/vector.js'
// Performance monitoring
export {
  formatPerformanceStats,
  globalPerformanceMonitor,
  PerformanceMonitor,
  type PerformanceStats,
  type QueryMetrics,
} from './internal/performance-monitor.js'
// Version checking
export {
  checkDuckDbVersion,
  checkEnvironment,
  checkNodeVersion,
} from './internal/version-check.js'
export * from './migrations/provider.js'
// Migration helpers
export * from './migrations/utils.js'
// Plugins
export * from './plugins/case-converter.js'
// Note: LoggerPlugin moved to internal/ - use Kysely's native logging instead
// Type definitions
export type {
  DuckDbArrayNode,
  DuckDbDataType,
  DuckDbDataTypeNode,
  DuckDbDialectConfig,
  DuckDbGeometryNode,
  DuckDbMapNode,
  DuckDbStructNode,
  DuckDbUnionNode,
  DuckDbVectorNode,
  TableMapping,
} from './types/data-types.js'

/**
 * Valibot schemas for configuration validation
 */
import {
  boolean,
  custom,
  type InferOutput,
  object,
  optional,
  pipe,
  record,
  string,
  union,
  unknown,
} from 'valibot'

/**
 * Logger configuration schema
 */
export const LoggerConfigSchema = object({
  /**
   * Enable debug logging (defaults to NODE_ENV=development or KYSELY_DEBUG=true/1)
   */
  debugEnabled: optional(boolean()),
  /**
   * Log prefix for all messages
   */
  prefix: optional(string()),
})

export type LoggerConfig = InferOutput<typeof LoggerConfigSchema>

/**
 * Table mapping configuration schema
 */
export const TableMappingSchema = object({
  source: pipe(
    string(),
    custom(value => {
      // Basic validation: non-empty string
      return typeof value === 'string' && value.trim().length > 0
    }, 'Source must be a non-empty string'),
  ),
  options: optional(record(string(), unknown())),
})

/**
 * Connection configuration schema with strict validation
 */
export const ConnectionConfigSchema = object({
  /**
   * Convert UUID columns to strings (default: false)
   */
  uuidAsString: optional(boolean()),
  /**
   * Table mappings for external data sources
   * Can be a simple string path or a full TableMapping object
   */
  tableMappings: optional(record(string(), union([string(), TableMappingSchema]))),
  /**
   * DuckDB configuration options
   */
  config: optional(record(string(), unknown())),
})

export type ConnectionConfig = InferOutput<typeof ConnectionConfigSchema>

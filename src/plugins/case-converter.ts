/**
 * Case conversion plugin for DuckDB
 * Converts between camelCase and snake_case automatically
 */

import type {
  KyselyPlugin,
  PluginTransformQueryArgs,
  PluginTransformResultArgs,
  QueryResult,
  RootOperationNode,
  UnknownRow,
} from 'kysely'

/**
 * Configuration for case conversion
 */
export interface CaseConverterConfig {
  /** Convert table and column names to snake_case in queries */
  toSnakeCase?: boolean
  /** Convert result column names to camelCase */
  toCamelCase?: boolean
  /** Custom conversion function for specific names */
  customConversions?: Record<string, string>
}

/**
 * Plugin that converts between camelCase and snake_case
 */
export class CaseConverterPlugin implements KyselyPlugin {
  readonly #config: CaseConverterConfig

  constructor(config: CaseConverterConfig = {}) {
    this.#config = {
      toSnakeCase: true,
      toCamelCase: true,
      ...config,
    }
  }

  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    if (!this.#config.toSnakeCase) {
      return args.node
    }

    // Transform the query node to convert identifiers to snake_case
    return this.#transformNode(args.node)
  }

  async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<UnknownRow>> {
    if (!this.#config.toCamelCase) {
      return args.result
    }

    // Transform result column names to camelCase
    return {
      ...args.result,
      rows: args.result.rows.map(row => this.#transformResultRow(row)),
    }
  }

  #transformNode(node: any, visited = new WeakSet()): any {
    if (!node || typeof node !== 'object') {
      return node
    }

    // Prevent circular references
    if (visited.has(node)) {
      return node
    }
    visited.add(node)

    if (Array.isArray(node)) {
      return node.map(item => this.#transformNode(item, visited))
    }

    // Handle identifier nodes
    if (node.kind === 'IdentifierNode' && node.name) {
      const converted = this.#toSnakeCase(node.name)
      return {
        ...node,
        name: this.#config.customConversions?.[node.name] || converted,
      }
    }

    // Handle column reference nodes
    if (node.kind === 'ColumnNode' && node.column) {
      return {
        ...node,
        column: this.#transformNode(node.column, visited),
      }
    }

    // Handle table reference nodes
    if (node.kind === 'TableNode' && node.table) {
      return {
        ...node,
        table: this.#transformNode(node.table, visited),
      }
    }

    // Recursively transform all properties
    const transformed: any = {}
    for (const [key, value] of Object.entries(node)) {
      transformed[key] = this.#transformNode(value, visited)
    }

    return transformed
  }

  #transformResultRow(row: UnknownRow): UnknownRow {
    const transformed: UnknownRow = {}

    for (const [key, value] of Object.entries(row)) {
      const camelKey = this.#config.customConversions?.[key] || this.#toCamelCase(key)
      transformed[camelKey] = value
    }

    return transformed
  }

  #toSnakeCase(str: string): string {
    return str
      .replace(/([A-Z])/g, '_$1')
      .toLowerCase()
      .replace(/^_/, '')
  }

  #toCamelCase(str: string): string {
    return str.toLowerCase().replace(/_([a-z])/g, (_, letter) => letter.toUpperCase())
  }
}

// Case Conversion Utilities - Individual exports for better tree-shaking

/**
 * Convert string to snake_case
 */
export function toSnakeCase(str: string): string {
  return str
    .replace(/([A-Z])/g, '_$1')
    .toLowerCase()
    .replace(/^_/, '')
}

/**
 * Convert string to camelCase
 */
export function toCamelCase(str: string): string {
  if (!str || str.length === 0) {
    return str
  }

  // Return as-is if it's already camelCase (no underscores or just trailing/leading)
  if (str.indexOf('_') === -1) {
    return str
  }

  // Special case: string with only trailing underscores like 'field__'
  // Split into two separate replacements to avoid ReDoS vulnerability (CWE-1333)
  // The pattern /^_+|_+$/g with global flag can cause polynomial backtracking
  const withoutLeading = str.replace(/^_+/, '')
  const withoutBoth = withoutLeading.replace(/_+$/, '')
  if (withoutBoth.indexOf('_') === -1) {
    const trailingMatch = str.match(/_+$/)
    if (trailingMatch && trailingMatch[0].length > 1) {
      // Reduce trailing underscores by one: 'field__' -> 'field_'
      return str.slice(0, -trailingMatch[0].length) + '_'.repeat(trailingMatch[0].length - 1)
    }
    return str
  }

  // Handle leading underscores - preserve them for single, reduce by 1 for multiple
  const leadingMatch = str.match(/^_+/)
  const leadingCount = leadingMatch ? leadingMatch[0].length : 0

  // Handle trailing underscores - reduce by one if multiple, preserve if single
  const trailingMatch = str.match(/_+$/)
  const trailingCount = trailingMatch ? trailingMatch[0].length : 0

  // Get the middle part
  let middle = str.slice(leadingCount, trailingCount > 0 ? -trailingCount : str.length)

  // Convert to camelCase: consecutive underscores become fewer underscores + capitalized next letter
  middle = middle.toLowerCase().replace(/_+([a-z0-9])/g, (match, nextChar) => {
    const underscoreCount = match.length - 1 // Number of underscores before the character
    if (underscoreCount > 1) {
      // Multiple underscores: keep one less and capitalize
      return '_'.repeat(underscoreCount - 1) + nextChar.toUpperCase()
    } else {
      // Single underscore: just capitalize
      return nextChar.toUpperCase()
    }
  })

  // Handle leading underscores: for camelCase, keep leading underscores but reduce by 1 for multiple
  const leadingPart =
    leadingCount > 1 ? '_'.repeat(leadingCount - 1) : leadingCount === 1 ? '_' : ''

  // Handle trailing underscores: preserve single, reduce multiple by one
  const trailingPart =
    trailingCount > 1 ? '_'.repeat(trailingCount - 1) : trailingCount === 1 ? '_' : ''

  return leadingPart + middle + trailingPart
}

/**
 * Convert string to PascalCase
 */
export function toPascalCase(str: string): string {
  const camelCase = toCamelCase(str)
  return camelCase.charAt(0).toUpperCase() + camelCase.slice(1)
}

/**
 * Convert object keys to snake_case
 */
export function objectToSnakeCase<T extends Record<string, any>>(obj: T): Record<string, any> {
  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(obj)) {
    result[toSnakeCase(key)] = value
  }
  return result
}

/**
 * Convert object keys to camelCase
 */
export function objectToCamelCase<T extends Record<string, any>>(obj: T): Record<string, any> {
  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(obj)) {
    result[toCamelCase(key)] = value
  }
  return result
}

// Backward compatibility - export object for existing code
export const CaseConverter = {
  toSnakeCase,
  toCamelCase,
  toPascalCase,
  objectToSnakeCase,
  objectToCamelCase,
} as const

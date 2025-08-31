/**
 * DuckDB JSON extension helpers
 * Only includes functions that actually exist in DuckDB
 */

import { type Expression, type RawBuilder, sql } from 'kysely'

/**
 * Extract a value from JSON using a path
 * Alias: json_extract_path, Operator: ->
 */
export function jsonExtract(
  json: Expression<unknown>,
  path: string | Expression<string>,
): RawBuilder<any> {
  return sql`json_extract(${json}, ${path})`
}

/**
 * Extract a string value from JSON using a path
 * Alias: json_extract_path_text, Operator: ->>
 */
export function jsonExtractString(
  json: Expression<unknown>,
  path: string | Expression<string>,
): RawBuilder<string | null> {
  return sql`json_extract_string(${json}, ${path})`
}

/**
 * Check if a path exists in JSON
 */
export function jsonExists(
  json: Expression<unknown>,
  path: string | Expression<string>,
): RawBuilder<boolean> {
  return sql`json_exists(${json}, ${path})`
}

/**
 * Extract scalar JSON value (returns NULL if not scalar)
 */
export function jsonValue(
  json: Expression<unknown>,
  path: string | Expression<string>,
): RawBuilder<any> {
  return sql`json_value(${json}, ${path})`
}

/**
 * Get the type of a JSON value
 */
export function jsonType(json: Expression<unknown>): RawBuilder<string | null> {
  return sql`json_type(${json})`
}

/**
 * Check if JSON is valid
 */
export function jsonValid(json: Expression<unknown>): RawBuilder<boolean> {
  return sql`json_valid(${json})`
}

/**
 * Get the length of a JSON array
 */
export function jsonArrayLength(
  json: Expression<unknown>,
  path?: string | Expression<string>,
): RawBuilder<bigint | null> {
  if (path) {
    return sql`json_array_length(${json}, ${path})`
  }
  return sql`json_array_length(${json})`
}

/**
 * Check if JSON contains another JSON value
 */
export function jsonContains(
  haystack: Expression<unknown>,
  needle: Expression<unknown>,
  path?: string | Expression<string>,
): RawBuilder<boolean> {
  if (path !== undefined) {
    // DuckDB doesn't support a 3-arg json_contains. Implement path by
    // extracting the JSON value at the path and applying the 2-arg variant.
    return sql`json_contains(json_extract(${haystack}, ${path}), ${needle})`
  }
  return sql`json_contains(${haystack}, ${needle})`
}

/**
 * Get the keys of a JSON object
 */
export function jsonKeys(
  json: Expression<unknown>,
  path?: string | Expression<string>,
): RawBuilder<string[]> {
  if (path) {
    return sql`json_keys(${json}, ${path})`
  }
  return sql`json_keys(${json})`
}

/**
 * Get the structure of JSON data
 */
export function jsonStructure(json: Expression<unknown>): RawBuilder<unknown> {
  return sql`json_structure(${json})`
}

/**
 * Transform JSON using a structure template
 * Alias: from_json
 */
// Overloads for jsonTransform helper to support path/value shorthand used in tests
export function jsonTransform(
  json: Expression<unknown>,
  structure: Expression<string>,
): RawBuilder<unknown>
export function jsonTransform(
  json: Expression<unknown>,
  path: string | Expression<string>,
  value: Expression<unknown>,
): RawBuilder<unknown>
export function jsonTransform(
  json: Expression<unknown>,
  transforms: Array<{ path: string | Expression<string>; value: Expression<unknown> }>,
): RawBuilder<unknown>
export function jsonTransform(
  json: Expression<unknown>,
  arg2:
    | Expression<string>
    | (string | Expression<string>)
    | Array<{
        path: string | Expression<string>
        value: Expression<unknown>
      }>,
  arg3?: Expression<unknown>,
): RawBuilder<unknown> {
  // 1) Raw structure form: json_transform(json, structure)
  if (arg3 === undefined && !Array.isArray(arg2) && typeof arg2 !== 'string') {
    return sql`json_transform(${json}, ${arg2 as Expression<string>})`
  }

  // 2) Single path/value form: json_transform(json, path, value)
  if (arg3 !== undefined) {
    const path = arg2 as string | Expression<string>
    const value = arg3
    // Use json_set to express a single transformation
    return sql`json_set(${json}, ${path}, ${value})`
  }

  // 3) Multiple transforms form: json_transform(json, [{path, value}, ...])
  const transforms = arg2 as Array<{
    path: string | Expression<string>
    value: Expression<unknown>
  }>
  // Chain json_set calls to build up transformations in a deterministic way
  // This is a convenience for tests; DuckDB json_set accepts multiple path/value pairs, but
  // building it as a chain keeps the API simple here.
  let expr: RawBuilder<unknown> | Expression<unknown> = json
  for (const t of transforms) {
    expr = sql`json_set(${expr}, ${t.path}, ${t.value})`
  }
  return expr as RawBuilder<unknown>
}

/**
 * Strict JSON transformation with error on mismatch
 * Alias: from_json_strict
 */
export function jsonTransformStrict(
  json: Expression<unknown>,
  structure: Expression<string>,
): RawBuilder<unknown> {
  return sql`json_transform_strict(${json}, ${structure})`
}

/**
 * Convert JSON to proper JSON data type
 */
export function json(json: Expression<unknown>): RawBuilder<unknown> {
  return sql`json(${json})`
}

// Aggregate Functions

/**
 * Aggregate values into a JSON array
 */
export function jsonGroupArray(value: Expression<unknown>): RawBuilder<unknown> {
  return sql`json_group_array(${value})`
}

/**
 * Aggregate key-value pairs into a JSON object
 */
export function jsonGroupObject(
  key: Expression<string>,
  value: Expression<unknown>,
): RawBuilder<unknown> {
  return sql`json_group_object(${key}, ${value})`
}

/**
 * Aggregate JSON structures
 */
export function jsonGroupStructure(json: Expression<unknown>): RawBuilder<unknown> {
  return sql`json_group_structure(${json})`
}

/**
 * Merge two JSON objects using json_merge_patch
 */
export function jsonMerge(
  json1: Expression<unknown>,
  json2: Expression<unknown>,
): RawBuilder<unknown> {
  return sql`json_merge_patch(${json1}, ${json2})`
}

// JSON File Reader Functions

/**
 * Generate a read_json query for loading JSON files
 */
export function readJson(path: string, options: Record<string, unknown> = {}): RawBuilder<unknown> {
  if (Object.keys(options).length === 0) {
    return sql`read_json(${sql.lit(path)})`
  }

  const optionPairs = Object.entries(options).map(([key, value]) => {
    if (typeof value === 'string' && key !== 'columns') {
      return `${key}='${value}'`
    }
    return `${key}=${value}`
  })

  const optionsStr = optionPairs.join(', ')
  return sql`read_json(${sql.lit(path)}, ${sql.raw(optionsStr)})`
}

/**
 * Generate a read_ndjson query for loading NDJSON files
 */
export function readNdjson(
  path: string,
  options: Record<string, unknown> = {},
): RawBuilder<unknown> {
  if (Object.keys(options).length === 0) {
    return sql`read_ndjson(${sql.lit(path)})`
  }

  const optionPairs = Object.entries(options).map(([key, value]) => {
    if (typeof value === 'string') {
      return `${key}='${value}'`
    }
    return `${key}=${value}`
  })

  const optionsStr = optionPairs.join(', ')
  return sql`read_ndjson(${sql.lit(path)}, ${sql.raw(optionsStr)})`
}

/**
 * Generate a read_json query for JSON arrays
 */
export function readJsonArray(
  path: string,
  options: Record<string, unknown> = {},
): RawBuilder<unknown> {
  const arrayOptions = { ...options, format: 'array' }
  return readJson(path, arrayOptions)
}

/**
 * Helper to install and load the JSON extension
 */
export async function loadJsonExtension(db: any): Promise<void> {
  if (!db || db === null || db === undefined) {
    throw new Error('Database instance is required')
  }

  // Validate that db has an execute method or is a Kysely instance
  const hasExecute = typeof db.execute === 'function'
  const hasSchema = db.schema && typeof db.schema === 'object'
  const hasDestroy = typeof db.destroy === 'function'

  if (!hasExecute && !hasSchema && !hasDestroy) {
    throw new Error('Database instance must have an execute method or be a Kysely instance')
  }

  // JSON extension behavior differs by build:
  // - On newer DuckDB builds, JSON is compiled-in: LOAD works and INSTALL may be unnecessary/unsupported.
  // - On others, INSTALL+LOAD is required. We try LOAD first (idempotent), then fall back.
  const isKysely = !!hasSchema
  const exec = async (q: string) => {
    if (isKysely) {
      await sql.raw(q).execute(db)
    } else {
      await db.execute(q)
    }
  }

  try {
    await exec('LOAD json')
    // Best-effort install (ignored if not needed or unsupported). This keeps code portable
    // across environments while making the operation idempotent and safe.
    try {
      await exec('INSTALL json')
    } catch {
      // ignore: extension may already be available/built-in
    }
  } catch {
    try {
      await exec('INSTALL json')
    } catch {
      // ignore: proceed to LOAD attempt regardless
    }
    await exec('LOAD json')
  }
}

// Note: For backward compatibility, you can still use JsonFunctions.methodName()
// but prefer using the direct function exports for better tree-shaking

// Legacy class exports for backward compatibility (deprecated)
export const JsonFunctions = {
  jsonExtract,
  jsonExtractString,
  jsonExists,
  jsonValue,
  jsonType,
  jsonValid,
  jsonArrayLength,
  jsonContains,
  jsonKeys,
  jsonStructure,
  jsonTransform,
  jsonTransformStrict,
  json,
  jsonGroupArray,
  jsonGroupObject,
  jsonGroupStructure,
  jsonMerge,
} as const

export const JsonReader = {
  readJson,
  readNdjson,
  readJsonArray,
} as const

/**
 * DuckDB Vector extension helpers for embeddings and vector search
 */

import { type Expression, sql } from 'kysely'

/**
 * Create a vector from an array
 */
export function vector(values: number[] | Expression<number[]>) {
  return sql`vector(${sql.val(values)})`
}

/**
 * Calculate cosine similarity between two vectors
 */
export function cosineSimilarity(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  // Use manual cosine similarity calculation
  return sql`
    list_dot_product(cast(${vec1} as double[]), cast(${vec2} as double[])) / 
    (sqrt(list_dot_product(cast(${vec1} as double[]), cast(${vec1} as double[]))) * 
     sqrt(list_dot_product(cast(${vec2} as double[]), cast(${vec2} as double[]))))
  `
}

/**
 * Calculate cosine distance between two vectors
 */
export function cosineDistance(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  return sql`1 - ${cosineSimilarity(vec1, vec2)}`
}

/**
 * Calculate Euclidean distance between two vectors
 */
export function l2Distance(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  return sql`
    sqrt(list_sum(list_transform(
      list_zip(cast(${vec1} as double[]), cast(${vec2} as double[])),
      x -> (x[1] - x[2]) * (x[1] - x[2])
    )))
  `
}

/**
 * Calculate Manhattan distance between two vectors
 */
export function l1Distance(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  return sql`
    list_sum(list_transform(
      list_zip(cast(${vec1} as double[]), cast(${vec2} as double[])),
      x -> abs(x[1] - x[2])
    ))
  `
}

/**
 * Calculate dot product of two vectors
 */
export function dotProduct(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  return sql`list_dot_product(cast(${vec1} as double[]), cast(${vec2} as double[]))`
}

/**
 * Calculate vector magnitude (L2 norm)
 */
export function vectorMagnitude(vec: Expression<unknown>) {
  return sql`sqrt(list_dot_product(cast(${vec} as double[]), cast(${vec} as double[])))`
}

/**
 * Normalize a vector
 */
export function normalize(vec: Expression<unknown>) {
  return sql`
    list_transform(
      cast(${vec} as double[]),
      x -> x / sqrt(list_dot_product(cast(${vec} as double[]), cast(${vec} as double[])))
    )
  `
}

/**
 * Get vector dimensions
 */
export function vectorDims(vec: Expression<unknown>) {
  return sql`len(cast(${vec} as double[]))`
}

/**
 * Add two vectors
 */
export function vectorAdd(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  return sql`
    list_transform(
      list_zip(cast(${vec1} as double[]), cast(${vec2} as double[])),
      x -> x[1] + x[2]
    )
  `
}

/**
 * Subtract two vectors
 */
export function vectorSubtract(vec1: Expression<unknown>, vec2: Expression<unknown>) {
  return sql`
    list_transform(
      list_zip(cast(${vec1} as double[]), cast(${vec2} as double[])),
      x -> x[1] - x[2]
    )
  `
}

/**
 * Multiply vector by scalar
 */
export function vectorMultiply(vec: Expression<unknown>, scalar: number | Expression<number>) {
  return sql`list_transform(cast(${vec} as double[]), x -> x * ${sql.val(scalar)})`
}

/**
 * Create zero vector of specified dimensions
 */
export function zeroVector(dimensions: number | Expression<number>) {
  return sql`list_transform(range(${sql.val(dimensions)}), x -> 0.0)`
}

/**
 * Create random vector of specified dimensions
 */
export function randomVector(dimensions: number | Expression<number>) {
  return sql`list_transform(range(${sql.val(dimensions)}), x -> random())`
}

/**
 * Find k nearest neighbors using cosine similarity
 */
export function knnCosine(
  table: string,
  vectorColumn: string,
  queryVector: number[] | Expression<unknown>,
  k: number = 10,
): string {
  const vectorStr = Array.isArray(queryVector)
    ? `[${queryVector.join(', ')}]`
    : String(queryVector).replace(/^sql`|`$/g, '')
  return `
    SELECT *, cosine_similarity(${vectorColumn}, ${vectorStr}) as similarity
    FROM ${table}
    ORDER BY similarity DESC
    LIMIT ${k}
  `
}

/**
 * Find k nearest neighbors using Euclidean distance
 */
export function knnL2(
  table: string,
  vectorColumn: string,
  queryVector: number[] | Expression<unknown>,
  k: number = 10,
): string {
  const vectorStr = Array.isArray(queryVector)
    ? `[${queryVector.join(', ')}]`
    : String(queryVector).replace(/^sql`|`$/g, '')
  return `
    SELECT *, l2_distance(${vectorColumn}, ${vectorStr}) as distance
    FROM ${table}
    ORDER BY distance ASC
    LIMIT ${k}
  `
}

/**
 * Find vectors within a similarity threshold
 */
export function similaritySearch(
  table: string,
  vectorColumn: string,
  queryVector: number[] | Expression<unknown>,
  threshold: number = 0.8,
): string {
  const vectorStr = Array.isArray(queryVector)
    ? `[${queryVector.join(', ')}]`
    : String(queryVector).replace(/^sql`|`$/g, '')
  return `
    SELECT *, cosine_similarity(${vectorColumn}, ${vectorStr}) as similarity
    FROM ${table}
    WHERE cosine_similarity(${vectorColumn}, ${vectorStr}) >= ${threshold}
    ORDER BY similarity DESC
  `
}

/**
 * Find vectors within a distance threshold
 */
export function radiusSearch(
  table: string,
  vectorColumn: string,
  queryVector: number[] | Expression<unknown>,
  radius: number = 1.0,
): string {
  const vectorStr = Array.isArray(queryVector)
    ? `[${queryVector.join(', ')}]`
    : String(queryVector).replace(/^sql`|`$/g, '')
  return `
    SELECT *, l2_distance(${vectorColumn}, ${vectorStr}) as distance
    FROM ${table}
    WHERE l2_distance(${vectorColumn}, ${vectorStr}) <= ${radius % 1 === 0 ? radius.toFixed(1) : radius}
    ORDER BY distance ASC
  `
}

/**
 * Batch insert embeddings with metadata
 */
export function createEmbeddingTable(tableName: string, dimensions: number): string {
  return `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id INTEGER PRIMARY KEY,
      content TEXT,
      embedding VECTOR(${dimensions}),
      metadata JSON,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `
}

/**
 * Create an index for vector search (if supported)
 */
export function createVectorIndex(tableName: string, columnName: string = 'embedding'): string {
  return `CREATE INDEX IF NOT EXISTS idx_${tableName}_${columnName} ON ${tableName} (${columnName})`
}

/**
 * Calculate average embedding from multiple vectors
 */
export function averageEmbedding(vectorColumn: string): string {
  return `
    SELECT vector_normalize(
      array_aggregate(
        unnest(${vectorColumn})
      )
    ) as avg_embedding
  `
}

/**
 * Find centroid of a cluster of vectors
 */
export function clusterCentroid(
  table: string,
  vectorColumn: string,
  clusterColumn: string,
  clusterId: unknown,
): string {
  return `
    SELECT vector_normalize(
      array_avg(${vectorColumn})
    ) as centroid
    FROM ${table}
    WHERE ${clusterColumn} = ${clusterId}
  `
}

// Backward compatibility - export objects for existing code
export const VectorFunctions = {
  vector,
  cosineSimilarity,
  cosineDistance,
  l2Distance,
  l1Distance,
  dotProduct,
  vectorMagnitude,
  normalize,
  vectorDims,
  vectorAdd,
  vectorSubtract,
  vectorMultiply,
  zeroVector,
  randomVector,
} as const

export const VectorSearch = {
  knnCosine,
  knnL2,
  similaritySearch,
  radiusSearch,
} as const

export const EmbeddingUtils = {
  createEmbeddingTable,
  createVectorIndex,
  averageEmbedding,
  clusterCentroid,
} as const

/**
 * Helper to install and load vector-related extensions
 */
export async function loadVectorExtensions(db: any): Promise<void> {
  // Validate database parameter
  if (!db || db === null || db === undefined) {
    throw new Error('Database instance is required')
  }

  // Check if it's a Kysely instance or raw database with execute method
  const hasExecute = typeof db.execute === 'function'
  const hasSchema = db.schema && typeof db.schema === 'object'
  const hasDestroy = typeof db.destroy === 'function'

  if (!hasExecute && !hasSchema && !hasDestroy) {
    throw new Error('Database instance must have an execute method or be a Kysely instance')
  }

  // Try to load vector-related extensions
  const extensions = ['vss'] // Vector Similarity Search extension (vector extension not available on all platforms)

  for (const extension of extensions) {
    // Let DuckDB handle the installation - if it fails, let the error bubble up
    await sql`INSTALL ${sql.lit(extension)}`.execute(db)
    await sql`LOAD ${sql.lit(extension)}`.execute(db)
  }
}

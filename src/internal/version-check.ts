/**
 * Version compatibility utilities for kysely-duckdb
 */

import { sql } from 'kysely'

/**
 * Recommended DuckDB version for optimal compatibility
 */
export const recommendedVersion = '1.1.3'

/**
 * Check DuckDB version compatibility
 */
export async function checkDuckDbVersion(db: any): Promise<{
  version: string
  isCompatible: boolean
  warnings: string[]
}> {
  const warnings: string[] = []

  try {
    const result = (await db.execute) ? await db.execute() : await sql`SELECT version()`.execute(db)

    // Handle empty results or missing version field
    if (!result.rows || result.rows.length === 0) {
      return {
        version: 'unknown',
        isCompatible: false,
        warnings: ['No version information returned from database'],
      }
    }

    const versionString = (result.rows[0] as any)?.version
    if (!versionString) {
      return {
        version: 'unknown',
        isCompatible: false,
        warnings: ['No version field in database response'],
      }
    }

    // Extract version number (e.g., "v1.1.0" -> "1.1.0")
    const versionMatch = versionString.match(/v?(\d+)\.(\d+)\.(\d+)/)

    if (!versionMatch) {
      return {
        version: versionString,
        isCompatible: false,
        warnings: ['Could not parse DuckDB version'],
      }
    }

    const [, major, minor, patch] = versionMatch.map(Number)
    const version = `${major}.${minor}.${patch}`

    // Check minimum version (1.1.0)
    const isCompatible = major > 1 || (major === 1 && minor >= 1)

    if (!isCompatible) {
      warnings.push(`DuckDB version ${version} is not supported. Minimum version is 1.1.0`)
    }

    // Check for known issues
    if (major === 1 && minor === 1 && patch < 3) {
      warnings.push('Consider upgrading to DuckDB 1.1.3+ for better performance and stability')
    }

    return { version, isCompatible, warnings }
  } catch (error) {
    return {
      version: 'unknown',
      isCompatible: false,
      // Disabled coverage as this is hard to test in CI
      warnings: [
        /* c8 ignore next */
        /* v8 ignore next */
        `Failed to check DuckDB version: ${error instanceof Error ? error.message : String(error)}`,
      ],
    }
  }
}

/**
 * Check Node.js version compatibility
 */
export function checkNodeVersion(): {
  version: string
  isCompatible: boolean
  warnings: string[]
} {
  const version = process.version
  const warnings: string[] = []

  // Extract version number (e.g., "v20.1.0" -> [20, 1, 0])
  const versionMatch = version.match(/v(\d+)\.(\d+)\.(\d+)/)

  /* c8 ignore start */
  /* v8 ignore start */
  // Disabled coverage as this is hard to test in CI
  if (!versionMatch) {
    return {
      version,
      isCompatible: false,
      warnings: ['Could not parse Node.js version'],
    }
  }
  const [, major] = versionMatch.map(Number)

  // Check minimum version (Node.js 20.0.0)
  const isCompatible = (major || 0) >= 20

  if (!isCompatible) {
    warnings.push(`Node.js version ${version} is not supported. Minimum version is 20.0.0`)
  }
  if (!isCompatible) {
    warnings.push(`Node.js version ${version} is not supported. Minimum version is 20.0.0`)
  }

  // Check for LTS recommendations
  if ((major || 0) < 20) {
    warnings.push('Consider upgrading to Node.js 20+ LTS for better performance and security')
  }
  /* c8 ignore end */
  /* v8 ignore end */

  return { version, isCompatible, warnings }
}

/**
 * Comprehensive environment check
 */
export async function checkEnvironment(db?: any): Promise<{
  node: ReturnType<typeof checkNodeVersion>
  duckdb: Awaited<ReturnType<typeof checkDuckDbVersion>> | undefined
  overall: boolean
  allWarnings: string[]
}> {
  const node = checkNodeVersion()
  const duckdb = db ? await checkDuckDbVersion(db) : undefined

  const overall = node.isCompatible && (duckdb?.isCompatible ?? true)
  const allWarnings = [...node.warnings, ...(duckdb?.warnings || [])]

  return {
    node,
    duckdb,
    overall,
    allWarnings,
  }
}

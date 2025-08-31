/**
 * Migration provider for DuckDB
 */

import { promises as fs } from 'node:fs'
import { extname, join } from 'node:path'
import { CompiledQuery, type Migration, type MigrationProvider } from 'kysely'

/**
 * File-based migration provider for DuckDB
 */
export class FileMigrationProvider implements MigrationProvider {
  readonly #migrationFolderPath: string
  readonly #migrationFileExtensions: string[]

  constructor(
    migrationFolderPath: string,
    migrationFileExtensions: string[] = ['.ts', '.js', '.sql'],
  ) {
    this.#migrationFolderPath = migrationFolderPath
    this.#migrationFileExtensions = migrationFileExtensions
  }

  async getMigrations(): Promise<Record<string, Migration>> {
    const migrations: Record<string, Migration> = {}

    try {
      const files = await fs.readdir(this.#migrationFolderPath)

      for (const fileName of files) {
        const extension = extname(fileName)

        if (!this.#migrationFileExtensions.includes(extension)) {
          continue
        }

        const migrationName = fileName.replace(extension, '')
        const migrationPath = join(this.#migrationFolderPath, fileName)

        if (extension === '.sql') {
          migrations[migrationName] = await this.#loadSqlMigration(migrationPath)
        } else {
          migrations[migrationName] = await this.#loadJsMigration(migrationPath)
        }
      }
    } catch (error) {
      throw new Error(`Failed to read migrations from ${this.#migrationFolderPath}: ${error}`)
    }

    return migrations
  }

  async #loadSqlMigration(filePath: string): Promise<Migration> {
    const sql = await fs.readFile(filePath, 'utf-8')

    // Split SQL file into up and down migrations
    // Convention: use -- migrate:up and -- migrate:down comments
    const upMatch = sql.match(/-- migrate:up\s*\n([\s\S]*?)(?=-- migrate:down|$)/i)
    const downMatch = sql.match(/-- migrate:down\s*\n([\s\S]*?)$/i)

    if (!upMatch) {
      throw new Error(`Migration file ${filePath} must contain '-- migrate:up' section`)
    }

    const upSql = upMatch[1]?.trim()
    const downSql = downMatch?.[1]?.trim()
    const splitStatements = this.#splitSqlStatements.bind(this)

    if (!upSql) {
      throw new Error(`Migration file ${filePath} has empty '-- migrate:up' section`)
    }

    return {
      async up(db) {
        for (const statement of splitStatements(upSql)) {
          if (statement.trim()) {
            await db.executeQuery(CompiledQuery.raw(statement))
          }
        }
      },
      async down(db) {
        if (!downSql) {
          throw new Error(
            `Migration file ${filePath} must contain '-- migrate:down' section for rollback`,
          )
        }

        for (const statement of splitStatements(downSql)) {
          if (statement.trim()) {
            await db.executeQuery(CompiledQuery.raw(statement))
          }
        }
      },
    }
  }

  async #loadJsMigration(filePath: string): Promise<Migration> {
    try {
      // Dynamic import for ES modules
      const migrationModule = await import(`file://${filePath}`)

      if (!migrationModule.up || typeof migrationModule.up !== 'function') {
        throw new Error(`Migration ${filePath} must export an 'up' function`)
      }

      return {
        up: migrationModule.up,
        down: migrationModule.down,
      }
    } catch (error) {
      throw new Error(`Failed to load migration ${filePath}: ${error}`)
    }
  }

  #splitSqlStatements(sql: string): string[] {
    // Simple SQL statement splitter
    // This is basic and might need improvement for complex cases
    return sql
      .split(';')
      .map(stmt => stmt.trim())
      .filter(stmt => stmt.length > 0)
      .map(stmt => `${stmt};`)
  }
}

/**
 * In-memory migration provider for testing
 */
export class InMemoryMigrationProvider implements MigrationProvider {
  readonly #migrations: Record<string, Migration>

  constructor(migrations: Record<string, Migration>) {
    this.#migrations = migrations
  }

  async getMigrations(): Promise<Record<string, Migration>> {
    return { ...this.#migrations }
  }
}

// Migration Utilities - Individual exports for better tree-shaking

/**
 * Generate a timestamp-based migration name
 */
export function generateMigrationName(description: string): string {
  const timestamp = new Date()
    .toISOString()
    .replace(/[-:T.]/g, '')
    .slice(0, 14)
  const slug = description.toLowerCase().replace(/[^a-z0-9]/g, '_')
  return `${timestamp}_${slug}`
}

/**
 * Create a migration file template
 */
export function createMigrationTemplate(description: string): string {
  return `-- Migration: ${description}
-- Created: ${new Date().toISOString()}

-- migrate:up
-- Add your migration SQL here


-- migrate:down
-- Add your rollback SQL here

`
}

/**
 * Create a TypeScript migration template
 */
export function createTsMigrationTemplate(description: string): string {
  return `import type { Kysely } from 'kysely'

/**
 * ${description}
 * Created: ${new Date().toISOString()}
 */

export async function up(db: Kysely<any>): Promise<void> {
  // Add your migration code here
}

export async function down(db: Kysely<any>): Promise<void> {
  // Add your rollback code here
}
`
}

/**
 * Validate migration name format
 */
export function isValidMigrationName(name: string): boolean {
  // Should be timestamp_description format
  return /^\d{14}_[a-z0-9_]+$/.test(name)
}

/**
 * Extract timestamp from migration name
 */
export function extractTimestamp(migrationName: string): Date | null {
  const match = migrationName.match(/^(\d{14})_/)
  if (!match || !match[1]) {
    return null
  }

  const timestamp = match[1]
  const year = Number.parseInt(timestamp.slice(0, 4), 10)
  const month = Number.parseInt(timestamp.slice(4, 6), 10) - 1 // JS months are 0-based
  const day = Number.parseInt(timestamp.slice(6, 8), 10)
  const hour = Number.parseInt(timestamp.slice(8, 10), 10)
  const minute = Number.parseInt(timestamp.slice(10, 12), 10)
  const second = Number.parseInt(timestamp.slice(12, 14), 10)

  return new Date(year, month, day, hour, minute, second)
}

/**
 * Sort migrations by timestamp
 */
export function sortMigrations(migrationNames: string[]): string[] {
  return migrationNames.sort((a, b) => {
    const timestampA = extractTimestamp(a)
    const timestampB = extractTimestamp(b)

    if (!timestampA || !timestampB) {
      return a.localeCompare(b)
    }

    return timestampA.getTime() - timestampB.getTime()
  })
}

// Backward compatibility - export object for existing code
export const MigrationUtils = {
  generateMigrationName,
  createMigrationTemplate,
  createTsMigrationTemplate,
  isValidMigrationName,
  extractTimestamp,
  sortMigrations,
} as const

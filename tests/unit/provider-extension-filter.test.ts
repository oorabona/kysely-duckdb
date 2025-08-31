import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { FileMigrationProvider } from '../../src/migrations/provider.js'

describe('FileMigrationProvider Extension Filtering (Lignes 34-35)', () => {
  describe('Extension filtering logic', () => {
    it('should skip files with non-matching extensions (lines 34-35)', async () => {
      const testDir = `/tmp/test-migrations-ext-filter-${Date.now()}-${Math.random().toString(36).slice(2)}`
      await fs.mkdir(testDir, { recursive: true })

      try {
        // Create files with various extensions
        const files = [
          // These should be included (default extensions: .ts, .js, .sql)
          {
            name: '001_users.sql',
            content:
              '-- migrate:up\nCREATE TABLE users (id INTEGER);\n-- migrate:down\nDROP TABLE users;',
          },
          {
            name: '002_posts.js',
            content: 'export async function up() {}; export async function down() {};',
          },
          {
            name: '003_comments.ts',
            content: 'export async function up() {}; export async function down() {};',
          },

          // These should be skipped (lines 34-35 - continue statement)
          { name: '004_readme.md', content: '# Documentation' },
          { name: '005_config.json', content: '{}' },
          { name: '006_data.xml', content: '<root></root>' },
          { name: '007_backup.bak', content: 'backup data' },
          { name: '008_temp.tmp', content: 'temporary' },
          { name: '009_text.txt', content: 'plain text' },
          { name: '010_image.png', content: 'fake image data' },
          { name: '011_style.css', content: 'body {}' },
          { name: '012_script.py', content: 'print("hello")' },
        ]

        for (const file of files) {
          await fs.writeFile(join(testDir, file.name), file.content)
        }

        const provider = new FileMigrationProvider(testDir) // Uses default ['.ts', '.js', '.sql']
        const migrations = await provider.getMigrations()

        const migrationKeys = Object.keys(migrations)

        // Should include only .sql, .js, .ts files
        expect(migrationKeys).toHaveLength(3)
        expect(migrationKeys).toContain('001_users')
        expect(migrationKeys).toContain('002_posts')
        expect(migrationKeys).toContain('003_comments')

        // Should skip all other extensions (these trigger lines 34-35)
        expect(migrationKeys).not.toContain('004_readme') // .md skipped
        expect(migrationKeys).not.toContain('005_config') // .json skipped
        expect(migrationKeys).not.toContain('006_data') // .xml skipped
        expect(migrationKeys).not.toContain('007_backup') // .bak skipped
        expect(migrationKeys).not.toContain('008_temp') // .tmp skipped
        expect(migrationKeys).not.toContain('009_text') // .txt skipped
        expect(migrationKeys).not.toContain('010_image') // .png skipped
        expect(migrationKeys).not.toContain('011_style') // .css skipped
        expect(migrationKeys).not.toContain('012_script') // .py skipped
      } finally {
        await fs.rm(testDir, { recursive: true })
      }
    })

    it('should handle limited custom extension filtering', async () => {
      const testDir = `/tmp/test-migrations-limited-filter-${Date.now()}-${Math.random().toString(36).slice(2)}`
      await fs.mkdir(testDir, { recursive: true })

      try {
        const files = [
          // Only SQL files with custom extension list
          {
            name: '001_migration.sql',
            content:
              '-- migrate:up\nCREATE TABLE test1 (id INTEGER);\n-- migrate:down\nDROP TABLE test1;',
          },

          // These should be skipped with custom extension list (lines 34-35)
          {
            name: '002_migration.js',
            content: 'export async function up() {}; export async function down() {};',
          }, // .js now skipped
          {
            name: '003_migration.ts',
            content: 'export async function up() {}; export async function down() {};',
          }, // .ts now skipped
          { name: '004_migration.json', content: '{}' }, // .json skipped
        ]

        for (const file of files) {
          await fs.writeFile(join(testDir, file.name), file.content)
        }

        // Use only .sql extension - should skip .js and .ts that are normally included
        const provider = new FileMigrationProvider(testDir, ['.sql'])
        const migrations = await provider.getMigrations()

        const migrationKeys = Object.keys(migrations)

        expect(migrationKeys).toHaveLength(1)
        expect(migrationKeys).toContain('001_migration')

        // These should be skipped due to custom extension filtering (lines 34-35)
        expect(migrationKeys).not.toContain('002_migration') // .js skipped
        expect(migrationKeys).not.toContain('003_migration') // .ts skipped
        expect(migrationKeys).not.toContain('004_migration') // .json skipped
      } finally {
        await fs.rm(testDir, { recursive: true })
      }
    })

    it('should handle directory with only non-matching files', async () => {
      const testDir = `/tmp/test-migrations-no-match-${Date.now()}-${Math.random().toString(36).slice(2)}`
      await fs.mkdir(testDir, { recursive: true })

      try {
        // Create only files that should be skipped
        const files = [
          { name: 'README.md', content: '# Migration docs' },
          { name: 'package.json', content: '{"name": "test"}' },
          { name: 'config.yml', content: 'debug: true' },
          { name: 'notes.txt', content: 'Migration notes' },
          { name: 'backup.bak', content: 'old stuff' },
        ]

        for (const file of files) {
          await fs.writeFile(join(testDir, file.name), file.content)
        }

        const provider = new FileMigrationProvider(testDir)
        const migrations = await provider.getMigrations()

        // All files should be skipped, result should be empty
        expect(Object.keys(migrations)).toHaveLength(0)
      } finally {
        await fs.rm(testDir, { recursive: true })
      }
    })

    it('should handle edge cases in extension filtering', async () => {
      const testDir = `/tmp/test-migrations-edge-${Date.now()}-${Math.random().toString(36).slice(2)}`
      await fs.mkdir(testDir, { recursive: true })

      try {
        const files = [
          // Valid files
          {
            name: 'migration.sql',
            content:
              '-- migrate:up\nCREATE TABLE test (id INTEGER);\n-- migrate:down\nDROP TABLE test;',
          },

          // Edge cases that should be skipped (lines 34-35)
          { name: 'file.sql.bak', content: 'backup' }, // .bak extension (not .sql)
          { name: 'file.js.old', content: 'old' }, // .old extension (not .js)
          { name: 'no-extension', content: 'no ext' }, // No extension
          { name: '.hidden.py', content: '# hidden python file' }, // .py extension (not in default list)
          { name: 'double.ext.ion', content: 'double extension' }, // .ion extension
        ]

        for (const file of files) {
          await fs.writeFile(join(testDir, file.name), file.content)
        }

        const provider = new FileMigrationProvider(testDir)
        const migrations = await provider.getMigrations()

        const migrationKeys = Object.keys(migrations)

        // Only the .sql file should be included
        expect(migrationKeys).toHaveLength(1)
        expect(migrationKeys).toContain('migration')

        // All edge cases should be skipped
        expect(migrationKeys).not.toContain('file.sql') // .bak extension
        expect(migrationKeys).not.toContain('file.js') // .old extension
        expect(migrationKeys).not.toContain('no-extension') // no extension
        expect(migrationKeys).not.toContain('.hidden') // .py extension
        expect(migrationKeys).not.toContain('double.ext') // .ion extension
      } finally {
        await fs.rm(testDir, { recursive: true })
      }
    })

    it('should handle case-sensitive extension filtering', async () => {
      const testDir = `/tmp/test-migrations-case-${Date.now()}-${Math.random().toString(36).slice(2)}`
      await fs.mkdir(testDir, { recursive: true })

      try {
        const files = [
          {
            name: 'lower.sql',
            content:
              '-- migrate:up\nCREATE TABLE lower (id INTEGER);\n-- migrate:down\nDROP TABLE lower;',
          },
          {
            name: 'upper.SQL',
            content:
              '-- migrate:up\nCREATE TABLE upper (id INTEGER);\n-- migrate:down\nDROP TABLE upper;',
          }, // Should be skipped - case sensitive
          {
            name: 'mixed.Sql',
            content:
              '-- migrate:up\nCREATE TABLE mixed (id INTEGER);\n-- migrate:down\nDROP TABLE mixed;',
          }, // Should be skipped - case sensitive
        ]

        for (const file of files) {
          await fs.writeFile(join(testDir, file.name), file.content)
        }

        const provider = new FileMigrationProvider(testDir)
        const migrations = await provider.getMigrations()

        const migrationKeys = Object.keys(migrations)

        // Only lowercase .sql should be included (extension matching is case-sensitive)
        expect(migrationKeys).toHaveLength(1)
        expect(migrationKeys).toContain('lower')

        // Case-sensitive filtering should skip these (lines 34-35)
        expect(migrationKeys).not.toContain('upper') // .SQL (uppercase) skipped
        expect(migrationKeys).not.toContain('mixed') // .Sql (mixed case) skipped
      } finally {
        await fs.rm(testDir, { recursive: true })
      }
    })
  })
})

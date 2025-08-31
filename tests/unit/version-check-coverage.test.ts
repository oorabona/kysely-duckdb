import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely } from 'kysely'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { DuckDbDialect } from '../../src/index.js'
import { checkDuckDbVersion, recommendedVersion } from '../../src/internal/version-check.js'

describe('Version Check Coverage', () => {
  let database: DuckDBInstance
  let db: Kysely<any>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    db = new Kysely({
      dialect: new DuckDbDialect({ database }),
    })
  })

  afterEach(async () => {
    await db.destroy()
  })

  describe('Version Compatibility Checks', () => {
    it('should handle normal DuckDB version check', async () => {
      // This should trigger the normal flow (lines 10-55)
      const result = await checkDuckDbVersion(db)

      expect(result).toHaveProperty('version')
      expect(result).toHaveProperty('isCompatible')
      expect(result).toHaveProperty('warnings')
      expect(Array.isArray(result.warnings)).toBe(true)
    })

    it('should handle unparseable version string', async () => {
      // Mock the SQL execution to return an unparseable version
      const mockDb = {
        execute: vi.fn().mockResolvedValue({
          rows: [{ version: 'some-unparseable-version-string' }],
        }),
      }

      // This should trigger the version parsing failure (lines 24-30)
      const result = await checkDuckDbVersion(mockDb)

      expect(result.version).toBe('some-unparseable-version-string')
      expect(result.isCompatible).toBe(false)
      expect(result.warnings).toContain('Could not parse DuckDB version')
    })

    it('should handle SQL execution errors', async () => {
      // Mock the SQL execution to throw an error
      const mockDb = {
        execute: vi.fn().mockRejectedValue(new Error('Database connection failed')),
      }

      // This should trigger the catch block (lines 72-77)
      const result = await checkDuckDbVersion(mockDb)

      expect(result.version).toBe('unknown')
      expect(result.isCompatible).toBe(false)
      expect(result.warnings).toContain(
        'Failed to check DuckDB version: Database connection failed',
      )
    })

    it('should detect version compatibility issues', async () => {
      // Mock an old version that should trigger warnings
      const mockDb = {
        execute: vi.fn().mockResolvedValue({
          rows: [{ version: 'v0.5.0 abc123' }], // Very old version
        }),
      }

      // This should trigger compatibility warnings
      const result = await checkDuckDbVersion(mockDb)

      expect(result.version).toBe('0.5.0')
      expect(result.warnings.length).toBeGreaterThan(0)
    })

    it('should handle missing version in result', async () => {
      // Mock result with no version field
      const mockDb = {
        execute: vi.fn().mockResolvedValue({
          rows: [{}], // No version field
        }),
      }

      const result = await checkDuckDbVersion(mockDb)

      expect(result.version).toBe('unknown')
      expect(result.isCompatible).toBe(false)
    })

    it('should handle empty result rows', async () => {
      // Mock result with empty rows
      const mockDb = {
        execute: vi.fn().mockResolvedValue({
          rows: [], // Empty rows
        }),
      }

      const result = await checkDuckDbVersion(mockDb)

      expect(result.version).toBe('unknown')
      expect(result.isCompatible).toBe(false)
    })
  })

  describe('Version Comparison Logic', () => {
    it('should handle various version formats', async () => {
      const testVersions = ['v1.0.0', 'v1.1.0-dev', '0.9.2 (abc123)', 'v1.2.0+build.123']

      for (const versionString of testVersions) {
        const mockDb = {
          execute: vi.fn().mockResolvedValue({
            rows: [{ version: versionString }],
          }),
        }

        const result = await checkDuckDbVersion(mockDb)
        expect(result.version).toBeDefined()
        expect(typeof result.isCompatible).toBe('boolean')
      }
    })

    it('should provide recommended version info', () => {
      // This should trigger the recommendedVersion export (lines 85-86, 90-91)
      expect(recommendedVersion).toBeDefined()
      expect(typeof recommendedVersion).toBe('string')
      expect(recommendedVersion).toMatch(/^\d+\.\d+\.\d+/)
    })

    it('should handle edge case version comparisons', async () => {
      // Test versions that might cause edge cases
      const edgeCases = [
        'v10.0.0', // Double digit major
        'v1.10.0', // Double digit minor
        'v1.0.10', // Double digit patch
        'v0.0.1', // Very early version
      ]

      for (const versionString of edgeCases) {
        const mockDb = {
          execute: vi.fn().mockResolvedValue({
            rows: [{ version: versionString }],
          }),
        }

        const result = await checkDuckDbVersion(mockDb)
        expect(result).toBeDefined()
      }
    })
  })

  describe('Warning Generation', () => {
    it('should generate appropriate warnings for different scenarios', async () => {
      // Test with a version that should generate specific warnings
      const mockDb = {
        execute: vi.fn().mockResolvedValue({
          rows: [{ version: 'v0.8.0' }], // Should trigger warnings about features
        }),
      }

      const result = await checkDuckDbVersion(mockDb)

      // Should have warnings for old version
      expect(result.warnings.length).toBeGreaterThan(0)
      expect(result.isCompatible).toBe(false) // Version 0.8.0 is below minimum 1.1.0
    })
  })
})

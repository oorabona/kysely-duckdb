/**
 * Path Traversal and File Access Security Tests
 *
 * These tests document the expected behavior for table mappings with
 * external file paths. While the library allows flexible path configuration,
 * applications should implement their own path validation for production use.
 *
 * IMPORTANT: Path validation is the responsibility of the application layer.
 * These tests serve as documentation for secure usage patterns.
 */

import { parse } from 'valibot'
import { describe, expect, it } from 'vitest'
import { ConnectionConfigSchema, TableMappingSchema } from '../../src/validation/config-schemas.js'

describe('Security: Path Validation', () => {
  describe('Path Traversal Detection (Documentation)', () => {
    it('documents that path traversal attempts are valid at schema level', () => {
      // Schema validation allows these - application must validate
      const suspiciousPaths = [
        '../../../etc/passwd',
        '..\\..\\..\\windows\\system32\\config\\sam',
        '/etc/shadow',
        'C:\\Windows\\System32\\config\\SAM',
      ]

      for (const path of suspiciousPaths) {
        const config = {
          tableMappings: {
            malicious: path,
          },
        }

        // Schema allows it - this is intentional for flexibility
        const result = parse(ConnectionConfigSchema, config)
        expect(result.tableMappings?.['malicious']).toBe(path)
      }
    })

    it('documents recommended path validation pattern for applications', () => {
      // RECOMMENDED: Application-level validation
      function isPathSafe(path: string): boolean {
        // 1. No parent directory references
        if (path.includes('..')) {
          return false
        }

        // 2. Must be relative or in allowed directory
        const allowedDir = '/app/data'
        if (path.startsWith('/') && !path.startsWith(allowedDir)) {
          return false
        }

        // 3. No absolute Windows paths
        if (/^[A-Za-z]:\\/.test(path)) {
          return false
        }

        return true
      }

      const safePaths = ['./data/users.csv', 'data/products.json', '/app/data/sales.parquet']

      const unsafePaths = [
        '../../../etc/passwd',
        'C:\\Windows\\System32',
        '/etc/shadow',
        'data/../../../etc/passwd',
      ]

      for (const path of safePaths) {
        expect(isPathSafe(path)).toBe(true)
      }

      for (const path of unsafePaths) {
        expect(isPathSafe(path)).toBe(false)
      }
    })
  })

  describe('Special Characters in Paths', () => {
    it('should handle special characters in table mapping paths', () => {
      const specialPaths = [
        './data/file with spaces.csv',
        './data/file-with-dashes.json',
        './data/file_with_underscores.parquet',
        './data/file.multiple.dots.csv',
        "./data/file'with'quotes.json",
      ]

      for (const path of specialPaths) {
        const mapping = {
          source: path,
          options: { header: true },
        }

        const result = parse(TableMappingSchema, mapping)
        expect(result.source).toBe(path)
      }
    })

    it('should reject empty or whitespace-only paths', () => {
      const invalidPaths = ['', '   ', '\t', '\n']

      for (const path of invalidPaths) {
        const mapping = {
          source: path,
        }

        expect(() => parse(TableMappingSchema, mapping)).toThrow()
      }
    })

    it('should handle URL-like paths (for DuckDB HTTP support)', () => {
      const urlPaths = [
        'https://example.com/data.csv',
        'http://localhost:8000/data.json',
        's3://bucket/path/to/file.parquet',
        'gs://bucket/data.csv',
      ]

      for (const path of urlPaths) {
        const config = {
          tableMappings: {
            remote_data: path,
          },
        }

        const result = parse(ConnectionConfigSchema, config)
        expect(result.tableMappings?.['remote_data']).toBe(path)
      }
    })
  })

  describe('Null Byte Injection in Paths', () => {
    it('should handle null bytes in file paths', () => {
      const pathWithNullByte = 'safe_file.csv\x00/etc/passwd'

      const config = {
        tableMappings: {
          table: pathWithNullByte,
        },
      }

      // Schema allows it - OS/filesystem will reject
      const result = parse(ConnectionConfigSchema, config)
      expect(result.tableMappings?.['table']).toBe(pathWithNullByte)
    })
  })

  describe('Symbolic Link Traversal (Documentation)', () => {
    it('documents that symlink validation is filesystem-level concern', () => {
      // DuckDB/OS will resolve symlinks - not library responsibility
      const symlinkPath = './data/symlink_to_sensitive_file'

      const config = {
        tableMappings: {
          data: symlinkPath,
        },
      }

      const result = parse(ConnectionConfigSchema, config)
      expect(result.tableMappings?.['data']).toBe(symlinkPath)

      // NOTE: Applications should:
      // 1. Disable symlink following if needed
      // 2. Validate resolved paths
      // 3. Use chroot/containers for isolation
    })
  })

  describe('Table Name Injection via Mappings', () => {
    it('should allow SQL-safe table names only (best practice)', () => {
      // RECOMMENDED: Validate table names before using
      function isTableNameSafe(name: string): boolean {
        // Only alphanumeric and underscores
        return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)
      }

      const safeNames = ['users', 'user_data', 'UserData', '_internal']

      const unsafeNames = [
        'users; DROP TABLE products;--',
        'users WHERE 1=1',
        'table-with-dashes',
        'table with spaces',
        '"; DELETE FROM users;',
      ]

      for (const name of safeNames) {
        expect(isTableNameSafe(name)).toBe(true)
      }

      for (const name of unsafeNames) {
        expect(isTableNameSafe(name)).toBe(false)
      }
    })

    it('documents that schema allows flexible table names', () => {
      // Schema is permissive - applications should validate
      const config = {
        tableMappings: {
          "table'; DROP TABLE users;--": './safe_file.csv',
        },
      }

      // Allowed by schema (for flexibility)
      const result = parse(ConnectionConfigSchema, config)
      expect(result.tableMappings).toBeDefined()
    })
  })

  describe('File Extension Validation', () => {
    it('should document supported file extensions', () => {
      const supportedExtensions = {
        csv: './data.csv',
        json: './data.json',
        jsonl: './data.jsonl',
        ndjson: './data.ndjson',
        parquet: './data.parquet',
        arrow: './data.arrow',
        xlsx: './data.xlsx',
        excel: './data.excel',
      }

      for (const [_ext, path] of Object.entries(supportedExtensions)) {
        const mapping = {
          source: path,
        }

        const result = parse(TableMappingSchema, mapping)
        expect(result.source).toBe(path)
      }
    })

    it('should allow arbitrary extensions (flexibility)', () => {
      // DuckDB is extensible - allow unknown extensions
      const arbitraryPaths = ['./data.xyz', './file.custom', './data']

      for (const path of arbitraryPaths) {
        const mapping = {
          source: path,
        }

        const result = parse(TableMappingSchema, mapping)
        expect(result.source).toBe(path)
      }
    })
  })

  describe('Security Best Practices Documentation', () => {
    it('demonstrates complete secure configuration pattern', () => {
      // RECOMMENDED PATTERN for production applications
      interface SecurePathConfig {
        allowedDirectory: string
        allowedExtensions: string[]
        maxPathLength: number
      }

      function validateSecurePath(path: string, config: SecurePathConfig): boolean {
        // 1. Length check (prevent buffer issues)
        if (path.length > config.maxPathLength) {
          return false
        }

        // 2. No path traversal
        if (path.includes('..')) {
          return false
        }

        // 3. Must be in allowed directory
        const resolvedPath = path.startsWith('/') ? path : `${config.allowedDirectory}/${path}`
        if (!resolvedPath.startsWith(config.allowedDirectory)) {
          return false
        }

        // 4. Extension whitelist
        const ext = path.split('.').pop()?.toLowerCase()
        if (ext && !config.allowedExtensions.includes(ext)) {
          return false
        }

        // 5. No special characters
        if (!/^[a-zA-Z0-9._/-]+$/.test(path)) {
          return false
        }

        return true
      }

      const secureConfig: SecurePathConfig = {
        allowedDirectory: '/app/data',
        allowedExtensions: ['csv', 'json', 'parquet'],
        maxPathLength: 256,
      }

      expect(validateSecurePath('./users.csv', secureConfig)).toBe(true)
      expect(validateSecurePath('../../../etc/passwd', secureConfig)).toBe(false)
      expect(validateSecurePath('./file.exe', secureConfig)).toBe(false)
    })
  })
})

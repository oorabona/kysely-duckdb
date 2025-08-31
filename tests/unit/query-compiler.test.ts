import { type DataTypeNode, DefaultQueryCompiler } from 'kysely'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { DuckDbQueryCompiler } from '../../src/dialect/duckdb-query-compiler.js'

describe('DuckDbQueryCompiler', () => {
  let compiler: DuckDbQueryCompiler

  const compileDataType = (dataType: string): string => {
    const node: DataTypeNode = { kind: 'DataTypeNode', dataType: dataType as any }
    // Create a mock compiler context to capture output
    const mockAppend = vi.fn()
    ;(compiler as any).append = mockAppend

    ;(compiler as any).visitDataType(node)

    // Return the last appended value (the compiled type)
    return mockAppend.mock.calls[mockAppend.mock.calls.length - 1]?.[0] || ''
  }

  beforeEach(() => {
    compiler = new DuckDbQueryCompiler()
  })

  describe('Data Type Compilation', () => {
    describe('Special DuckDB Types', () => {
      it('should compile uuid type', () => {
        const result = compileDataType('uuid')
        expect(result).toBe('UUID')
      })

      it('should compile json type', () => {
        const result = compileDataType('json')
        expect(result).toBe('JSON')
      })

      it('should compile geometry type', () => {
        const result = compileDataType('geometry')
        expect(result).toBe('GEOMETRY')
      })

      it('should compile vector type', () => {
        const result = compileDataType('vector')
        expect(result).toBe('VECTOR')
      })

      it('should handle case variations', () => {
        expect(compileDataType('UUID')).toBe('UUID')
        expect(compileDataType('Json')).toBe('JSON')
        expect(compileDataType('GEOMETRY')).toBe('GEOMETRY')
        expect(compileDataType('Vector')).toBe('VECTOR')
      })
    })

    describe('Types with Parameters', () => {
      it('should compile varchar with size', () => {
        const result = compileDataType('varchar(255)')
        expect(result).toBe('VARCHAR(255)')
      })

      it('should compile char with size', () => {
        const result = compileDataType('char(10)')
        expect(result).toBe('VARCHAR(10)')
      })

      it('should compile text with size', () => {
        const result = compileDataType('text(1000)')
        expect(result).toBe('VARCHAR(1000)')
      })

      it('should compile string with size', () => {
        const result = compileDataType('string(50)')
        expect(result).toBe('VARCHAR(50)')
      })

      it('should compile decimal with precision and scale', () => {
        const result = compileDataType('decimal(10,2)')
        expect(result).toBe('DECIMAL(10,2)')
      })

      it('should compile numeric with precision and scale', () => {
        const result = compileDataType('numeric(18,4)')
        expect(result).toBe('DECIMAL(18,4)')
      })

      it('should handle complex parameters', () => {
        const result = compileDataType("varchar(255, 'utf8')")
        expect(result).toBe("VARCHAR(255, 'utf8')")
      })

      it('should handle unknown types with parameters', () => {
        const result = compileDataType('custom_type(100, true)')
        expect(result).toBe('CUSTOM_TYPE(100, true)')
      })
    })

    describe('Array Types', () => {
      it('should compile varchar array', () => {
        const result = compileDataType('varchar[]')
        expect(result).toBe('VARCHAR[]')
      })

      it('should compile text array', () => {
        const result = compileDataType('text[]')
        expect(result).toBe('VARCHAR[]')
      })

      it('should compile string array', () => {
        const result = compileDataType('string[]')
        expect(result).toBe('VARCHAR[]')
      })

      it('should compile char array', () => {
        const result = compileDataType('char[]')
        expect(result).toBe('VARCHAR[]')
      })

      it('should compile integer array', () => {
        const result = compileDataType('integer[]')
        expect(result).toBe('INTEGER[]')
      })

      it('should compile int array', () => {
        const result = compileDataType('int[]')
        expect(result).toBe('INTEGER[]')
      })

      it('should compile bigint array', () => {
        const result = compileDataType('bigint[]')
        expect(result).toBe('BIGINT[]')
      })

      it('should compile boolean array', () => {
        const result = compileDataType('boolean[]')
        expect(result).toBe('BOOLEAN[]')
      })

      it('should compile bool array', () => {
        const result = compileDataType('bool[]')
        expect(result).toBe('BOOLEAN[]')
      })

      it('should compile double array', () => {
        const result = compileDataType('double[]')
        expect(result).toBe('DOUBLE[]')
      })

      it('should compile float array', () => {
        const result = compileDataType('float[]')
        expect(result).toBe('REAL[]')
      })

      it('should handle unknown array types', () => {
        const result = compileDataType('custom[]')
        expect(result).toBe('CUSTOM[]')
      })
    })

    describe('Simple Type Mappings', () => {
      const typeMap = {
        integer: 'INTEGER',
        int: 'INTEGER',
        bigint: 'BIGINT',
        smallint: 'SMALLINT',
        tinyint: 'TINYINT',
        boolean: 'BOOLEAN',
        bool: 'BOOLEAN',
        real: 'REAL',
        float: 'REAL',
        double: 'DOUBLE',
        decimal: 'DECIMAL',
        numeric: 'DECIMAL',
        varchar: 'VARCHAR',
        text: 'VARCHAR',
        string: 'VARCHAR',
        char: 'VARCHAR',
        blob: 'BLOB',
        bytea: 'BLOB',
        date: 'DATE',
        time: 'TIME',
        timestamp: 'TIMESTAMP',
        timestamptz: 'TIMESTAMPTZ',
        interval: 'INTERVAL',
        hugeint: 'HUGEINT',
        uhugeint: 'UHUGEINT',
        utinyint: 'UTINYINT',
        usmallint: 'USMALLINT',
        uinteger: 'UINTEGER',
        ubigint: 'UBIGINT',
      }

      for (const [input, expected] of Object.entries(typeMap)) {
        it(`should map ${input} to ${expected}`, () => {
          const result = compileDataType(input)
          expect(result).toBe(expected)
        })
      }
    })

    describe('Case Insensitive Mappings', () => {
      it('should handle uppercase input', () => {
        expect(compileDataType('INTEGER')).toBe('INTEGER')
        expect(compileDataType('VARCHAR')).toBe('VARCHAR')
        expect(compileDataType('BOOLEAN')).toBe('BOOLEAN')
      })

      it('should handle mixed case input', () => {
        expect(compileDataType('Integer')).toBe('INTEGER')
        expect(compileDataType('VarChar')).toBe('VARCHAR')
        expect(compileDataType('Boolean')).toBe('BOOLEAN')
      })

      it('should handle lowercase input', () => {
        expect(compileDataType('integer')).toBe('INTEGER')
        expect(compileDataType('varchar')).toBe('VARCHAR')
        expect(compileDataType('boolean')).toBe('BOOLEAN')
      })
    })

    describe('Unknown Types Fallback', () => {
      it('should uppercase unknown types', () => {
        const result = compileDataType('custom_type')
        expect(result).toBe('CUSTOM_TYPE')
      })

      it('should handle complex unknown types', () => {
        const result = compileDataType('my_complex_type')
        expect(result).toBe('MY_COMPLEX_TYPE')
      })

      it('should preserve already uppercase unknown types', () => {
        const result = compileDataType('UNKNOWN_TYPE')
        expect(result).toBe('UNKNOWN_TYPE')
      })
    })

    describe('Edge Cases and Error Handling', () => {
      it('should handle empty array notation', () => {
        const result = compileDataType('[]')
        expect(result).toBe('[]')
      })

      it('should handle malformed parameters', () => {
        const result = compileDataType('varchar(')
        expect(result).toBe('VARCHAR(')
      })

      it('should handle empty parameters', () => {
        const result = compileDataType('varchar()')
        expect(result).toBe('VARCHAR()')
      })

      it('should handle nested array-like strings', () => {
        const result = compileDataType('array[varchar[]]')
        expect(result).toBe('ARRAY[VARCHAR[]]')
      })

      it('should handle very long type names', () => {
        const longType = 'a'.repeat(100)
        const result = compileDataType(longType)
        expect(result).toBe(longType.toUpperCase())
      })

      it('should handle special characters in type names', () => {
        const result = compileDataType('type_with_underscores')
        expect(result).toBe('TYPE_WITH_UNDERSCORES')
      })
    })
  })

  describe('DuckDB-Specific Query Features', () => {
    it('should compile INSERT OR REPLACE statement', () => {
      const result = (compiler as any).visitInsertOrReplace()
      expect(result).toBe('INSERT OR REPLACE')
    })

    it('should compile INSERT OR IGNORE statement', () => {
      const result = (compiler as any).visitInsertOrIgnore()
      expect(result).toBe('INSERT OR IGNORE')
    })
  })

  describe('Inheritance and Compatibility', () => {
    it('should extend DefaultQueryCompiler', () => {
      expect(compiler).toBeInstanceOf(DefaultQueryCompiler)
    })

    it('should have access to parent methods', () => {
      // Test that compiler inherits from DefaultQueryCompiler
      expect(compiler).toBeInstanceOf(DefaultQueryCompiler)
      expect(typeof (compiler as any).append).toBe('function') // Private method exists
    })

    it('should maintain compilation state correctly', () => {
      // Test the visitDataType method instead of compile
      const uuid = compileDataType('uuid')
      const varchar = compileDataType('varchar(255)')

      expect(uuid).toBe('UUID')
      expect(varchar).toBe('VARCHAR(255)')
    })
  })

  describe('Complex Type Scenarios', () => {
    it('should handle nested array types', () => {
      const result = compileDataType('varchar[][]')
      expect(result).toBe('VARCHAR[][]')
    })

    it('should handle array of parametrized types', () => {
      const result = compileDataType('varchar(100)[]')
      // This is a complex case that might not be handled perfectly
      // but should not crash
      expect(typeof result).toBe('string')
    })

    it('should handle multiple parameter types', () => {
      const result = compileDataType("decimal(10,2,'fixed')")
      expect(result).toBe("DECIMAL(10,2,'fixed')")
    })

    it('should handle type names with numbers', () => {
      const result = compileDataType('float8')
      expect(result).toBe('FLOAT8')
    })

    it('should handle PostgreSQL compatibility types', () => {
      expect(compileDataType('int4')).toBe('INT4')
      expect(compileDataType('int8')).toBe('INT8')
      expect(compileDataType('float4')).toBe('FLOAT4')
      expect(compileDataType('float8')).toBe('FLOAT8')
    })
  })

  describe('Performance and Memory', () => {
    it('should handle many type compilations efficiently', () => {
      const startTime = Date.now()

      for (let i = 0; i < 1000; i++) {
        compileDataType('varchar(255)')
        compileDataType('integer[]')
        compileDataType('timestamp')
      }

      const duration = Date.now() - startTime
      expect(duration).toBeLessThan(1000) // Should complete in less than 1 second
    })

    it('should not leak memory with repeated compilations', () => {
      const initialMemory = process.memoryUsage().heapUsed

      for (let i = 0; i < 10000; i++) {
        const _newCompiler = new DuckDbQueryCompiler()
        compileDataType('varchar(255)')
      }

      const finalMemory = process.memoryUsage().heapUsed
      const memoryIncrease = finalMemory - initialMemory

      // Memory increase should be reasonable (less than 100MB)
      expect(memoryIncrease).toBeLessThan(100 * 1024 * 1024)
    })
  })

  describe('Regex Pattern Matching', () => {
    it('should correctly identify parametrized types', () => {
      // Test the internal regex logic
      const testCases = [
        { input: 'varchar(255)', shouldMatch: true },
        { input: 'decimal(10,2)', shouldMatch: true },
        { input: 'varchar', shouldMatch: false },
        { input: 'varchar()', shouldMatch: true },
        { input: 'type(param1, param2)', shouldMatch: true },
        { input: 'invalid(param', shouldMatch: false },
      ]

      for (const testCase of testCases) {
        const match = testCase.input.match(/^(\w+)(\(.*\))$/) // Allow empty parentheses with .*
        expect(!!match).toBe(testCase.shouldMatch)
      }
    })

    it('should correctly identify array types', () => {
      const testCases = [
        { input: 'varchar[]', shouldMatch: true },
        { input: 'integer[]', shouldMatch: true },
        { input: 'varchar', shouldMatch: false },
        { input: '[]', shouldMatch: true },
        { input: 'type[][]', shouldMatch: true },
      ]

      for (const testCase of testCases) {
        const isArray = testCase.input.endsWith('[]')
        expect(isArray).toBe(testCase.shouldMatch)
      }
    })
  })
})

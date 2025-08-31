import { describe, expect, it } from 'vitest'
import type {
  DuckDbConfig,
  DuckDbDataType,
  DuckDbDataTypeNode,
  TableMapping,
} from '../../src/types/data-types.js'

describe('Data Types', () => {
  describe('DuckDbDataTypeNode', () => {
    it('should create valid data type nodes', () => {
      const node: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'INTEGER',
      }

      expect(node.kind).toBe('DuckDbDataTypeNode')
      expect(node.dataType).toBe('INTEGER')
    })

    it('should support all DuckDB data types', () => {
      const supportedTypes: DuckDbDataType[] = [
        'BOOLEAN',
        'TINYINT',
        'SMALLINT',
        'INTEGER',
        'BIGINT',
        'UTINYINT',
        'USMALLINT',
        'UINTEGER',
        'UBIGINT',
        'REAL',
        'DOUBLE',
        'DECIMAL',
        'VARCHAR',
        'BLOB',
        'TIMESTAMP',
        'DATE',
        'TIME',
        'INTERVAL',
        'HUGEINT',
        'UHUGEINT',
        'UUID',
        'JSON',
        'ARRAY',
        'STRUCT',
        'MAP',
        'UNION',
        'GEOMETRY',
        'VECTOR',
      ]

      for (const dataType of supportedTypes) {
        const node: DuckDbDataTypeNode = {
          kind: 'DuckDbDataTypeNode',
          dataType,
        }
        expect(node.dataType).toBe(dataType)
      }
    })
  })

  describe('TableMapping', () => {
    it('should create simple table mapping', () => {
      const mapping: TableMapping = {
        source: '/path/to/data.csv',
      }

      expect(mapping.source).toBe('/path/to/data.csv')
      expect(mapping.options).toBeUndefined()
    })

    it('should create table mapping with options', () => {
      const mapping: TableMapping = {
        source: '/path/to/data.csv',
        options: {
          header: true,
          delimiter: ',',
          quote: '"',
        },
      }

      expect(mapping.source).toBe('/path/to/data.csv')
      expect(mapping.options).toBeDefined()
      expect(mapping.options?.['header']).toBe(true)
      expect(mapping.options?.['delimiter']).toBe(',')
    })

    it('should support various file formats', () => {
      const csvMapping: TableMapping = {
        source: 'data.csv',
        options: { header: true, delimiter: ',' },
      }

      const jsonMapping: TableMapping = {
        source: 'data.json',
        options: { format: 'newline_delimited' },
      }

      const parquetMapping: TableMapping = {
        source: 's3://bucket/data.parquet',
        options: { compression: 'snappy' },
      }

      expect(csvMapping.source).toContain('.csv')
      expect(jsonMapping.source).toContain('.json')
      expect(parquetMapping.source).toContain('.parquet')
    })
  })

  describe('DuckDbConfig', () => {
    it('should create config with string database', () => {
      const config: DuckDbConfig = {
        database: ':memory:',
      }

      expect(config.database).toBe(':memory:')
      expect(config.tableMappings).toBeUndefined()
      expect(config.config).toBeUndefined()
    })

    it('should create config with database instance', () => {
      const mockDb = { connect: () => {}, close: () => {} }
      const config: DuckDbConfig = {
        database: mockDb,
      }

      expect(config.database).toBe(mockDb)
    })

    it('should create config with table mappings', () => {
      const config: DuckDbConfig = {
        database: ':memory:',
        tableMappings: {
          users: '/data/users.csv',
          orders: {
            source: '/data/orders.parquet',
            options: { compression: 'snappy' },
          },
        },
      }

      expect(config.tableMappings).toBeDefined()
      expect(config.tableMappings?.['users']).toBe('/data/users.csv')
      expect(typeof config.tableMappings?.['orders']).toBe('object')
    })

    it('should create config with DuckDB options', () => {
      const config: DuckDbConfig = {
        database: 'database.db',
        config: {
          memory_limit: '2GB',
          threads: 4,
          enable_external_access: false,
          enable_fsst_vectors: true,
        },
      }

      expect(config.config).toBeDefined()
      expect(config.config?.['memory_limit']).toBe('2GB')
      expect(config.config?.['threads']).toBe(4)
      expect(config.config?.['enable_external_access']).toBe(false)
    })

    it('should create comprehensive config', () => {
      const config: DuckDbConfig = {
        database: { type: 'instance', connection: {} },
        tableMappings: {
          events: {
            source: 's3://data-lake/events/',
            options: {
              format: 'parquet',
              hive_partitioning: true,
              union_by_name: true,
            },
          },
          users: '/local/users.csv',
        },
        config: {
          memory_limit: '4GB',
          threads: 8,
          enable_progress_bar: true,
          preserve_insertion_order: false,
        },
      }

      expect(config.database).toHaveProperty('type')
      expect(config.tableMappings).toHaveProperty('events')
      expect(config.tableMappings).toHaveProperty('users')
      expect(config.config).toHaveProperty('memory_limit')
      expect(config.config?.['threads']).toBe(8)
    })
  })

  describe('Type compatibility', () => {
    it('should work with type guards', () => {
      const isStringDatabase = (db: string | unknown): db is string => {
        return typeof db === 'string'
      }

      const config1: DuckDbConfig = { database: ':memory:' }
      const config2: DuckDbConfig = { database: { instance: true } }

      expect(isStringDatabase(config1.database)).toBe(true)
      expect(isStringDatabase(config2.database)).toBe(false)
    })

    it('should support generic usage patterns', () => {
      const createConfig = <T>(database: T): DuckDbConfig & { database: T } => ({
        database,
      })

      const stringConfig = createConfig(':memory:')
      const objectConfig = createConfig({ connection: 'active' })

      expect(stringConfig.database).toBe(':memory:')
      expect(objectConfig.database).toEqual({ connection: 'active' })
    })
  })
})

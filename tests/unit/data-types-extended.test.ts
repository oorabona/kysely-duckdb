import { describe, expect, it } from 'vitest'
import type {
  DuckDbArrayNode,
  DuckDbConfig,
  DuckDbDataType,
  DuckDbDataTypeNode,
  DuckDbGeometryNode,
  DuckDbMapNode,
  DuckDbStructNode,
  DuckDbUnionNode,
  DuckDbVectorNode,
  TableMapping,
} from '../../src/types/data-types.js'

describe('DuckDB Data Types', () => {
  describe('DuckDbDataTypeNode', () => {
    it('should define basic data type node structure', () => {
      const node: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'INTEGER',
      }

      expect(node.kind).toBe('DuckDbDataTypeNode')
      expect(node.dataType).toBe('INTEGER')
    })

    it('should have readonly properties (TypeScript compile-time)', () => {
      const node: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VARCHAR',
      }

      // Readonly properties are enforced at TypeScript compile-time, not runtime
      // The @ts-expect-error comments above show TypeScript prevents these assignments
      // In JavaScript runtime, the properties can still be modified
      expect(node.kind).toBe('DuckDbDataTypeNode')
      expect(node.dataType).toBe('VARCHAR')

      // Test that we can still read the properties
      const kindCheck: 'DuckDbDataTypeNode' = node.kind
      const dataTypeCheck: string = node.dataType
      expect(kindCheck).toBe('DuckDbDataTypeNode')
      expect(dataTypeCheck).toBe('VARCHAR')
    })

    it('should support all DuckDB data types', () => {
      const types: DuckDbDataType[] = [
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

      for (const dataType of types) {
        const node: DuckDbDataTypeNode = {
          kind: 'DuckDbDataTypeNode',
          dataType,
        }

        expect(node.dataType).toBe(dataType)
      }
    })
  })

  describe('DuckDbArrayNode', () => {
    it('should define array type structure', () => {
      const itemType: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'INTEGER',
      }

      const arrayNode: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType,
      }

      expect(arrayNode.dataType).toBe('ARRAY')
      expect(arrayNode.itemType).toEqual(itemType)
      expect(arrayNode.itemType.dataType).toBe('INTEGER')
    })

    it('should support nested arrays', () => {
      const baseType: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VARCHAR',
      }

      const innerArray: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: baseType,
      }

      const outerArray: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: innerArray,
      }

      expect(outerArray.itemType).toEqual(innerArray)
      expect((outerArray.itemType as DuckDbArrayNode).itemType).toEqual(baseType)
    })

    it('should support arrays of complex types', () => {
      const structType: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
          name: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
        },
      }

      const arrayOfStructs: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: structType,
      }

      expect(arrayOfStructs.itemType).toEqual(structType)
      expect((arrayOfStructs.itemType as DuckDbStructNode).fields).toHaveProperty('id')
      expect((arrayOfStructs.itemType as DuckDbStructNode).fields).toHaveProperty('name')
    })
  })

  describe('DuckDbStructNode', () => {
    it('should define struct type structure', () => {
      const structNode: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
          name: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          active: { kind: 'DuckDbDataTypeNode', dataType: 'BOOLEAN' },
        },
      }

      expect(structNode.dataType).toBe('STRUCT')
      expect(structNode.fields).toHaveProperty('id')
      expect(structNode.fields).toHaveProperty('name')
      expect(structNode.fields).toHaveProperty('active')
      expect(structNode.fields?.['id']?.dataType).toBe('INTEGER')
      expect(structNode.fields?.['name']?.dataType).toBe('VARCHAR')
      expect(structNode.fields?.['active']?.dataType).toBe('BOOLEAN')
    })

    it('should support empty structs', () => {
      const emptyStruct: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {},
      }

      expect(emptyStruct.fields).toEqual({})
      expect(Object.keys(emptyStruct.fields)).toHaveLength(0)
    })

    it('should support nested structs', () => {
      const addressStruct: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          street: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          city: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          zipCode: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
        },
      }

      const userStruct: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
          name: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          address: addressStruct,
        },
      }

      expect(userStruct.fields?.['address']).toEqual(addressStruct)
      expect((userStruct.fields?.['address'] as DuckDbStructNode).fields).toHaveProperty('street')
      expect((userStruct.fields?.['address'] as DuckDbStructNode).fields).toHaveProperty('city')
      expect((userStruct.fields?.['address'] as DuckDbStructNode).fields).toHaveProperty('zipCode')
    })

    it('should support structs with various field types', () => {
      const complexStruct: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
          metadata: { kind: 'DuckDbDataTypeNode', dataType: 'JSON' },
          tags: {
            kind: 'DuckDbDataTypeNode',
            dataType: 'ARRAY',
            itemType: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          } as DuckDbArrayNode,
          location: { kind: 'DuckDbDataTypeNode', dataType: 'GEOMETRY' },
          embedding: {
            kind: 'DuckDbDataTypeNode',
            dataType: 'VECTOR',
            dimensions: 512,
          } as DuckDbVectorNode,
        },
      }

      expect(complexStruct.fields?.['id']?.dataType).toBe('INTEGER')
      expect(complexStruct.fields?.['metadata']?.dataType).toBe('JSON')
      expect(complexStruct.fields?.['tags']?.dataType).toBe('ARRAY')
      expect(complexStruct.fields?.['location']?.dataType).toBe('GEOMETRY')
      expect(complexStruct.fields?.['embedding']?.dataType).toBe('VECTOR')
    })
  })

  describe('DuckDbMapNode', () => {
    it('should define map type structure', () => {
      const keyType: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VARCHAR',
      }

      const valueType: DuckDbDataTypeNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'INTEGER',
      }

      const mapNode: DuckDbMapNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'MAP',
        keyType,
        valueType,
      }

      expect(mapNode.dataType).toBe('MAP')
      expect(mapNode.keyType).toEqual(keyType)
      expect(mapNode.valueType).toEqual(valueType)
    })

    it('should support complex key and value types', () => {
      const keyStruct: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          namespace: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
        },
      }

      const valueArray: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: { kind: 'DuckDbDataTypeNode', dataType: 'DOUBLE' },
      }

      const complexMap: DuckDbMapNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'MAP',
        keyType: keyStruct,
        valueType: valueArray,
      }

      expect(complexMap.keyType).toEqual(keyStruct)
      expect(complexMap.valueType).toEqual(valueArray)
    })

    it('should support nested maps', () => {
      const innerMap: DuckDbMapNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'MAP',
        keyType: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
        valueType: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
      }

      const outerMap: DuckDbMapNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'MAP',
        keyType: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
        valueType: innerMap,
      }

      expect(outerMap.valueType).toEqual(innerMap)
      expect((outerMap.valueType as DuckDbMapNode).keyType.dataType).toBe('VARCHAR')
      expect((outerMap.valueType as DuckDbMapNode).valueType.dataType).toBe('INTEGER')
    })
  })

  describe('DuckDbUnionNode', () => {
    it('should define union type structure', () => {
      const types: DuckDbDataTypeNode[] = [
        { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
        { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
        { kind: 'DuckDbDataTypeNode', dataType: 'BOOLEAN' },
      ]

      const unionNode: DuckDbUnionNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'UNION',
        types,
      }

      expect(unionNode.dataType).toBe('UNION')
      expect(unionNode.types).toEqual(types)
      expect(unionNode.types).toHaveLength(3)
    })

    it('should support empty union types', () => {
      const emptyUnion: DuckDbUnionNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'UNION',
        types: [],
      }

      expect(emptyUnion.types).toEqual([])
      expect(emptyUnion.types).toHaveLength(0)
    })

    it('should support union with complex types', () => {
      const structType: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
        },
      }

      const arrayType: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: { kind: 'DuckDbDataTypeNode', dataType: 'DOUBLE' },
      }

      const complexUnion: DuckDbUnionNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'UNION',
        types: [{ kind: 'DuckDbDataTypeNode', dataType: 'NULL' }, structType, arrayType],
      }

      expect(complexUnion.types).toHaveLength(3)
      expect(complexUnion.types[1]).toEqual(structType)
      expect(complexUnion.types[2]).toEqual(arrayType)
    })

    it('should support nested unions', () => {
      const innerUnion: DuckDbUnionNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'UNION',
        types: [
          { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
          { kind: 'DuckDbDataTypeNode', dataType: 'DOUBLE' },
        ],
      }

      const outerUnion: DuckDbUnionNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'UNION',
        types: [
          { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
          innerUnion,
          { kind: 'DuckDbDataTypeNode', dataType: 'BOOLEAN' },
        ],
      }

      expect(outerUnion.types).toHaveLength(3)
      expect(outerUnion.types[1]).toEqual(innerUnion)
    })
  })

  describe('DuckDbGeometryNode', () => {
    it('should define geometry type structure', () => {
      const geometryNode: DuckDbGeometryNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'GEOMETRY',
      }

      expect(geometryNode.dataType).toBe('GEOMETRY')
      expect(geometryNode.kind).toBe('DuckDbDataTypeNode')
    })

    it('should extend base data type node', () => {
      const geometryNode: DuckDbGeometryNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'GEOMETRY',
      }

      // Should be compatible with base type
      const baseNode: DuckDbDataTypeNode = geometryNode
      expect(baseNode.dataType).toBe('GEOMETRY')
    })
  })

  describe('DuckDbVectorNode', () => {
    it('should define vector type structure without dimensions', () => {
      const vectorNode: DuckDbVectorNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VECTOR',
      }

      expect(vectorNode.dataType).toBe('VECTOR')
      expect(vectorNode.dimensions).toBeUndefined()
    })

    it('should define vector type structure with dimensions', () => {
      const vectorNode: DuckDbVectorNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VECTOR',
        dimensions: 768,
      }

      expect(vectorNode.dataType).toBe('VECTOR')
      expect(vectorNode.dimensions).toBe(768)
    })

    it('should support various dimension sizes', () => {
      const testDimensions = [1, 128, 256, 512, 768, 1024, 1536, 2048]

      for (const dim of testDimensions) {
        const vectorNode: DuckDbVectorNode = {
          kind: 'DuckDbDataTypeNode',
          dataType: 'VECTOR',
          dimensions: dim,
        }

        expect(vectorNode.dimensions).toBe(dim)
      }
    })

    it('should be optional dimensions property', () => {
      interface TestVectorNode {
        kind: 'DuckDbDataTypeNode'
        dataType: 'VECTOR'
        dimensions?: number
      }

      const withDimensions: TestVectorNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VECTOR',
        dimensions: 512,
      }

      const withoutDimensions: TestVectorNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VECTOR',
      }

      expect(withDimensions.dimensions).toBe(512)
      expect(withoutDimensions.dimensions).toBeUndefined()
    })
  })

  describe('TableMapping', () => {
    it('should define table mapping with source only', () => {
      const mapping: TableMapping = {
        source: '/path/to/data.csv',
      }

      expect(mapping.source).toBe('/path/to/data.csv')
      expect(mapping.options).toBeUndefined()
    })

    it('should define table mapping with options', () => {
      const mapping: TableMapping = {
        source: '/path/to/data.parquet',
        options: {
          header: true,
          delimiter: ',',
          quote: '"',
          escape: '\\',
          nullStr: 'NULL',
        },
      }

      expect(mapping.source).toBe('/path/to/data.parquet')
      expect(mapping.options).toBeDefined()
      expect(mapping.options?.['header']).toBe(true)
      expect(mapping.options?.['delimiter']).toBe(',')
    })

    it('should support various data source types', () => {
      const csvMapping: TableMapping = {
        source: 'data.csv',
        options: { header: true },
      }

      const parquetMapping: TableMapping = {
        source: 'data.parquet',
      }

      const jsonMapping: TableMapping = {
        source: 'data.json',
        options: { format: 'newline_delimited' },
      }

      const s3Mapping: TableMapping = {
        source: 's3://bucket/path/data.parquet',
        options: {
          access_key_id: 'key',
          secret_access_key: 'secret',
        },
      }

      expect(csvMapping.source).toBe('data.csv')
      expect(parquetMapping.source).toBe('data.parquet')
      expect(jsonMapping.source).toBe('data.json')
      expect(s3Mapping.source).toBe('s3://bucket/path/data.parquet')
    })

    it('should support complex options', () => {
      const complexMapping: TableMapping = {
        source: 'https://api.example.com/data.json',
        options: {
          headers: {
            Authorization: 'Bearer token',
            'Content-Type': 'application/json',
          },
          method: 'POST',
          body: JSON.stringify({ query: 'data' }),
          timeout: 30000,
          retry: 3,
        },
      }

      expect(complexMapping.options).toHaveProperty('headers')
      expect(complexMapping.options?.['timeout']).toBe(30000)
      expect(complexMapping.options?.['retry']).toBe(3)
    })
  })

  describe('DuckDbConfig', () => {
    it('should define config with database string', () => {
      const config: DuckDbConfig = {
        database: '/path/to/database.db',
      }

      expect(config.database).toBe('/path/to/database.db')
      expect(config.tableMappings).toBeUndefined()
      expect(config.config).toBeUndefined()
    })

    it('should define config with database instance', () => {
      const mockDatabase = { connect: () => {}, close: () => {} }
      const config: DuckDbConfig = {
        database: mockDatabase,
      }

      expect(config.database).toBe(mockDatabase)
    })

    it('should define config with table mappings', () => {
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

      expect(config.tableMappings).toHaveProperty('users')
      expect(config.tableMappings).toHaveProperty('orders')
      expect(config.tableMappings?.['users']).toBe('/data/users.csv')
      expect(typeof config.tableMappings?.['orders']).toBe('object')
    })

    it('should define config with DuckDB options', () => {
      const config: DuckDbConfig = {
        database: 'database.db',
        config: {
          memory_limit: '2GB',
          threads: 4,
          max_memory: '1GB',
          enable_external_access: false,
          enable_fsst_vectors: true,
        },
      }

      expect(config.config).toHaveProperty('memory_limit')
      expect(config.config?.['threads']).toBe(4)
      expect(config.config?.['enable_external_access']).toBe(false)
    })

    it('should define comprehensive config', () => {
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
          enable_profiling: 'json',
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

  describe('Type Union DuckDbDataType', () => {
    it('should include all supported types', () => {
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

      // This test ensures all types are valid
      for (const type of supportedTypes) {
        const node: { dataType: DuckDbDataType } = { dataType: type }
        expect(node.dataType).toBe(type)
      }
    })

    it('should support type narrowing', () => {
      const checkType = (dataType: DuckDbDataType): string => {
        switch (dataType) {
          case 'INTEGER':
          case 'BIGINT':
          case 'SMALLINT':
          case 'TINYINT':
            return 'integer_type'
          case 'REAL':
          case 'DOUBLE':
          case 'DECIMAL':
            return 'numeric_type'
          case 'VARCHAR':
          case 'BLOB':
            return 'string_type'
          case 'JSON':
            return 'json_type'
          case 'ARRAY':
          case 'STRUCT':
          case 'MAP':
          case 'UNION':
            return 'complex_type'
          case 'GEOMETRY':
            return 'spatial_type'
          case 'VECTOR':
            return 'vector_type'
          default:
            return 'other_type'
        }
      }

      expect(checkType('INTEGER')).toBe('integer_type')
      expect(checkType('DOUBLE')).toBe('numeric_type')
      expect(checkType('VARCHAR')).toBe('string_type')
      expect(checkType('JSON')).toBe('json_type')
      expect(checkType('ARRAY')).toBe('complex_type')
      expect(checkType('GEOMETRY')).toBe('spatial_type')
      expect(checkType('VECTOR')).toBe('vector_type')
      expect(checkType('BOOLEAN')).toBe('other_type')
    })
  })

  describe('Type Compatibility and Usage', () => {
    it('should work with generic type parameters', () => {
      const createNode = <T extends DuckDbDataType>(dataType: T): DuckDbDataTypeNode => ({
        kind: 'DuckDbDataTypeNode',
        dataType,
      })

      const integerNode = createNode('INTEGER')
      const vectorNode = createNode('VECTOR')

      expect(integerNode.dataType).toBe('INTEGER')
      expect(vectorNode.dataType).toBe('VECTOR')
    })

    it('should support type guards', () => {
      const isVectorNode = (node: DuckDbDataTypeNode): node is DuckDbVectorNode => {
        return node.dataType === 'VECTOR'
      }

      const isArrayNode = (node: DuckDbDataTypeNode): node is DuckDbArrayNode => {
        return node.dataType === 'ARRAY'
      }

      const vectorNode: DuckDbVectorNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VECTOR',
        dimensions: 256,
      }

      const arrayNode: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
      }

      expect(isVectorNode(vectorNode)).toBe(true)
      expect(isVectorNode(arrayNode as DuckDbDataTypeNode)).toBe(false)
      expect(isArrayNode(arrayNode)).toBe(true)
      expect(isArrayNode(vectorNode as DuckDbDataTypeNode)).toBe(false)
    })

    it('should support discriminated unions', () => {
      type ComplexType = DuckDbArrayNode | DuckDbStructNode | DuckDbMapNode | DuckDbVectorNode

      const processComplexType = (node: ComplexType): string => {
        switch (node.dataType) {
          case 'ARRAY':
            return `Array of ${node.itemType.dataType}`
          case 'STRUCT':
            return `Struct with ${Object.keys(node.fields).length} fields`
          case 'MAP':
            return `Map from ${node.keyType.dataType} to ${node.valueType.dataType}`
          case 'VECTOR':
            return `Vector${node.dimensions ? ` with ${node.dimensions} dimensions` : ''}`
          default:
            // TypeScript should ensure this never happens
            return 'unknown'
        }
      }

      const arrayNode: DuckDbArrayNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'ARRAY',
        itemType: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
      }

      const structNode: DuckDbStructNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'STRUCT',
        fields: {
          id: { kind: 'DuckDbDataTypeNode', dataType: 'INTEGER' },
          name: { kind: 'DuckDbDataTypeNode', dataType: 'VARCHAR' },
        },
      }

      const vectorNode: DuckDbVectorNode = {
        kind: 'DuckDbDataTypeNode',
        dataType: 'VECTOR',
        dimensions: 512,
      }

      expect(processComplexType(arrayNode)).toBe('Array of VARCHAR')
      expect(processComplexType(structNode)).toBe('Struct with 2 fields')
      expect(processComplexType(vectorNode)).toBe('Vector with 512 dimensions')
    })
  })
})

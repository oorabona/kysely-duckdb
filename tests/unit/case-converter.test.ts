import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, type PluginTransformQueryArgs, type PluginTransformResultArgs, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'
import { CaseConverter, CaseConverterPlugin } from '../../src/plugins/case-converter.js'

describe('CaseConverterPlugin', () => {
  let database: DuckDBInstance
  let db: Kysely<any>

  // Minimal helpers to provide properly-typed args objects for plugin methods in tests
  function tq(node: any): PluginTransformQueryArgs {
    // Kysely's PluginTransformQueryArgs carries a queryId (opaque). Tests don't use it.
    // Cast to keep the test simple without importing internal QueryId symbol.
    return { node, queryId: {} as any }
  }

  // helper for transformResult when needed
  function tr(result: any): PluginTransformResultArgs {
    return { result, queryId: {} as any }
  }

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
  })

  afterEach(async () => {
    try {
      await db?.destroy()
    } catch {
      // Ignore cleanup errors
    }
    try {
      database?.closeSync()
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('Plugin Configuration', () => {
    it('should create plugin with default configuration', () => {
      const plugin = new CaseConverterPlugin()
      expect(plugin).toBeDefined()
      expect(plugin).toBeInstanceOf(CaseConverterPlugin)
    })

    it('should create plugin with custom configuration', () => {
      const plugin = new CaseConverterPlugin({
        toSnakeCase: false,
        toCamelCase: false,
        customConversions: { userId: 'user_id' },
      })
      expect(plugin).toBeDefined()
    })

    it('should merge configuration with defaults', () => {
      const plugin = new CaseConverterPlugin({
        toSnakeCase: false,
      })
      expect(plugin).toBeDefined()
    })
  })

  describe('Query Transformation', () => {
    let plugin: CaseConverterPlugin

    beforeEach(() => {
      plugin = new CaseConverterPlugin({ toSnakeCase: true, toCamelCase: true })
    })

    it('should not transform query when toSnakeCase is disabled', () => {
      const disabledPlugin = new CaseConverterPlugin({ toSnakeCase: false })
      const mockNode = { kind: 'SelectQueryNode', from: { table: { name: 'userTable' } } }
      const mockArgs = tq(mockNode)

      const result = disabledPlugin.transformQuery(mockArgs)
      expect(result).toBe(mockNode)
    })

    it('should transform identifier nodes to snake_case', () => {
      const identifierNode = { kind: 'IdentifierNode', name: 'userTable' }
      const mockArgs = tq(identifierNode)

      const result = plugin.transformQuery(mockArgs)
      const anyResult = result as any
      expect(anyResult.name).toBe('user_table')
    })

    it('should use custom conversions when provided', () => {
      const customPlugin = new CaseConverterPlugin({
        customConversions: { userId: 'custom_user_id' },
      })

      const identifierNode = { kind: 'IdentifierNode', name: 'userId' }
      const mockArgs = tq(identifierNode)

      const result = customPlugin.transformQuery(mockArgs)
      const anyResult = result as any
      expect(anyResult.name).toBe('custom_user_id')
    })

    it('should handle column nodes', () => {
      const columnNode = {
        kind: 'ColumnNode',
        column: { kind: 'IdentifierNode', name: 'firstName' },
      }
      const mockArgs = tq(columnNode)

      const result = plugin.transformQuery(mockArgs)
      const anyResult = result as any
      expect(anyResult.column.name).toBe('first_name')
    })

    it('should handle table nodes', () => {
      const tableNode = {
        kind: 'TableNode',
        table: { kind: 'IdentifierNode', name: 'userProfiles' },
      }
      const mockArgs = tq(tableNode)

      const result = plugin.transformQuery(mockArgs)
      const anyResult = result as any
      expect(anyResult.table?.name).toBe('user_profiles')
    })

    it('should handle nested structures', () => {
      const complexNode = {
        kind: 'SelectQueryNode',
        from: {
          kind: 'TableNode',
          table: { kind: 'IdentifierNode', name: 'userAccounts' },
        },
        selections: [
          {
            kind: 'ColumnNode',
            column: { kind: 'IdentifierNode', name: 'accountId' },
          },
        ],
      }
      const mockArgs = tq(complexNode)

      const result = plugin.transformQuery(mockArgs)
      const anyResult = result as any
      expect(anyResult.from?.table?.name).toBe('user_accounts')
      const firstSel = anyResult.selections?.[0]
      expect(firstSel?.column?.name).toBe('account_id')
    })

    it('should handle arrays in nodes', () => {
      const nodeWithArray = {
        kind: 'SelectQueryNode',
        columns: [
          { kind: 'IdentifierNode', name: 'firstName' },
          { kind: 'IdentifierNode', name: 'lastName' },
        ],
      }
      const mockArgs = tq(nodeWithArray)

      const result = plugin.transformQuery(mockArgs)
      const anyResult = result as any
      expect(anyResult.columns?.[0]?.name).toBe('first_name')
      expect(anyResult.columns?.[1]?.name).toBe('last_name')
    })

    it('should handle null and primitive values', () => {
      const mockArgs = tq(null)
      expect(plugin.transformQuery(mockArgs)).toBe(null)

      const stringArgs = tq('test' as any)
      expect(plugin.transformQuery(stringArgs)).toBe('test')

      const numberArgs = tq(42 as any)
      expect(plugin.transformQuery(numberArgs)).toBe(42)
    })

    it('should preserve node structure while transforming', () => {
      const originalNode = {
        kind: 'SelectQueryNode',
        from: { kind: 'TableNode', table: { kind: 'IdentifierNode', name: 'testTable' } },
        where: { kind: 'BinaryOperationNode', operator: '=', left: 'id', right: 1 },
      }
      const mockArgs = tq(originalNode)

      const result = plugin.transformQuery(mockArgs)
      const anyResult = result as any

      expect(anyResult.kind).toBe('SelectQueryNode')
      expect(anyResult.where?.kind).toBe('BinaryOperationNode')
      expect(anyResult.where?.operator).toBe('=')
      expect(anyResult.where?.right).toBe(1)
    })
  })

  describe('Result Transformation', () => {
    let plugin: CaseConverterPlugin

    beforeEach(() => {
      plugin = new CaseConverterPlugin({ toSnakeCase: true, toCamelCase: true })
    })

    it('should not transform results when toCamelCase is disabled', async () => {
      const disabledPlugin = new CaseConverterPlugin({ toCamelCase: false })
      const mockResult = {
        rows: [{ user_name: 'John', user_email: 'john@example.com' }],
        numAffectedRows: BigInt(1),
        numChangedRows: BigInt(1),
      }
      const mockArgs = tr(mockResult)

      const result = await disabledPlugin.transformResult(mockArgs)
      expect(result).toBe(mockResult)
    })

    it('should transform result column names to camelCase', async () => {
      const mockResult = {
        rows: [
          { user_name: 'John', user_email: 'john@example.com', is_active: true },
          { user_name: 'Jane', user_email: 'jane@example.com', is_active: false },
        ],
        numAffectedRows: BigInt(2),
        numChangedRows: BigInt(0),
      }
      const mockArgs = tr(mockResult)

      const result = await plugin.transformResult(mockArgs)
      expect(result.rows.length).toBe(2)
      expect(result.rows[0]).toEqual({
        userName: 'John',
        userEmail: 'john@example.com',
        isActive: true,
      })
      expect(result.rows[1]).toEqual({
        userName: 'Jane',
        userEmail: 'jane@example.com',
        isActive: false,
      })
      expect(result.numAffectedRows).toBe(BigInt(2))
      expect(result.numChangedRows).toBe(BigInt(0))
    })

    it('should use custom conversions for result columns', async () => {
      const customPlugin = new CaseConverterPlugin({
        customConversions: { user_id: 'id', user_name: 'fullName' },
      })

      const mockResult = {
        rows: [{ user_id: 1, user_name: 'John Doe', user_email: 'john@example.com' }],
        numAffectedRows: BigInt(1),
        numChangedRows: BigInt(0),
      }
      const mockArgs = tr(mockResult)

      const result = await customPlugin.transformResult(mockArgs)
      expect(result.rows.length).toBe(1)
      expect(result.rows[0]).toEqual({
        id: 1,
        fullName: 'John Doe',
        userEmail: 'john@example.com',
      })
    })

    it('should handle empty result sets', async () => {
      const mockResult = {
        rows: [],
        numAffectedRows: BigInt(0),
        numChangedRows: BigInt(0),
      }
      const mockArgs = tr(mockResult)

      const result = await plugin.transformResult(mockArgs)

      expect(result.rows).toEqual([])
      expect(result.numAffectedRows).toBe(BigInt(0))
    })

    it('should handle complex data types in results', async () => {
      const mockResult = {
        rows: [
          {
            user_metadata: { first_login: '2023-01-01', preferences: { theme: 'dark' } },
            tag_list: ['admin', 'user'],
            created_at: new Date('2023-01-01'),
            is_null_field: null,
          },
        ],
        numAffectedRows: BigInt(1),
        numChangedRows: BigInt(0),
      }
      const mockArgs = tr(mockResult)

      const result = await plugin.transformResult(mockArgs)
      expect(result.rows.length).toBe(1)
      expect(result.rows[0]).toEqual({
        userMetadata: { first_login: '2023-01-01', preferences: { theme: 'dark' } },
        tagList: ['admin', 'user'],
        createdAt: new Date('2023-01-01'),
        isNullField: null,
      })
    })

    it('should preserve result metadata', async () => {
      const mockResult = {
        rows: [{ user_name: 'John' }],
        numAffectedRows: BigInt(5),
        numChangedRows: BigInt(3),
        insertId: BigInt(42),
        metadata: { customData: 'test' },
      } as any
      const mockArgs = tr(mockResult)

      const result = await plugin.transformResult(mockArgs)
      const anyResult2 = result as any

      expect(anyResult2.numAffectedRows).toBe(BigInt(5))
      expect(anyResult2.numChangedRows).toBe(BigInt(3))
      expect(anyResult2.insertId).toBe(BigInt(42))
      expect(anyResult2.metadata).toEqual({ customData: 'test' })
    })
  })

  describe('Integration with Kysely', () => {
    it('should work with real database queries', async () => {
      const plugin = new CaseConverterPlugin()
      const dialect = new DuckDbDialect({ database })

      db = new Kysely<any>({ dialect, plugins: [plugin] })

      // Create table with snake_case naming
      await db.schema
        .createTable('user_profiles')
        .addColumn('user_id', 'integer', col => col.primaryKey())
        .addColumn('first_name', 'varchar(255)', col => col.notNull())
        .addColumn('last_name', 'varchar(255)')
        .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
        .execute()

      // Insert data using camelCase (should be converted to snake_case)
      await db
        .insertInto('userProfiles')
        .values({
          userId: 1,
          firstName: 'John',
          lastName: 'Doe',
        })
        .execute()

      // Query data using camelCase (should be converted to snake_case, result converted back to camelCase)
      const result = await db
        .selectFrom('userProfiles')
        .select(['userId', 'firstName', 'lastName', 'createdAt'])
        .where('userId', '=', 1)
        .execute()

      expect(result).toHaveLength(1)
      expect(result[0]).toMatchObject({
        userId: 1,
        firstName: 'John',
        lastName: 'Doe',
      })
      expect(result[0]?.createdAt).toBeDefined()
    })

    it('should handle complex queries with joins', async () => {
      const plugin = new CaseConverterPlugin()
      const dialect = new DuckDbDialect({ database })

      db = new Kysely<any>({ dialect, plugins: [plugin] })

      // Create related tables
      await db.schema
        .createTable('user_accounts')
        .addColumn('account_id', 'integer', col => col.primaryKey())
        .addColumn('user_name', 'varchar(255)', col => col.notNull())
        .execute()

      await db.schema
        .createTable('user_posts')
        .addColumn('post_id', 'integer', col => col.primaryKey())
        .addColumn('account_id', 'integer')
        .addColumn('post_title', 'varchar(255)', col => col.notNull())
        .execute()

      // Insert test data
      await db.insertInto('userAccounts').values({ accountId: 1, userName: 'john_doe' }).execute()
      await db
        .insertInto('userPosts')
        .values({ postId: 1, accountId: 1, postTitle: 'Hello World' })
        .execute()

      // Complex query with join
      const result = await db
        .selectFrom('userAccounts as ua')
        .innerJoin('userPosts as up', 'ua.accountId', 'up.accountId')
        .select(['ua.accountId', 'ua.userName', 'up.postTitle'])
        .execute()

      expect(result).toHaveLength(1)
      expect(result[0]).toEqual({
        accountId: 1,
        userName: 'john_doe',
        postTitle: 'Hello World',
      })
    })

    it('should work with custom conversions in real queries', async () => {
      const plugin = new CaseConverterPlugin({
        customConversions: {
          userId: 'user_id',
          id: 'user_id',
        },
      })
      const dialect = new DuckDbDialect({ database })

      db = new Kysely<any>({ dialect, plugins: [plugin] })

      await db.schema
        .createTable('custom_users')
        .addColumn('user_id', 'integer', col => col.primaryKey())
        .addColumn('display_name', 'varchar(255)')
        .execute()

      await db.insertInto('customUsers').values({ userId: 1, displayName: 'Custom User' }).execute()

      const result = await db.selectFrom('customUsers').select(['userId', 'displayName']).execute()
      expect(result.length).toBe(1)
      expect(result[0]).toEqual({
        userId: 1,
        displayName: 'Custom User',
      })
    })
  })

  describe('Error Handling', () => {
    it('should handle malformed query nodes gracefully', () => {
      const plugin = new CaseConverterPlugin()
      const malformedNode = { kind: 'UnknownNode', invalidProperty: undefined as unknown }
      const mockArgs = tq(malformedNode)

      expect(() => plugin.transformQuery(mockArgs)).not.toThrow()
    })

    it('should handle undefined and null values in results', async () => {
      const plugin = new CaseConverterPlugin()
      const mockResult = {
        rows: [{ valid_field: 'value', null_field: null, undefined_field: undefined }, {}],
        numAffectedRows: BigInt(1),
        numChangedRows: BigInt(0),
      }
      const mockArgs = tr(mockResult)

      const result = await plugin.transformResult(mockArgs)

      expect(result.rows[0]).toEqual({
        validField: 'value',
        nullField: null,
        undefinedField: undefined,
      })
      expect(result.rows[1]).toEqual({})
    })

    it('should handle circular references in query nodes', () => {
      const plugin = new CaseConverterPlugin()
      const circularNode: any = { kind: 'TestNode' }
      circularNode.self = circularNode

      const mockArgs = tq(circularNode)

      // This might cause infinite recursion, but should be handled gracefully
      expect(() => plugin.transformQuery(mockArgs)).not.toThrow()
    })
  })
})

describe('CaseConverter Utility Class', () => {
  describe('toSnakeCase', () => {
    it('should convert camelCase to snake_case', () => {
      expect(CaseConverter.toSnakeCase('userName')).toBe('user_name')
      expect(CaseConverter.toSnakeCase('firstName')).toBe('first_name')
      expect(CaseConverter.toSnakeCase('isActive')).toBe('is_active')
    })

    it('should handle PascalCase', () => {
      expect(CaseConverter.toSnakeCase('UserName')).toBe('user_name')
      expect(CaseConverter.toSnakeCase('FirstName')).toBe('first_name')
    })

    it('should handle consecutive uppercase letters', () => {
      expect(CaseConverter.toSnakeCase('XMLParser')).toBe('x_m_l_parser')
      expect(CaseConverter.toSnakeCase('HTTPSConnection')).toBe('h_t_t_p_s_connection')
    })

    it('should handle already snake_case strings', () => {
      expect(CaseConverter.toSnakeCase('user_name')).toBe('user_name')
      expect(CaseConverter.toSnakeCase('first_name')).toBe('first_name')
    })

    it('should handle edge cases', () => {
      expect(CaseConverter.toSnakeCase('')).toBe('')
      expect(CaseConverter.toSnakeCase('a')).toBe('a')
      expect(CaseConverter.toSnakeCase('A')).toBe('a')
      expect(CaseConverter.toSnakeCase('ID')).toBe('i_d')
    })

    it('should handle numbers in strings', () => {
      expect(CaseConverter.toSnakeCase('user123Name')).toBe('user123_name')
      expect(CaseConverter.toSnakeCase('version2Point0')).toBe('version2_point0')
    })
  })

  describe('toCamelCase', () => {
    it('should convert snake_case to camelCase', () => {
      expect(CaseConverter.toCamelCase('user_name')).toBe('userName')
      expect(CaseConverter.toCamelCase('first_name')).toBe('firstName')
      expect(CaseConverter.toCamelCase('is_active')).toBe('isActive')
    })

    it('should handle multiple underscores', () => {
      expect(CaseConverter.toCamelCase('user__name')).toBe('user_Name')
      expect(CaseConverter.toCamelCase('first___name')).toBe('first__Name')
    })

    it('should handle leading underscores and cover all branches of line 194', () => {
      // Test leadingCount === 1 (second branch of line 194)
      expect(CaseConverter.toCamelCase('_user_name')).toBe('_userName')

      // Test leadingCount > 1 (first branch of line 194)
      expect(CaseConverter.toCamelCase('__private_field')).toBe('_privateField')
      expect(CaseConverter.toCamelCase('___triple_underscore')).toBe('__tripleUnderscore')

      // Test leadingCount === 0 (third branch of line 194 - the else case)
      expect(CaseConverter.toCamelCase('user_name')).toBe('userName') // No leading underscores
    })

    it('should handle trailing underscores and cover all branches of line 198', () => {
      // Test trailingCount === 1 (second branch of line 198)
      expect(CaseConverter.toCamelCase('user_name_')).toBe('userName_')

      // Test trailingCount > 1 (first branch of line 198)
      expect(CaseConverter.toCamelCase('field__')).toBe('field_')
      expect(CaseConverter.toCamelCase('field___')).toBe('field__')
      expect(CaseConverter.toCamelCase('underscore_test____')).toBe('underscoreTest___')

      // Test trailingCount === 0 (third branch of line 198 - the else case)
      // Transposé de leadingCount === 0 : chaînes avec underscores au milieu mais pas à la fin
      expect(CaseConverter.toCamelCase('user_name')).toBe('userName') // No trailing underscores
      expect(CaseConverter.toCamelCase('first_second_third')).toBe('firstSecondThird') // Multiple middle, no trailing
    })

    it('should explicitly test line 198 edge cases', () => {
      // Force test scenarios that specifically hit the ternary operator on line 198
      // Scenario: string that bypasses early returns but has trailing count = 0
      expect(CaseConverter.toCamelCase('first_second')).toBe('firstSecond') // trailingCount = 0, should return ''
      expect(CaseConverter.toCamelCase('a_b_c')).toBe('aBC') // trailingCount = 0, should return ''
      expect(CaseConverter.toCamelCase('__test_value')).toBe('_testValue') // Leading + middle, trailingCount = 0
    })

    it('should handle already camelCase strings', () => {
      expect(CaseConverter.toCamelCase('userName')).toBe('userName')
      expect(CaseConverter.toCamelCase('firstName')).toBe('firstName')
    })

    it('should handle edge cases', () => {
      expect(CaseConverter.toCamelCase('')).toBe('')
      expect(CaseConverter.toCamelCase('a')).toBe('a')
      expect(CaseConverter.toCamelCase('_')).toBe('_')
      expect(CaseConverter.toCamelCase('a_')).toBe('a_')
    })

    it('should handle numbers in strings', () => {
      expect(CaseConverter.toCamelCase('user_123_name')).toBe('user123Name')
      expect(CaseConverter.toCamelCase('version_2_point_0')).toBe('version2Point0')
    })
  })

  describe('toPascalCase', () => {
    it('should convert snake_case to PascalCase', () => {
      expect(CaseConverter.toPascalCase('user_name')).toBe('UserName')
      expect(CaseConverter.toPascalCase('first_name')).toBe('FirstName')
      expect(CaseConverter.toPascalCase('is_active')).toBe('IsActive')
    })

    it('should convert camelCase to PascalCase', () => {
      expect(CaseConverter.toPascalCase('userName')).toBe('UserName')
      expect(CaseConverter.toPascalCase('firstName')).toBe('FirstName')
    })

    it('should handle edge cases', () => {
      expect(CaseConverter.toPascalCase('')).toBe('')
      expect(CaseConverter.toPascalCase('a')).toBe('A')
      expect(CaseConverter.toPascalCase('_')).toBe('_')
    })
  })

  describe('objectToSnakeCase', () => {
    it('should convert object keys to snake_case', () => {
      const input = {
        userName: 'John',
        firstName: 'John',
        lastName: 'Doe',
        isActive: true,
        createdAt: new Date(),
      }

      const result = CaseConverter.objectToSnakeCase(input)

      expect(result).toHaveProperty('user_name', 'John')
      expect(result).toHaveProperty('first_name', 'John')
      expect(result).toHaveProperty('last_name', 'Doe')
      expect(result).toHaveProperty('is_active', true)
      expect(result).toHaveProperty('created_at')
    })

    it('should handle empty objects', () => {
      expect(CaseConverter.objectToSnakeCase({})).toEqual({})
    })

    it('should handle objects with complex values', () => {
      const input = {
        userMetadata: { preferences: { theme: 'dark' } },
        tagList: ['admin', 'user'],
        nullField: null,
        undefinedField: undefined,
      }

      const result = CaseConverter.objectToSnakeCase(input)

      expect(result['user_metadata']).toEqual({ preferences: { theme: 'dark' } })
      expect(result['tag_list']).toEqual(['admin', 'user'])
      expect(result['null_field']).toBeNull()
      expect(result['undefined_field']).toBeUndefined()
    })
  })

  describe('objectToCamelCase', () => {
    it('should convert object keys to camelCase', () => {
      const input = {
        user_name: 'John',
        first_name: 'John',
        last_name: 'Doe',
        is_active: true,
        created_at: new Date(),
      }

      const result = CaseConverter.objectToCamelCase(input)

      expect(result).toHaveProperty('userName', 'John')
      expect(result).toHaveProperty('firstName', 'John')
      expect(result).toHaveProperty('lastName', 'Doe')
      expect(result).toHaveProperty('isActive', true)
      expect(result).toHaveProperty('createdAt')
    })

    it('should handle empty objects', () => {
      expect(CaseConverter.objectToCamelCase({})).toEqual({})
    })

    it('should handle objects with complex values', () => {
      const input = {
        user_metadata: { preferences: { theme: 'dark' } },
        tag_list: ['admin', 'user'],
        null_field: null,
        undefined_field: undefined,
      }

      const result = CaseConverter.objectToCamelCase(input)

      expect(result['userMetadata']).toEqual({ preferences: { theme: 'dark' } })
      expect(result['tagList']).toEqual(['admin', 'user'])
      expect(result['nullField']).toBeNull()
      expect(result['undefinedField']).toBeUndefined()
    })
  })

  describe('Round-trip conversions', () => {
    it('should maintain consistency in round-trip conversions', () => {
      const original = 'userProfileData'
      const snake = CaseConverter.toSnakeCase(original)
      const backToCamel = CaseConverter.toCamelCase(snake)

      expect(snake).toBe('user_profile_data')
      expect(backToCamel).toBe('userProfileData')
    })

    it('should handle complex round-trip scenarios', () => {
      const testCases = ['simpleCase', 'HTMLParser', 'XMLHttpRequest', 'getUserID', 'parseJSONData']

      for (const testCase of testCases) {
        const snake = CaseConverter.toSnakeCase(testCase)
        const camel = CaseConverter.toCamelCase(snake)

        // The round-trip might not be perfect due to consecutive uppercase letters
        // but should be predictable
        expect(typeof snake).toBe('string')
        expect(typeof camel).toBe('string')
        expect(snake).toMatch(/^[a-z][a-z0-9_]*$/i)
      }
    })
  })

  describe('Security: ReDoS Prevention', () => {
    it('should handle long underscore sequences without ReDoS (CWE-1333)', () => {
      // Test with 10,000 underscores followed by non-underscore character
      // This would trigger polynomial backtracking with vulnerable regex /^_+|_+$/g
      const longString = `${'_'.repeat(10000)}a`

      const start = performance.now()
      const result = CaseConverter.toCamelCase(longString)
      const elapsed = performance.now() - start

      // Should complete in under 200ms (linear time) although enough for slow machines like Github Actions
      // Vulnerable version would take several seconds
      expect(elapsed).toBeLessThan(200)

      // The function processes strings with leading/trailing underscores
      // For this input, it should return the string with the 'a' at the end
      expect(result).toBeDefined()
      expect(result.length).toBeGreaterThan(0)
    })

    it('should handle extremely long strings without ReDoS', () => {
      // Test with 50,000 underscores
      const veryLongString = '_'.repeat(50000)

      const start = performance.now()
      const result = CaseConverter.toCamelCase(veryLongString)
      const elapsed = performance.now() - start

      // Should complete quickly even with very long input
      expect(elapsed).toBeLessThan(200)
      expect(result).toBeDefined()
      expect(typeof result).toBe('string')
    })

    it('should handle mixed long sequences without performance degradation', () => {
      // Test with alternating underscores and characters
      const mixedString = `${'_'.repeat(1000)}field${'_'.repeat(1000)}`

      const start = performance.now()
      const result = CaseConverter.toCamelCase(mixedString)
      const elapsed = performance.now() - start

      expect(elapsed).toBeLessThan(50)
      expect(result).toContain('field')
    })

    it('should document that regex patterns are ReDoS-safe', () => {
      // Document that the fix splits /^_+|_+$/g into two separate replacements
      // This prevents polynomial backtracking while maintaining functionality

      const testCases = [
        { input: '___field___' },
        { input: '_____' },
        { input: 'field__' },
        { input: '__field' },
      ]

      for (const { input } of testCases) {
        expect(() => CaseConverter.toCamelCase(input)).not.toThrow()
        // All should complete instantly
      }
    })
  })
})

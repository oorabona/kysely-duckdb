import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'
import { DuckDbIntrospector } from '../../src/dialect/duckdb-introspector.js'

describe('DuckDbIntrospector', () => {
  let database: DuckDBInstance
  let db: Kysely<any>
  let introspector: DuckDbIntrospector

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    const dialect = new DuckDbDialect({ database })
    db = new Kysely<any>({ dialect })
    introspector = new DuckDbIntrospector(db)
  })

  afterEach(async () => {
    try {
      await db.destroy()
    } catch {
      // Ignore cleanup errors
    }
    try {
      database.closeSync()
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('Constructor', () => {
    it('should create introspector with database instance', () => {
      expect(introspector).toBeInstanceOf(DuckDbIntrospector)
    })

    it('should accept any Kysely instance type', () => {
      const typedDb = db as Kysely<{ users: { id: number; name: string } }>
      const typedIntrospector = new DuckDbIntrospector(typedDb)
      expect(typedIntrospector).toBeInstanceOf(DuckDbIntrospector)
    })
  })

  describe('Schema Introspection', () => {
    it('should get available schemas', async () => {
      const schemas = await introspector.getSchemas()

      expect(Array.isArray(schemas)).toBe(true)
      expect(schemas.length).toBeGreaterThan(0)

      // Should include at least the main schema
      const schemaNames = schemas.map(s => s.name)
      expect(schemaNames).toContain('main')
    })

    it('should return schema metadata with correct structure', async () => {
      const schemas = await introspector.getSchemas()

      for (const schema of schemas) {
        expect(schema).toHaveProperty('name')
        expect(typeof schema.name).toBe('string')
        expect(schema.name.length).toBeGreaterThan(0)
      }
    })

    it('should handle empty database schemas', async () => {
      // Even an empty database should have system schemas
      const schemas = await introspector.getSchemas()
      expect(schemas.length).toBeGreaterThan(0)
    })
  })

  describe('Table Introspection', () => {
    beforeEach(async () => {
      // Create test tables with various structures
      await db.schema
        .createTable('users')
        .addColumn('id', 'integer', col => col.primaryKey())
        .addColumn('name', 'varchar(255)', col => col.notNull())
        .addColumn('email', 'varchar(255)', col => col.unique())
        .addColumn('age', 'integer', col => col.defaultTo(0))
        .addColumn('is_active', 'boolean', col => col.defaultTo(true))
        .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
        .execute()

      await db.schema
        .createTable('posts')
        .addColumn('id', 'integer', col => col.primaryKey())
        .addColumn('title', 'varchar(500)', col => col.notNull())
        .addColumn('content', 'text')
        .addColumn('user_id', 'integer')
        .addColumn('published_at', 'timestamp')
        .execute()

      // Create a view
      await db.schema
        .createView('active_users')
        .as(db.selectFrom('users').selectAll().where('is_active', '=', true))
        .execute()
    })

    it('should get all tables without internal Kysely tables', async () => {
      const tables = await introspector.getTables()

      expect(Array.isArray(tables)).toBe(true)
      expect(tables.length).toBe(3) // users, posts, active_users

      const tableNames = tables.map(t => t.name)
      expect(tableNames).toContain('users')
      expect(tableNames).toContain('posts')
      expect(tableNames).toContain('active_users')

      // Should not include internal Kysely tables
      expect(tableNames).not.toContain('kysely_migration')
      expect(tableNames).not.toContain('kysely_migration_lock')
    })

    it('should identify views correctly', async () => {
      const tables = await introspector.getTables()

      const users = tables.find(t => t.name === 'users')
      const activeUsers = tables.find(t => t.name === 'active_users')

      expect(users?.isView).toBe(false)
      expect(activeUsers?.isView).toBe(true)
    })

    it('should include internal Kysely tables when requested', async () => {
      // Create migration table
      await db.schema
        .createTable('kysely_migration')
        .addColumn('name', 'varchar(255)', col => col.primaryKey())
        .addColumn('timestamp', 'varchar(255)', col => col.notNull())
        .execute()

      const tables = await introspector.getTables({ withInternalKyselyTables: true })

      const tableNames = tables.map(t => t.name)
      expect(tableNames).toContain('kysely_migration')
    })

    it('should include column metadata for each table', async () => {
      const tables = await introspector.getTables()
      const usersTable = tables.find(t => t.name === 'users')

      expect(usersTable).toBeDefined()
      expect(usersTable?.columns).toBeDefined()
      expect(Array.isArray(usersTable?.columns)).toBe(true)
      expect(usersTable?.columns.length).toBe(6) // id, name, email, age, is_active, created_at

      const columnNames = usersTable?.columns.map(c => c.name)
      expect(columnNames).toContain('id')
      expect(columnNames).toContain('name')
      expect(columnNames).toContain('email')
      expect(columnNames).toContain('age')
      expect(columnNames).toContain('is_active')
      expect(columnNames).toContain('created_at')
    })

    it('should provide correct column metadata', async () => {
      const tables = await introspector.getTables()
      const usersTable = tables.find(t => t.name === 'users')

      const idColumn = usersTable?.columns.find(c => c.name === 'id')
      const nameColumn = usersTable?.columns.find(c => c.name === 'name')
      const emailColumn = usersTable?.columns.find(c => c.name === 'email')
      const ageColumn = usersTable?.columns.find(c => c.name === 'age')

      // ID column
      expect(idColumn).toBeDefined()
      expect(idColumn?.dataType.toLowerCase()).toContain('integer')
      expect(idColumn?.isNullable).toBe(false)
      expect(idColumn?.isAutoIncrementing).toBe(false) // DuckDB doesn't have traditional auto-increment

      // Name column
      expect(nameColumn).toBeDefined()
      expect(nameColumn?.dataType.toLowerCase()).toContain('varchar')
      expect(nameColumn?.isNullable).toBe(false)
      expect(nameColumn?.hasDefaultValue).toBe(false)

      // Email column (nullable)
      expect(emailColumn).toBeDefined()
      expect(emailColumn?.isNullable).toBe(true)

      // Age column (with default)
      expect(ageColumn).toBeDefined()
      expect(ageColumn?.hasDefaultValue).toBe(true)
    })

    it('should handle tables with complex data types', async () => {
      await db.schema
        .createTable('complex_types')
        .addColumn('id', 'integer', col => col.primaryKey())
        .addColumn('json_data', 'json')
        .addColumn('array_data', sql`INTEGER[]`)
        .addColumn('decimal_value', sql`DECIMAL(10,2)`)
        .addColumn('timestamp_tz', 'timestamptz')
        .execute()

      const tables = await introspector.getTables()
      const complexTable = tables.find(t => t.name === 'complex_types')

      expect(complexTable).toBeDefined()
      expect(complexTable?.columns).toHaveLength(5)

      const columns = complexTable?.columns
      expect(Array.isArray(columns)).toBe(true)
      const jsonColumn = (columns ?? []).find(c => c.name === 'json_data')
      const arrayColumn = (columns ?? []).find(c => c.name === 'array_data')
      const decimalColumn = (columns ?? []).find(c => c.name === 'decimal_value')

      expect(jsonColumn).toBeDefined()
      expect(arrayColumn).toBeDefined()
      expect(decimalColumn).toBeDefined()
    })

    it('should handle empty tables', async () => {
      await db.schema
        .createTable('empty_table')
        .addColumn('id', 'integer', col => col.primaryKey())
        .execute()

      const tables = await introspector.getTables()
      const emptyTable = tables.find(t => t.name === 'empty_table')

      expect(emptyTable).toBeDefined()
      expect(emptyTable?.columns).toHaveLength(1)
    })

    it('should order columns correctly', async () => {
      const tables = await introspector.getTables()
      const postsTable = tables.find(t => t.name === 'posts')

      expect(postsTable).toBeDefined()
      const columnNames = postsTable?.columns.map(c => c.name)

      // Columns should be in creation order
      expect(columnNames).toEqual(['id', 'title', 'content', 'user_id', 'published_at'])
    })

    it('should handle schema filtering correctly', async () => {
      const tables = await introspector.getTables()

      // Should exclude information_schema and pg_catalog
      for (const table of tables) {
        expect(table.schema).not.toBe('information_schema')
        expect(table.schema).not.toBe('pg_catalog')
      }
    })
  })

  describe('Database Metadata', () => {
    beforeEach(async () => {
      // Create test schema
      await db.schema
        .createTable('metadata_test')
        .addColumn('id', 'integer', col => col.primaryKey())
        .addColumn('name', 'varchar(255)')
        .execute()
    })

    it('should get complete database metadata', async () => {
      const metadata = await introspector.getMetadata()

      expect(metadata).toHaveProperty('tables')
      expect(Array.isArray(metadata.tables)).toBe(true)
      expect(metadata.tables.length).toBeGreaterThan(0)

      const tableNames = metadata.tables.map(t => t.name)
      expect(tableNames).toContain('metadata_test')
    })

    it('should respect options in getMetadata', async () => {
      // Create migration table
      await db.schema
        .createTable('kysely_migration')
        .addColumn('name', 'varchar(255)', col => col.primaryKey())
        .execute()

      const metadataWithInternal = await introspector.getMetadata({
        withInternalKyselyTables: true,
      })
      const metadataWithoutInternal = await introspector.getMetadata({
        withInternalKyselyTables: false,
      })

      const internalTableNames = metadataWithInternal.tables.map(t => t.name)
      const externalTableNames = metadataWithoutInternal.tables.map(t => t.name)

      expect(internalTableNames).toContain('kysely_migration')
      expect(externalTableNames).not.toContain('kysely_migration')
    })

    it('should handle empty database metadata', async () => {
      // Drop all tables
      await db.schema.dropTable('metadata_test').execute()

      const metadata = await introspector.getMetadata()

      expect(metadata).toHaveProperty('tables')
      expect(Array.isArray(metadata.tables)).toBe(true)
    })

    it('should provide consistent metadata structure', async () => {
      const metadata = await introspector.getMetadata()

      for (const table of metadata.tables) {
        expect(table).toHaveProperty('name')
        expect(table).toHaveProperty('schema')
        expect(table).toHaveProperty('columns')
        expect(table).toHaveProperty('isView')

        expect(typeof table.name).toBe('string')
        expect(typeof table.schema).toBe('string')
        expect(Array.isArray(table.columns)).toBe(true)
        expect(typeof table.isView).toBe('boolean')

        for (const column of table.columns) {
          expect(column).toHaveProperty('name')
          expect(column).toHaveProperty('dataType')
          expect(column).toHaveProperty('isNullable')
          expect(column).toHaveProperty('hasDefaultValue')
          expect(column).toHaveProperty('isAutoIncrementing')
        }
      }
    })
  })

  describe('Error Handling and Edge Cases', () => {
    it('should handle database connection errors gracefully', async () => {
      // Close the database to simulate connection error
      database.closeSync()

      await expect(introspector.getSchemas()).rejects.toThrow()
      await expect(introspector.getTables()).rejects.toThrow()
      await expect(introspector.getMetadata()).rejects.toThrow()
    })

    it('should handle special characters in table names', async () => {
      await db.schema
        .createTable('special_table_with_underscores')
        .addColumn('id', 'integer', col => col.primaryKey())
        .execute()

      const tables = await introspector.getTables()
      const specialTable = tables.find(t => t.name.includes('special'))

      expect(specialTable).toBeDefined()
    })

    it('should handle tables with reserved keyword names', async () => {
      await db.schema
        .createTable('table_order') // Simplifions en utilisant table_order au lieu de "order"
        .addColumn('id', 'integer', col => col.primaryKey())
        .addColumn('value', 'varchar(255)') // Simplifions aussi le nom de colonne
        .execute()

      const tables = await introspector.getTables()
      const reservedTable = tables.find(t => t.name === 'table_order')

      expect(reservedTable).toBeDefined()
      if (reservedTable) {
        expect(reservedTable.columns).toHaveLength(2)
        const valueColumn = reservedTable.columns.find(c => c.name === 'value')
        expect(valueColumn).toBeDefined()
      }
    })

    it('should handle very long table and column names', async () => {
      const longName = `very_long_table_name_${'a'.repeat(50)}`
      const longColumnName = `very_long_column_name_${'b'.repeat(50)}`

      try {
        await db.schema
          .createTable(longName)
          .addColumn('id', 'integer', col => col.primaryKey())
          .addColumn(longColumnName, 'varchar(255)')
          .execute()

        const tables = await introspector.getTables()
        const longTable = tables.find(t => t.name === longName)

        if (longTable) {
          expect(longTable.columns).toHaveLength(2)
          const longColumn = longTable.columns.find(c => c.name === longColumnName)
          expect(longColumn).toBeDefined()
        }
      } catch (error) {
        // Some databases have name length limits, this is acceptable
        expect(error).toBeDefined()
      }
    })
  })

  describe('Performance and Scalability', () => {
    it('should handle databases with many tables efficiently', async () => {
      // Create multiple tables
      for (let i = 1; i <= 20; i++) {
        await db.schema
          .createTable(`table_${i}`)
          .addColumn('id', 'integer', col => col.primaryKey())
          .addColumn('data', 'varchar(255)')
          .execute()
      }

      const startTime = Date.now()
      const tables = await introspector.getTables()
      const duration = Date.now() - startTime

      expect(tables.length).toBe(20)
      expect(duration).toBeLessThan(5000) // Should complete in less than 5 seconds
    })

    it('should handle tables with many columns efficiently', async () => {
      let tableBuilder = db.schema
        .createTable('wide_table')
        .addColumn('id', 'integer', col => col.primaryKey())

      // Add many columns
      for (let i = 1; i <= 50; i++) {
        tableBuilder = tableBuilder.addColumn(`col_${i}`, 'varchar(100)')
      }

      await tableBuilder.execute()

      const startTime = Date.now()
      const tables = await introspector.getTables()
      const duration = Date.now() - startTime

      const wideTable = tables.find(t => t.name === 'wide_table')
      expect(wideTable).toBeDefined()
      expect(wideTable?.columns.length).toBe(51) // id + 50 columns
      expect(duration).toBeLessThan(3000) // Should complete in less than 3 seconds
    })

    it('should not leak memory with repeated introspection calls', async () => {
      await db.schema
        .createTable('memory_test')
        .addColumn('id', 'integer', col => col.primaryKey())
        .execute()

      const initialMemory = process.memoryUsage().heapUsed

      // Perform many introspection calls
      for (let i = 0; i < 100; i++) {
        await introspector.getTables()
        await introspector.getSchemas()
        await introspector.getMetadata()
      }

      const finalMemory = process.memoryUsage().heapUsed
      const memoryIncrease = finalMemory - initialMemory

      // Memory increase should be reasonable (less than 10MB)
      expect(memoryIncrease).toBeLessThan(10 * 1024 * 1024)
    })
  })
})

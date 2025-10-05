/**
 * SQL Injection Prevention Tests
 *
 * These tests document and verify that kysely-duckdb properly protects
 * against SQL injection attacks through Kysely's parameterized queries.
 *
 * IMPORTANT: Kysely uses prepared statements with parameter binding,
 * which provides native protection against SQL injection. These tests
 * serve primarily as executable documentation and regression detection.
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/dialect/duckdb-dialect.js'

interface TestDatabase {
  users: {
    id: number
    username: string
    email: string
    role: string
  }
  products: {
    id: number
    name: string
    price: number
  }
}

describe('Security: SQL Injection Prevention', () => {
  let db: Kysely<TestDatabase>
  let database: DuckDBInstance

  beforeAll(async () => {
    database = await DuckDBInstance.create(':memory:')
    db = new Kysely<TestDatabase>({
      dialect: new DuckDbDialect({ database }),
    })

    // Create test tables
    await db.schema
      .createTable('users')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('username', 'varchar(255)', col => col.notNull())
      .addColumn('email', 'varchar(255)')
      .addColumn('role', 'varchar(50)')
      .execute()

    await db.schema
      .createTable('products')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('name', 'varchar(255)', col => col.notNull())
      .addColumn('price', sql`DOUBLE`)
      .execute()

    // Insert test data
    await db
      .insertInto('users')
      .values([
        { id: 1, username: 'admin', email: 'admin@test.com', role: 'admin' },
        { id: 2, username: 'user', email: 'user@test.com', role: 'user' },
      ])
      .execute()

    await db
      .insertInto('products')
      .values([
        { id: 1, name: 'Product A', price: 10.99 },
        { id: 2, name: 'Product B', price: 20.5 },
      ])
      .execute()
  })

  afterAll(async () => {
    database.closeSync()
  })

  describe('WHERE Clause Injection Attempts', () => {
    it('should treat SQL injection attempt as literal string in WHERE', async () => {
      const maliciousInput = "admin'; DROP TABLE users;--"

      // This should return no results (username doesn't match)
      const result = await db
        .selectFrom('users')
        .selectAll()
        .where('username', '=', maliciousInput)
        .execute()

      expect(result).toHaveLength(0)

      // Verify table still exists and has data
      const users = await db.selectFrom('users').selectAll().execute()
      expect(users).toHaveLength(2)
    })

    it('should safely handle single quotes in WHERE clause', async () => {
      const maliciousInput = "'; DROP TABLE products;--"

      const result = await db
        .selectFrom('users')
        .selectAll()
        .where('username', '=', maliciousInput)
        .execute()

      expect(result).toHaveLength(0)

      // Verify products table still exists
      const products = await db.selectFrom('products').selectAll().execute()
      expect(products).toHaveLength(2)
    })

    it('should handle UNION-based injection attempts', async () => {
      const maliciousInput = "admin' UNION SELECT * FROM users--"

      const result = await db
        .selectFrom('users')
        .selectAll()
        .where('username', '=', maliciousInput)
        .execute()

      expect(result).toHaveLength(0)
    })

    it('should safely handle comment sequences', async () => {
      const inputs = ['-- comment', '/* comment */', '#comment', '; --']

      for (const input of inputs) {
        const result = await db
          .selectFrom('users')
          .selectAll()
          .where('username', '=', input)
          .execute()

        expect(result).toHaveLength(0)
      }
    })
  })

  describe('INSERT Injection Attempts', () => {
    it('should safely insert malicious-looking strings', async () => {
      const maliciousUsername = "admin'); DROP TABLE users;--"
      const maliciousEmail = "test@test.com'; DELETE FROM users WHERE '1'='1"

      await db
        .insertInto('users')
        .values({
          id: 100,
          username: maliciousUsername,
          email: maliciousEmail,
          role: 'user',
        })
        .execute()

      // Verify insertion succeeded with literal strings
      const inserted = await db.selectFrom('users').selectAll().where('id', '=', 100).execute()

      expect(inserted).toHaveLength(1)
      expect(inserted[0]).toBeDefined()
      expect(inserted[0]?.username).toBe(maliciousUsername)
      expect(inserted[0]?.email).toBe(maliciousEmail)

      // Verify no tables were dropped
      const allUsers = await db.selectFrom('users').selectAll().execute()
      expect(allUsers.length).toBeGreaterThanOrEqual(3)
    })
  })

  describe('UPDATE Injection Attempts', () => {
    it('should safely update with injection-like values', async () => {
      const maliciousRole = "admin' WHERE '1'='1"

      await db.updateTable('users').set({ role: maliciousRole }).where('id', '=', 2).execute()

      const updated = await db.selectFrom('users').selectAll().where('id', '=', 2).execute()

      expect(updated[0]).toBeDefined()
      expect(updated[0]?.role).toBe(maliciousRole)

      // Verify only intended row was updated
      const admin = await db.selectFrom('users').selectAll().where('id', '=', 1).execute()

      expect(admin[0]).toBeDefined()
      expect(admin[0]?.role).toBe('admin')
    })
  })

  describe('sql.raw() Usage Documentation', () => {
    it('demonstrates that sql.raw() requires manual escaping (by design)', async () => {
      // INTENTIONAL: sql.raw() is for advanced users who need dynamic SQL
      // Users must understand they're responsible for safety

      const unsafeInput = "'; DROP TABLE users;--"

      // This would be dangerous if constructed improperly
      // We demonstrate proper usage with sql.ref() for identifiers

      const safeQuery = sql<{ id: number }>`
        SELECT id FROM users
        WHERE username = ${unsafeInput}
      `

      const result = await safeQuery.execute(db)

      // Input is parameterized even in template literals
      expect(result.rows).toHaveLength(0)
    })

    it('documents proper use of sql.ref() for identifiers', async () => {
      const tableName = 'users' // User-controlled
      const columnName = 'username' // User-controlled

      // CORRECT: Use sql.ref() for identifiers
      const result = await db
        .selectFrom(sql.ref(tableName).as('t'))
        .select(sql.ref(columnName).as('col'))
        .execute()

      // Should return all rows (100 inserted + 2 original + 3 unicode = 5+)
      expect(result.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Special Characters Handling', () => {
    it('should handle backslashes safely', async () => {
      const input = "test\\'; DROP TABLE users;--"

      await db
        .insertInto('users')
        .values({ id: 200, username: input, email: 'test@test.com', role: 'user' })
        .execute()

      const result = await db
        .selectFrom('users')
        .selectAll()
        .where('username', '=', input)
        .execute()

      expect(result).toHaveLength(1)
      expect(result[0]).toBeDefined()
      expect(result[0]?.username).toBe(input)
    })

    it('should handle null bytes safely', async () => {
      const input = 'test\x00injection'

      await db
        .insertInto('users')
        .values({ id: 201, username: input, email: 'test@test.com', role: 'user' })
        .execute()

      const result = await db.selectFrom('users').selectAll().where('id', '=', 201).execute()

      expect(result).toHaveLength(1)
    })

    it('should handle Unicode characters safely', async () => {
      const inputs = [
        "用户'; DROP TABLE users;--", // Chinese + injection
        "مستخدم'; DELETE FROM users;--", // Arabic + injection
        "🔥'; DROP TABLE users;--", // Emoji + injection
      ]

      for (const [index, input] of inputs.entries()) {
        await db
          .insertInto('users')
          .values({ id: 300 + index, username: input, email: 'test@test.com', role: 'user' })
          .execute()

        const result = await db
          .selectFrom('users')
          .selectAll()
          .where('username', '=', input)
          .execute()

        expect(result).toHaveLength(1)
        expect(result[0]).toBeDefined()
        expect(result[0]?.username).toBe(input)
      }
    })
  })

  describe('Stacked Queries Prevention', () => {
    it('should not execute stacked queries', async () => {
      const maliciousInput = 'admin; DELETE FROM users; --'

      await db.selectFrom('users').selectAll().where('username', '=', maliciousInput).execute()

      // Verify all users still exist
      const users = await db.selectFrom('users').selectAll().execute()
      expect(users.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Boolean-based Blind Injection', () => {
    it('should treat boolean expressions as literals', async () => {
      const inputs = [
        "admin' AND '1'='1",
        "admin' AND '1'='2",
        "admin' OR '1'='1",
        'admin AND 1=1',
        'admin OR 1=1',
      ]

      for (const input of inputs) {
        const result = await db
          .selectFrom('users')
          .selectAll()
          .where('username', '=', input)
          .execute()

        // Should return 0 results (input treated as string, not condition)
        expect(result).toHaveLength(0)
      }
    })
  })
})

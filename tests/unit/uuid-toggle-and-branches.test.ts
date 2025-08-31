import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { __test } from '../../src/dialect/duckdb-driver.js'
import { DuckDbDialect } from '../../src/index.js'

describe('UUID handling toggle and conversion branches', () => {
  let database: DuckDBInstance

  beforeAll(async () => {
    database = await DuckDBInstance.create(':memory:')
  })

  afterAll(async () => {
    database.closeSync()
  })

  describe('uuidAsString=false returns native values', () => {
    let db: Kysely<any>
    let localDatabase: DuckDBInstance

    beforeAll(async () => {
      localDatabase = await DuckDBInstance.create(':memory:')
      db = new Kysely<any>({
        dialect: new DuckDbDialect({ database: localDatabase, uuidAsString: false }),
      })
      await sql`CREATE TABLE uuid_toggle_test (id UUID PRIMARY KEY)`.execute(db)
      await sql`INSERT INTO uuid_toggle_test (id) VALUES (CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID))`.execute(
        db,
      )
    })

    afterAll(async () => {
      await db.destroy()
      // db.destroy() closes the underlying DuckDB instance via the driver
    })

    it('keeps UUID column as object when uuidAsString=false', async () => {
      const rows = await db.selectFrom('uuid_toggle_test').selectAll().execute()
      expect(rows).toHaveLength(1)
      // Should not be string when toggle is false
      expect(typeof rows?.[0]?.['id']).toBe('object')
    })
  })

  describe('convertUuidToString branches', () => {
    it('handles real DuckDBUUIDValue instance via instanceof path when available', async () => {
      const conn = await database.connect()
      try {
        const pending = await conn.startStream(
          "SELECT CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID) AS id",
        )
        // Wait until ready (0 is RESULT_READY)
        while (pending.runTask() !== 0) {}
        const reader = await pending.read()
        await reader.readUntil(1)
        const rows = reader.getRowObjects()
        const value = rows?.[0]?.['id']
        // Ensure we got an object from DuckDB
        expect(typeof value).toBe('object')
        // The conversion should return the canonical string
        const s = __test.convertUuidToString(value)
        expect(s).toBe('123e4567-e89b-12d3-a456-426614174000')
      } finally {
        conn.closeSync()
      }
    })

    it('gracefully handles hugeint getter throwing (catch path in tryHugeint)', () => {
      const obj: any = {}
      Object.defineProperty(obj, 'hugeint', {
        get() {
          throw new Error('boom')
        },
      })
      const s = __test.convertUuidToString(obj)
      // Falls back to String(obj)
      expect(typeof s).toBe('string')
    })
  })
})

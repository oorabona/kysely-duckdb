import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDialect } from '../../src/index.js'

describe('DuckDB Temporal Types Integration', () => {
  let database: DuckDBInstance
  let db: Kysely<any>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    db = new Kysely<any>({
      dialect: new DuckDbDialect({ database }),
    })
    await sql.raw('PRAGMA old_implicit_casting=true').execute(db)
  })

  afterEach(async () => {
    await db.destroy()
  })

  describe('Extended Temporal Types', () => {
    it('should handle Date objects for TIME WITH TIME ZONE columns', async () => {
      await db.schema
        .createTable('temporal_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('time_tz', sql`TIME WITH TIME ZONE`)
        .addColumn('timestamp_tz', sql`TIMESTAMP WITH TIME ZONE`)
        .execute()

      const now = new Date()

      await db
        .insertInto('temporal_test')
        .values({
          time_tz: now,
          timestamp_tz: now,
        })
        .execute()

      const results = await db.selectFrom('temporal_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['time_tz']).toBeDefined()
      expect(r0?.['timestamp_tz']).toBeDefined()
    })

    it('should handle string temporal formats', async () => {
      await db.schema
        .createTable('temporal_string_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('time_tz', sql`TIME WITH TIME ZONE`)
        .addColumn('timestamp_tz', sql`TIMESTAMP WITH TIME ZONE`)
        .execute()

      await db
        .insertInto('temporal_string_test')
        .values({
          time_tz: '14:30:00+01:00',
          timestamp_tz: '2025-08-27 14:30:00+01:00',
        })
        .execute()

      const results = await db.selectFrom('temporal_string_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['time_tz']).toBeDefined()
      expect(r0?.['timestamp_tz']).toBeDefined()
    })

    it('should handle various timezone formats', async () => {
      await db.schema
        .createTable('timezone_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('timestamp_utc', sql`TIMESTAMP WITH TIME ZONE`)
        .addColumn('timestamp_est', sql`TIMESTAMP WITH TIME ZONE`)
        .execute()

      await db
        .insertInto('timezone_test')
        .values([
          {
            timestamp_utc: '2025-08-27 14:30:00+00:00',
            timestamp_est: '2025-08-27 09:30:00-05:00',
          },
          {
            timestamp_utc: new Date('2025-08-27T14:30:00Z'),
            timestamp_est: new Date('2025-08-27T09:30:00-05:00'),
          },
        ])
        .execute()

      const results = await db.selectFrom('timezone_test').selectAll().execute()
      expect(results).toHaveLength(2)
    })
  })

  describe('Standard Date/Time Types', () => {
    it('should handle basic Date objects', async () => {
      await db.schema
        .createTable('basic_temporal_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('date_col', 'date')
        .addColumn('time_col', 'time')
        .addColumn('timestamp_col', 'timestamp')
        .execute()

      const testDate = new Date('2025-08-27T14:30:00Z')

      await db
        .insertInto('basic_temporal_test')
        .values({
          date_col: testDate,
          time_col: testDate,
          timestamp_col: testDate,
        })
        .execute()

      const results = await db.selectFrom('basic_temporal_test').selectAll().execute()
      expect(results).toHaveLength(1)
      const r0 = results[0]
      expect(r0?.['date_col']).toBeDefined()
      expect(r0?.['time_col']).toBeDefined()
      expect(r0?.['timestamp_col']).toBeDefined()
    })

    it('should handle edge case dates', async () => {
      await db.schema
        .createTable('edge_dates_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('early_date', 'timestamp')
        .addColumn('far_future', 'timestamp')
        .execute()

      await db
        .insertInto('edge_dates_test')
        .values([
          {
            early_date: new Date('1900-01-01T00:00:00Z'),
            far_future: new Date('2100-12-31T23:59:59Z'),
          },
          {
            early_date: new Date(0), // Unix epoch
            far_future: new Date('2038-01-19T03:14:07Z'), // Near Y2038
          },
        ])
        .execute()

      const results = await db.selectFrom('edge_dates_test').selectAll().execute()
      expect(results).toHaveLength(2)
    })
  })

  describe('Date Arithmetic and Functions', () => {
    it('should support date queries with temporal functions', async () => {
      await db.schema
        .createTable('date_functions_test')
        .addColumn('id', 'uuid', col => col.primaryKey().defaultTo(sql`uuid()`))
        .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`now()`))
        .addColumn('event_date', 'date')
        .execute()

      const testDate = new Date('2025-08-27')

      await db
        .insertInto('date_functions_test')
        .values([
          { event_date: testDate },
          { event_date: new Date('2025-08-28') },
          { event_date: new Date('2025-08-26') },
        ])
        .execute()

      // Test date range queries
      const recentResults = await db
        .selectFrom('date_functions_test')
        .selectAll()
        .where('event_date', '>=', testDate)
        .execute()

      expect(recentResults.length).toBeGreaterThanOrEqual(1)

      // Test count
      const countResult = await db
        .selectFrom('date_functions_test')
        .select(sql`count(*)`.as('total'))
        .executeTakeFirst()

      expect(Number(countResult?.total)).toBe(3)
    })
  })
})

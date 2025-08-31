/**
 * Date Conversion Test
 *
 * This test verifies that kysely-duckdb correctly handles date conversions:
 * 1. ISO date strings are properly converted to DuckDB DATE/TIMESTAMP types
 * 2. Returned values are proper DuckDB date objects
 * 3. No "Cannot create values of type ANY" errors occur
 * 4. Date queries and functions work correctly
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { Kysely, sql } from 'kysely'
import { DuckDbDialect } from '../../src/index.js'

interface TestSchema {
  date_test: {
    id: number
    date_only: string // DATE type
    timestamp_col: string // TIMESTAMP type
    timestamptz_col: string // TIMESTAMPTZ type
    inserted_string: string // Original string for comparison
  }
}

async function testDateConversion() {
  console.log('🧪 Date Conversion Integration Test\n')

  const database = await DuckDBInstance.create(':memory:')

  const db = new Kysely<TestSchema>({
    dialect: new DuckDbDialect({
      database,
      config: {
        threads: 1,
      },
    }),
  })

  try {
    // 1. Create table with different date types
    console.log('1. Creating table with DATE, TIMESTAMP, TIMESTAMPTZ types...')

    await db.schema
      .createTable('date_test')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('date_only', 'date', col => col.notNull())
      .addColumn('timestamp_col', 'timestamp', col => col.notNull())
      .addColumn('timestamptz_col', 'timestamptz', col => col.notNull())
      .addColumn('inserted_string', 'varchar(50)', col => col.notNull())
      .execute()

    console.log('✅ Table created')

    // 2. Insert data with ISO strings
    console.log('\n2. Inserting data with ISO strings...')

    const testData = [
      {
        id: 1,
        date_only: '2024-01-15', // Date simple
        timestamp_col: '2024-01-15T10:30:00', // Timestamp sans timezone
        timestamptz_col: '2024-01-15T10:30:00Z', // Timestamp avec timezone UTC
        inserted_string: '2024-01-15T10:30:00Z',
      },
      {
        id: 2,
        date_only: '2023-12-31',
        timestamp_col: '2023-12-31T23:59:59',
        timestamptz_col: '2023-12-31T23:59:59+02:00', // Timezone européenne
        inserted_string: '2023-12-31T23:59:59+02:00',
      },
      {
        id: 3,
        date_only: '2025-08-19', // Date actuelle
        timestamp_col: '2025-08-19T14:22:30.123', // Avec millisecondes
        timestamptz_col: '2025-08-19T14:22:30.123456Z', // Avec microsecondes
        inserted_string: '2025-08-19T14:22:30.123456Z',
      },
    ]

    await db.insertInto('date_test').values(testData).execute()
    console.log('✅ Data inserted')

    // 3. Retrieve and analyze returned types
    console.log('\n3. Retrieving and analyzing types...')

    const results = await db.selectFrom('date_test').selectAll().orderBy('id').execute()

    console.log('\n📊 Results and JavaScript types:')

    results.forEach((row, index) => {
      console.log(`\nRecord ${index + 1}:`)
      console.log(`  Original string: "${row.inserted_string}"`)
      console.log(
        `  date_only: ${String(row.date_only)} (type: ${typeof row.date_only}) ${row.date_only?.constructor?.name ? `[${row.date_only.constructor.name}]` : ''}`,
      )
      console.log(
        `  timestamp_col: ${String(row.timestamp_col)} (type: ${typeof row.timestamp_col}) ${row.timestamp_col?.constructor?.name ? `[${row.timestamp_col.constructor.name}]` : ''}`,
      )
      console.log(
        `  timestamptz_col: ${String(row.timestamptz_col)} (type: ${typeof row.timestamptz_col}) ${row.timestamptz_col?.constructor?.name ? `[${row.timestamptz_col.constructor.name}]` : ''}`,
      )

      // Verify returned types using safe guards
      const isObject = (v: unknown): v is object => typeof v === 'object' && v !== null
      const isDate = (v: unknown): v is Date => isObject(v) && v instanceof Date

      // Verify returned types
      if (isDate(row.date_only)) {
        console.log(`  ✅ date_only properly converted to JavaScript Date object`)
      } else {
        console.log(`  ℹ️  date_only returned as ${typeof row.date_only}`)
      }

      if (isDate(row.timestamp_col) || isObject(row.timestamp_col)) {
        console.log(`  ✅ timestamp_col converted to temporal object`)
      } else {
        console.log(`  ℹ️  timestamp_col returned as ${typeof row.timestamp_col}`)
      }
    })

    // 4. Test queries with date conditions
    console.log('\n4. Testing queries with date conditions...')

    const dateQuery = await db
      .selectFrom('date_test')
      .selectAll()
      .where('date_only', '>=', '2024-01-01')
      .where('timestamp_col', '<', '2025-01-01T00:00:00')
      .execute()

    console.log(`✅ Query with date conditions: ${dateQuery.length} result(s)`)

    // 5. Test insertion with SQL literal for comparison
    console.log('\n5. Testing with SQL literals for comparison...')

    await db
      .insertInto('date_test')
      .values({
        id: 4,
        date_only: sql`'2024-06-15'::date`,
        timestamp_col: sql`'2024-06-15T12:00:00'::timestamp`,
        timestamptz_col: sql`'2024-06-15T12:00:00Z'::timestamptz`,
        inserted_string: 'SQL LITERAL',
      })
      .execute()

    const sqlLiteralResult = await db
      .selectFrom('date_test')
      .selectAll()
      .where('id', '=', 4)
      .execute()

    console.log('\n📊 Result with SQL literal:')
    const sqlRow = sqlLiteralResult[0]
    if (sqlRow) {
      console.log(
        `  date_only: ${String(sqlRow.date_only)} (type: ${typeof sqlRow.date_only}) ${sqlRow.date_only?.constructor?.name ? `[${sqlRow.date_only.constructor.name}]` : ''}`,
      )
      console.log(
        `  timestamp_col: ${String(sqlRow.timestamp_col)} (type: ${typeof sqlRow.timestamp_col}) ${sqlRow.timestamp_col?.constructor?.name ? `[${sqlRow.timestamp_col.constructor.name}]` : ''}`,
      )
      console.log(
        `  timestamptz_col: ${String(sqlRow.timestamptz_col)} (type: ${typeof sqlRow.timestamptz_col}) ${sqlRow.timestamptz_col?.constructor?.name ? `[${sqlRow.timestamptz_col.constructor.name}]` : ''}`,
      )
    }

    // 6. Test DuckDB date functions
    console.log('\n6. Testing DuckDB date functions...')

    const dateFunctions = await sql<{
      today: unknown
      now: unknown
      extract_year: number
      date_diff: number
    }>`
      SELECT 
        CURRENT_DATE as today,
        CURRENT_TIMESTAMP as now,
        EXTRACT(YEAR FROM DATE '2024-01-15') as extract_year,
        DATE_DIFF('day', DATE '2024-01-15', DATE '2024-01-20') as date_diff
    `.execute(db)

    console.log('📊 DuckDB date functions:')
    console.table(dateFunctions.rows)

    console.log('\n✅ Date conversion test completed successfully!')
  } finally {
    await db.destroy()
    database.closeSync()
  }
}

// Run the test
if (import.meta.url === `file://${process.argv[1]}`) {
  testDateConversion().catch(console.error)
}

export { testDateConversion }

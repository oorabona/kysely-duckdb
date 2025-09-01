/**
 * SERIAL-like Auto-Increment Example (DuckDB)
 *
 * DuckDB n'a pas le mot-clé SERIAL/IDENTITY façon Postgres.
 * Utilisez une SEQUENCE + DEFAULT nextval('seq') pour l'auto-incrément.
 */

import { Kysely, sql, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

interface DB {
  users: {
    // Allow undefined on insert so DEFAULT nextval('...') can populate it
    id: ColumnType<number, number | undefined, number>
    name: string
    email: string
    created_at: ColumnType<Date, Date | undefined, Date>
  }
}

async function main() {
  console.log('🔢 SERIAL-like Auto-Increment Demo (DuckDB)\n')

  const database = await DuckDBInstance.create(':memory:')
  const db = new Kysely<DB>({ dialect: new DuckDbDialect({ database }) })

  try {
    // 1) Create a sequence (starts at 1)
    await sql`CREATE SEQUENCE IF NOT EXISTS users_id_seq START 1`.execute(db)

    // 2) Create a table using DEFAULT nextval('users_id_seq')
    await db.schema
      .createTable('users')
      .addColumn('id', 'integer', (c) => c.primaryKey().defaultTo(sql`nextval('users_id_seq')`))
      .addColumn('name', 'varchar(100)', (c) => c.notNull())
      .addColumn('email', 'varchar(255)', (c) => c.unique())
      .addColumn('created_at', 'timestamp', (c) => c.defaultTo(sql`CURRENT_TIMESTAMP`))
      .execute()

    // 3) Insert rows without specifying id
    await db
      .insertInto('users')
      .values([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' },
        { name: 'Carol', email: 'carol@example.com' },
      ])
      .execute()

    // 4) Verify auto-incremented ids
    const rows = await db.selectFrom('users').selectAll().orderBy('id').execute()
    console.table(rows)

    // 5) Demonstrate next values are continuous
    const more = await db
      .insertInto('users')
      .values({ name: 'David', email: 'david@example.com' })
      .returning(['id'])
      .executeTakeFirstOrThrow()
    console.log(`Next id was: ${more.id}`)

    console.log('\n✅ SERIAL-like example completed successfully!')
  } finally {
    await db.destroy()
    database.closeSync()
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // eslint-disable-next-line unicorn/prefer-top-level-await
  main().catch(console.error)
}

export { main }

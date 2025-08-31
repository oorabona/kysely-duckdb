/**
 * Native Logging Example
 * 
 * This example demonstrates how to use Kysely's built-in logging system
 * with DuckDB. This is the recommended approach for logging queries
 * instead of using a custom LoggerPlugin.
 */

import { Kysely, sql, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

// Define simple database schema
interface DatabaseSchema {
  users: {
    id: number
    name: string
    email: string
    created_at: ColumnType<Date, Date | undefined, Date>
  }
}

/**
 * Custom logger implementation using Kysely's Log interface
 */
function createCustomLogger() {
  return (event: any) => {
    const { level, query, queryDurationMillis } = event
    
    // Format the query nicely
    const formattedQuery = query.sql
      .replace(/\s+/g, ' ')
      .trim()
    
    // Create performance indicators
    const performanceIcon = queryDurationMillis > 1000 ? '⚠️' : '✅'
    const duration = queryDurationMillis ? `${queryDurationMillis}ms` : 'unknown'
    
    // Identify query type
    const queryType = formattedQuery.match(/^\w+/)?.[0]?.toUpperCase() || 'UNKNOWN'
    
    // Log with colors and formatting
    console.log(`${performanceIcon} [${queryType}] ${formattedQuery}`)
    console.log(`   📊 Duration: ${duration}`)
    
    if (query.parameters?.length > 0) {
      console.log(`   📎 Parameters:`, query.parameters)
    }
    
    console.log('') // Empty line for readability
  }
}

async function main() {
  console.log('🎯 Native Logging Demo\n')
  
  // Create DuckDB database instance
  const database = await DuckDBInstance.create(':memory:')

  // Create Kysely instance with native logging
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({
      database,
    }),
    // Use Kysely's native logging system
    log: createCustomLogger()
  })

  try {
    console.log('1. Creating table with logging...')
    
    await db.schema
      .createTable('users')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('name', 'varchar(255)', col => col.notNull())
      .addColumn('email', 'varchar(255)', col => col.unique())
      .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
      .execute()

    console.log('2. Inserting data with logging...')
    
    await db
      .insertInto('users')
      .values([
        { id: 1, name: 'John Doe', email: 'john@example.com' },
        { id: 2, name: 'Jane Smith', email: 'jane@example.com' },
        { id: 3, name: 'Bob Wilson', email: 'bob@example.com' },
      ])
      .execute()

    console.log('3. Querying data with logging...')
    
    const users = await db
      .selectFrom('users')
      .select(['id', 'name', 'email'])
      .where('name', 'like', '%J%')
      .orderBy('name')
      .execute()

    console.log('📋 Query results:')
    console.table(users)

    console.log('4. Complex query with parameters...')
    
    const userCount = await db
      .selectFrom('users')
      .select(eb => eb.fn.count('id').as('total'))
      .where('email', '!=', 'admin@example.com') // This will show parameter binding
      .executeTakeFirstOrThrow()

    console.log(`👥 Total users: ${userCount.total}`)

    console.log('5. Transaction with logging...')
    
    await db.transaction().execute(async trx => {
      await trx
        .insertInto('users')
        .values({ id: 4, name: 'Alice Cooper', email: 'alice@example.com' })
        .execute()

      await trx
        .updateTable('users')
        .set({ name: 'Alice Cooper-Smith' })
        .where('id', '=', 4)
        .execute()
    })

    console.log('✅ Native logging demo completed successfully!')

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

// Alternative: Simple console logging
function createSimpleLogger() {
  return (event: any) => {
    console.log(`🔍 Query: ${event.query.sql}`)
    if (event.queryDurationMillis) {
      console.log(`⏱️  Duration: ${event.queryDurationMillis}ms`)
    }
  }
}

// Alternative: Production-ready logger with levels
function createProductionLogger() {
  return (event: any) => {
    const { level, query, queryDurationMillis } = event
    
    // Only log slow queries in production
    if (queryDurationMillis > 100) {
      console.warn(`⚠️ Slow query detected (${queryDurationMillis}ms):`, query.sql)
    }
    
    // Log all DDL operations
    if (query.sql.match(/^(CREATE|DROP|ALTER)/i)) {
      console.info(`🔧 Schema change:`, query.sql)
    }
  }
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { main, createCustomLogger, createSimpleLogger, createProductionLogger }
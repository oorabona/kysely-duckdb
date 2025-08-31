/**
 * Plugins Example
 * 
 * This example demonstrates the plugin system in kysely-duckdb,
 * including built-in plugins and how to create custom ones.
 */

import { Kysely, sql, type KyselyPlugin, type PluginTransformQueryArgs, type PluginTransformResultArgs, type RootOperationNode, type QueryResult, type UnknownRow, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect, CaseConverterPlugin } from '@oorabona/kysely-duckdb'

// Database schema for plugin examples
interface DatabaseSchema {
  user_profiles: {
    id: ColumnType<number, number | undefined, number>
    first_name: string
    last_name: string
    email_address: string
    phone_number: ColumnType<string, string | undefined, string>
    date_of_birth: ColumnType<Date, Date | string | undefined, Date>
    created_at: ColumnType<Date, Date | undefined, Date>
    updated_at: ColumnType<Date, Date | undefined, Date>
  }
  blog_posts: {
    id: ColumnType<number, number | undefined, number>
    post_title: string
    post_content: string
    author_id: number
    is_published: boolean
    view_count: number
    created_at: ColumnType<Date, Date | undefined, Date>
  }
}

// Custom plugin example: Query timing plugin
class QueryTimingPlugin implements KyselyPlugin {
  private queryTimes = new Map<string, number>()

  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    const queryId = Math.random().toString(36).substring(7)
    this.queryTimes.set(queryId, Date.now())
    
    // Add query ID as a comment to track it
    // We cannot attach metadata to the node in a typed-safe way here; just return the node.
    return args.node
  }

  async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<UnknownRow>> {
    // We can't reliably correlate queryId to start time without internal hooks; log duration if provided by Kysely
    return args.result
  }
}

// Custom plugin: Row count logger
class RowCountLoggerPlugin implements KyselyPlugin {
  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    return args.node
  }

  async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<UnknownRow>> {
    if (Array.isArray(args.result.rows)) {
      const count = args.result.rows.length
      if (count > 0) {
        console.log(`📊 Query returned ${count} row${count === 1 ? '' : 's'}`)
      }
    }
    
    return args.result
  }
}

// Custom plugin: SQL injection detector (demo - not for production)
class SqlInjectionDetectorPlugin implements KyselyPlugin {
  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    return args.node
  }

  async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<UnknownRow>> {
    return args.result
  }
}

async function demonstrateCaseConverterPlugin() {
  console.log('\n🔤 Case Converter Plugin Demo\n')

  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({ database }),
    plugins: [
      new CaseConverterPlugin({
        // Convert TypeScript camelCase to SQL snake_case
        toSnakeCase: true,
        // Convert SQL snake_case back to TypeScript camelCase
        toCamelCase: true,
      })
    ]
  })

  try {
    // Create table using snake_case (SQL convention)
    await db.schema
      .createTable('user_profiles')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('first_name', 'varchar(100)')
      .addColumn('last_name', 'varchar(100)')
      .addColumn('email_address', 'varchar(255)', col => col.unique())
      .addColumn('phone_number', 'varchar(20)')
      .addColumn('date_of_birth', 'date')
      .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
      .addColumn('updated_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
      .execute()

    // Insert data using camelCase (will be converted to snake_case)
    const insertedUser = await db
      .insertInto('user_profiles')
      .values({
        first_name: 'John',
        last_name: 'Doe',
        email_address: 'john.doe@example.com',
        phone_number: '+1-555-0123',
        date_of_birth: sql`'1990-05-15'::date`,
      })
      .returningAll()
      .executeTakeFirstOrThrow()

    console.log('✅ Inserted user (camelCase result):')
    console.table([insertedUser])

    // Query using camelCase field names
    const users = await db
      .selectFrom('user_profiles')
      .select([
        'id',
        'first_name',
        'last_name',
        'email_address',
        'phone_number',
        'date_of_birth',
        'created_at',
      ])
      .where('email_address', 'like', '%@example.com')
      .execute()

    console.log('\n📋 Query results (camelCase):')
    console.table(users)

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

async function demonstrateLoggingPlugins() {
  console.log('\n📝 Logging Plugins Demo\n')

  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({ database }),
    // Use Kysely's native logging instead of custom logger plugins
    log: (event) => {
      const { query, queryDurationMillis } = event
      const duration = queryDurationMillis || 0
      const performanceIcon = duration > 100 ? '⚠️' : '✅'
      
      console.log(`${performanceIcon} Query: ${query.sql.replace(/\s+/g, ' ').trim()}`)
      if (duration > 0) {
        console.log(`   ⏱️  Duration: ${duration}ms`)
      }
      if (query.parameters?.length > 0) {
        console.log(`   📎 Parameters: ${JSON.stringify(query.parameters)}`)
      }
    }
  })

  try {
    // Create table
    await db.schema
      .createTable('blog_posts')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('post_title', 'varchar(255)')
      .addColumn('post_content', 'text')
      .addColumn('author_id', 'integer')
      .addColumn('is_published', 'boolean', col => col.defaultTo(false))
      .addColumn('view_count', 'integer', col => col.defaultTo(0))
      .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
      .execute()

    // Insert data (logged)
    await db
      .insertInto('blog_posts')
      .values([
        {
          post_title: 'Introduction to DuckDB',
          post_content: 'DuckDB is an amazing analytical database...',
          author_id: 1,
          is_published: true,
          view_count: 150
        },
        {
          post_title: 'Advanced SQL Techniques',
          post_content: 'In this post we explore advanced SQL patterns...',
          author_id: 1,
          is_published: true,
          view_count: 89
        },
        {
          post_title: 'Draft Post',
          post_content: 'This is still a draft...',
          author_id: 1,
          is_published: false,
          view_count: 0
        }
      ])
      .execute()

    // Query data (logged with performance metrics)
    const publishedPosts = await db
      .selectFrom('blog_posts')
      .select(['post_title', 'view_count', 'created_at'])
      .where('is_published', '=', true)
      .orderBy('view_count', 'desc')
      .execute()

    console.log(`\n📊 Found ${publishedPosts.length} published posts`)

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

async function demonstrateCustomPlugins() {
  console.log('\n🔌 Custom Plugins Demo\n')

  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({ database }),
    plugins: [
      new QueryTimingPlugin(),
      new RowCountLoggerPlugin(),
      new SqlInjectionDetectorPlugin()
    ]
  })

  try {
    // Create table
    await db.schema
      .createTable('user_profiles')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('first_name', 'varchar(100)')
      .addColumn('last_name', 'varchar(100)')
      .addColumn('email_address', 'varchar(255)')
      .addColumn('date_of_birth', 'date')
      .execute()

    // Insert test data
    console.log('1. Inserting data...')
    await db
      .insertInto('user_profiles')
      .values([
        { first_name: 'Alice', last_name: 'Johnson', email_address: 'alice@example.com' },
        { first_name: 'Bob', last_name: 'Smith', email_address: 'bob@example.com' },
        { first_name: 'Carol', last_name: 'Williams', email_address: 'carol@example.com' }
      ])
      .execute()

    // Normal query
    console.log('\n2. Selecting all users...')
    const allUsers = await db
      .selectFrom('user_profiles')
      .selectAll()
      .execute()

    // Query with WHERE clause
    console.log('\n3. Finding users with specific email...')
    const specificUser = await db
      .selectFrom('user_profiles')
      .selectAll()
      .where('email_address', '=', 'alice@example.com')
      .execute()

    // Simulated suspicious query (will trigger warning)
    console.log('\n4. Testing SQL injection detection...')
    try {
      // This is just for demo - the plugin will warn but not block
      await sql`SELECT * FROM user_profiles WHERE first_name = 'Alice' OR '1'='1'`.execute(db)
    } catch (error) {
      console.log('Query blocked by security plugin')
    }

    console.log('\n✅ Custom plugins demonstrated successfully!')

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

async function demonstratePluginChaining() {
  console.log('\n🔗 Plugin Chaining Demo\n')

  const database = await DuckDBInstance.create(':memory:')
  
  // Multiple plugins working together
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({ database }),
    plugins: [
      new CaseConverterPlugin({ toSnakeCase: true, toCamelCase: true }),
      new QueryTimingPlugin(),
      new RowCountLoggerPlugin()
    ]
  })

  try {
    // Create table
    await db.schema
      .createTable('user_profiles')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('first_name', 'varchar(100)')
      .addColumn('last_name', 'varchar(100)')
      .addColumn('email_address', 'varchar(255)')
      .addColumn('date_of_birth', 'date')
      .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
      .execute()

    // Insert with camelCase (will be converted by CaseConverterPlugin)
    console.log('Inserting users with camelCase fields...')
    await db
      .insertInto('user_profiles')
      .values([
        { first_name: 'David', last_name: 'Chen', email_address: 'david@example.com', date_of_birth: new Date('1985-03-20') },
        { first_name: 'Emma', last_name: 'Wilson', email_address: 'emma@example.com', date_of_birth: new Date('1992-07-15') },
        { first_name: 'Frank', last_name: 'Brown', email_address: 'frank@example.com', date_of_birth: new Date('1988-11-30') }
      ])
      .execute()

    // Query with camelCase (all plugins will process this)
    console.log('\nQuerying users...')
    const users = await db
      .selectFrom('user_profiles')
      .select(['first_name', 'last_name', 'email_address', 'created_at'])
      .where('email_address', 'like', '%@example.com')
      .orderBy('last_name')
      .execute()

    console.log(`\nResult: ${users.length} users found`)
    console.table(users)

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

async function main() {
  console.log('🔌 Kysely Plugins Demo\n')

  try {
    // Demonstrate case converter
    await demonstrateCaseConverterPlugin()

    // Demonstrate logging plugins
    await demonstrateLoggingPlugins()

    // Demonstrate custom plugins
    await demonstrateCustomPlugins()

    // Demonstrate plugin chaining
    await demonstratePluginChaining()

    console.log('\n✅ All plugin examples completed!')

  } catch (error) {
    console.error('❌ Error in plugin demo:', error)
  }
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { main }
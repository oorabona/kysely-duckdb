/**
 * Database Migrations Example
 * 
 * This example demonstrates the migration system in kysely-duckdb,
 * including both SQL and TypeScript migrations.
 */

import { Kysely, sql, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { 
  DuckDbDialect, 
  FileMigrationProvider,
  InMemoryMigrationProvider,
  createMigrationTemplate,
  createTsMigrationTemplate
} from '@oorabona/kysely-duckdb'
import { Migrator } from 'kysely'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'

// Database schema after migrations
interface DatabaseSchema {
  users: {
    id: ColumnType<number, number | undefined, number>
    name: string
    email: string
    created_at: ColumnType<Date, Date | undefined, Date>
    updated_at: ColumnType<Date, Date | undefined, Date>
  }
  posts: {
    id: ColumnType<number, number | undefined, number>
    title: string
    content: string
    author_id: number
    status: 'draft' | 'published' | 'archived'
    tags: string[]
    metadata: unknown
    created_at: ColumnType<Date, Date | undefined, Date>
    updated_at: ColumnType<Date, Date | undefined, Date>
  }
  comments: {
    id: ColumnType<number, number | undefined, number>
    post_id: number
    author_id: number
    content: string
    created_at: ColumnType<Date, Date | undefined, Date>
  }
}

async function setupMigrationFiles() {
  const migrationsDir = './temp_migrations'
  
  // Create migrations directory
  await fs.mkdir(migrationsDir, { recursive: true })

  // Helper: deterministic timestamp (YYYYMMDDHHmmss)
  const pad = (n: number) => n.toString().padStart(2, '0')
  const formatTs = (d: Date) => {
    const y = d.getFullYear()
    const M = pad(d.getMonth() + 1)
    const D = pad(d.getDate())
    const h = pad(d.getHours())
    const m = pad(d.getMinutes())
    const s = pad(d.getSeconds())
    return `${y}${M}${D}${h}${m}${s}`
  }
  const base = new Date()
  const ts = (incSec: number) => formatTs(new Date(base.getTime() + incSec * 1000))

  // Migration 1: Create users table (SQL)
  const migration1Name = `${ts(0)}_create_users_table`
  const migration1Content = `-- Migration: Create users table
-- Created: ${new Date().toISOString()}

-- migrate:up
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX idx_users_email ON users(email);

-- migrate:down
DROP INDEX idx_users_email;
DROP TABLE users;`
  
  await fs.writeFile(join(migrationsDir, `${migration1Name}.sql`), migration1Content)

  // Slight delay to ensure strictly increasing timestamps in filenames
  await new Promise(r => setTimeout(r, 5))

  // Migration 2: Create posts table (TypeScript)
  const migration2Name = `${ts(1)}_create_posts_table`
  const migration2Content = `import { sql, type Kysely } from 'kysely'

/**
 * Create posts table with references to users
 * Created: ${new Date().toISOString()}
 */

export async function up(db: Kysely<any>): Promise<void> {
  await db.schema
    .createTable('posts')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('title', 'varchar(255)', col => col.notNull())
    .addColumn('content', 'text')
    .addColumn('author_id', 'integer', col => 
      // DuckDB doesn't support ON DELETE CASCADE actions; use a basic FK reference
      col.references('users.id').notNull()
    )
    .addColumn('status', 'varchar(20)', col => 
      col.defaultTo('draft').check(sql\`status IN ('draft', 'published', 'archived')\`)
    )
    // Use raw data type to bypass Kysely's data type parser for array suffix
    .addColumn('tags', sql\`VARCHAR[]\`, col => col.defaultTo(sql\`[]::VARCHAR[]\`))
    // Ensure JSON default is a typed JSON literal
    .addColumn('metadata', 'json', col => col.defaultTo(sql\`'{}'::JSON\`))
    .addColumn('created_at', 'timestamp', col => col.defaultTo(sql\`CURRENT_TIMESTAMP\`))
    .addColumn('updated_at', 'timestamp', col => col.defaultTo(sql\`CURRENT_TIMESTAMP\`))
    .execute()

  // Create indexes for better performance
  await db.schema
    .createIndex('idx_posts_author')
    .on('posts')
    .column('author_id')
    .execute()

  await db.schema
    .createIndex('idx_posts_status')
    .on('posts')
    .column('status')
    .execute()
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropIndex('idx_posts_status').execute()
  await db.schema.dropIndex('idx_posts_author').execute()
  await db.schema.dropTable('posts').execute()
}`
  
  await fs.writeFile(join(migrationsDir, `${migration2Name}.ts`), migration2Content)

  // Ensure next timestamp is greater
  await new Promise(r => setTimeout(r, 5))

  // Migration 3: Create comments table (SQL)
  const migration3Name = `${ts(2)}_create_comments_table`
  const migration3Content = `-- Migration: Create comments table
-- Created: ${new Date().toISOString()}

-- migrate:up
CREATE TABLE comments (
  id INTEGER PRIMARY KEY,
  post_id INTEGER NOT NULL REFERENCES posts(id),
  author_id INTEGER NOT NULL REFERENCES users(id),
  content TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create composite index for efficient queries
CREATE INDEX idx_comments_post_created ON comments(post_id, created_at);

-- migrate:down
DROP INDEX idx_comments_post_created;
DROP TABLE comments;`
  
  await fs.writeFile(join(migrationsDir, `${migration3Name}.sql`), migration3Content)

  // Ensure next timestamp is greater
  await new Promise(r => setTimeout(r, 5))

  // Migration 4: Add updated_at trigger (SQL)
  const migration4Name = `${ts(3)}_add_updated_at_support`
  const migration4Content = `-- Migration: Add updated_at support (DuckDB compatible)
-- Created: ${new Date().toISOString()}

-- migrate:up
-- In DuckDB, triggers and user-defined trigger functions are not supported.
-- We demonstrate adding an updated_at column if missing and backfilling values.

ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
UPDATE users SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP);

ALTER TABLE posts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
UPDATE posts SET updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP);

-- migrate:down
-- Remove the columns added above. Note: this will drop any existing data in them.
ALTER TABLE posts DROP COLUMN IF EXISTS updated_at;
ALTER TABLE users DROP COLUMN IF EXISTS updated_at;`
  
  await fs.writeFile(join(migrationsDir, `${migration4Name}.sql`), migration4Content)

  return migrationsDir
}

async function demonstrateInMemoryMigrations(db: Kysely<DatabaseSchema>) {
  console.log('\n📝 In-Memory Migrations Demo\n')

  // Create in-memory migrations
  const migrations = {
    '001_create_test_table': {
      async up(db: Kysely<any>) {
        await db.schema
          .createTable('test_table')
          .addColumn('id', 'integer', col => col.primaryKey())
          .addColumn('name', 'varchar(100)')
          .execute()
      },
      async down(db: Kysely<any>) {
        await db.schema.dropTable('test_table').execute()
      }
    },
    '002_add_test_data': {
      async up(db: Kysely<any>) {
        await db
          .insertInto('test_table')
          .values([
            { id: 1, name: 'Test 1' },
            { id: 2, name: 'Test 2' }
          ])
          .execute()
      },
      async down(db: Kysely<any>) {
        await db.deleteFrom('test_table').execute()
      }
    }
  }

  const migrator = new Migrator({
    db,
    provider: new InMemoryMigrationProvider(migrations)
  })

  // Run migrations
  const { error, results } = await migrator.migrateToLatest()
  
  if (error) {
    console.error('Migration failed:', error)
    return
  }

  console.log(`✅ Executed ${results?.length} migrations`)
  results?.forEach(result => {
    console.log(`  - ${result.migrationName}: ${result.status}`)
  })

  // Check migration status
  const allMigrations = await migrator.getMigrations()
  const executedMigrations = results || []
  const pendingMigrations = allMigrations.filter(m => !executedMigrations.some(r => r.migrationName === m.name))
  
  console.log(`\n📊 Migration Status:`)
  console.log(`  - Executed: ${executedMigrations.length}`)
  console.log(`  - Pending: ${pendingMigrations.length}`)

  // Verify data exists
  const testData = await sql<{id: number, name: string}>`SELECT * FROM test_table`.execute(db)
  console.log(`\n📋 Test data (${testData.rows.length} rows):`)
  console.table(testData.rows)

  // Rollback one migration
  console.log('\n⏪ Rolling back last migration...')
  const { error: rollbackError } = await migrator.migrateDown()
  
  if (!rollbackError) {
    console.log('✅ Rollback successful')
    
    // Verify data was removed
    const afterRollback = await sql<{id: number, name: string}>`SELECT * FROM test_table`.execute(db)
    console.log(`📋 Data after rollback (${afterRollback.rows.length} rows):`)
    console.table(afterRollback.rows)
  }

  // Clean up
  await migrator.migrateDown() // Remove table
}

async function demonstrateFileMigrations(db: Kysely<DatabaseSchema>) {
  console.log('\n📁 File-Based Migrations Demo\n')

  const migrationsDir = await setupMigrationFiles()

  try {
    const migrator = new Migrator({
      db,
      provider: new FileMigrationProvider(migrationsDir)
    })

    // Show pending migrations
    const initialStatus = await migrator.getMigrations()
    const pendingMigrations = initialStatus.filter(m => !m.executedAt)
    console.log(`📋 Found ${pendingMigrations.length} pending migrations:`)
    pendingMigrations.forEach(migration => {
      console.log(`  - ${migration.name}`)
    })

    // Run all migrations
    console.log('\n▶️  Running migrations...')
    const { error, results } = await migrator.migrateToLatest()
    
    if (error) {
      console.error('❌ Migration failed:', error)
      return
    }

    console.log(`\n✅ Successfully executed ${results?.length} migrations:`)
    results?.forEach(result => {
      console.log(`  - ${result.migrationName}: ${result.status}`)
    })

    // Verify tables were created
    const tables = await sql`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'main'
      ORDER BY table_name
    `.execute(db)
    
    console.log('\n📊 Created tables:')
    tables.rows.forEach((row: any) => {
      console.log(`  - ${row.table_name}`)
    })

    // Insert test data
    console.log('\n📝 Inserting test data...')
    
    const user = await db
      .insertInto('users')
      .values({
        id: 1,
        name: 'John Doe',
        email: 'john@example.com'
      })
      .returningAll()
      .executeTakeFirstOrThrow()

    const post = await db
      .insertInto('posts')
      .values({
        id: 1,
        title: 'My First Post',
        content: 'This is the content of my first post.',
        author_id: user.id,
        status: 'published',
        tags: ['intro', 'first-post'],
        metadata: JSON.stringify({ featured: true })
      })
      .returningAll()
      .executeTakeFirstOrThrow()

    await db
      .insertInto('comments')
      .values({
        id: 1,
        post_id: post.id,
        author_id: user.id,
        content: 'Great first post!'
      })
      .execute()

    // Query the data
    const postWithComments = await db
      .selectFrom('posts')
      .innerJoin('users', 'users.id', 'posts.author_id')
      .leftJoin('comments', 'comments.post_id', 'posts.id')
      .select([
        'posts.title',
        'posts.content',
        'posts.status',
        'users.name as author_name',
        'comments.content as comment'
      ])
      .execute()

    console.log('\n📋 Post with comments:')
    console.table(postWithComments)

    // Test rollback
    console.log('\n⏪ Testing rollback...')
    const { error: rollbackError } = await migrator.migrateDown()
    
    if (!rollbackError) {
      console.log('✅ Rollback successful')
      
      // Check final status
      const finalStatus = await migrator.getMigrations()
      const executedFinal = finalStatus.filter(m => m.executedAt)
      const pendingFinal = finalStatus.filter(m => !m.executedAt)
      console.log(`\n📊 Final Status:`)
      console.log(`  - Executed: ${executedFinal.length}`)
      console.log(`  - Pending: ${pendingFinal.length}`)
    }

  } finally {
    // Clean up migration files
    await fs.rm(migrationsDir, { recursive: true, force: true })
  }
}

async function demonstrateMigrationUtilities() {
  console.log('\n🛠️  Migration Utilities Demo\n')

  // Generate migration names
  // Deterministic examples (for display only)
  const fmt = (desc: string) => {
    const now = new Date()
    const y = now.getFullYear()
    const pad = (n: number) => n.toString().padStart(2, '0')
    const M = pad(now.getMonth() + 1)
    const D = pad(now.getDate())
    const h = pad(now.getHours())
    const m = pad(now.getMinutes())
    const s = pad(now.getSeconds())
    const slug = desc.toLowerCase().replace(/[^a-z0-9]/g, '_')
    return `${y}${M}${D}${h}${m}${s}_${slug}`
  }
  const name1 = fmt('create users table')
  const name2 = fmt('add-indexes-for-performance')
  
  console.log('📝 Generated migration names:')
  console.log(`  - ${name1}`)
  console.log(`  - ${name2}`)

  // Create SQL template
  const sqlTemplate = createMigrationTemplate('Add user preferences table')
  console.log('\n📄 SQL Migration Template:')
  console.log(sqlTemplate.split('\n').slice(0, 10).join('\n') + '\n...')

  // Create TypeScript template
  const tsTemplate = createTsMigrationTemplate('Add user preferences table')
  console.log('\n📄 TypeScript Migration Template:')
  console.log(tsTemplate.split('\n').slice(0, 10).join('\n') + '\n...')
}

async function main() {
  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({
      database,
    }),
  })

  try {
    console.log('🔄 Database Migrations Demo\n')

    // Demonstrate utilities
    await demonstrateMigrationUtilities()

    // Demonstrate in-memory migrations
    await demonstrateInMemoryMigrations(db)

    // Demonstrate file-based migrations
    await demonstrateFileMigrations(db)

    console.log('\n✅ Migration examples complete!')

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { main }
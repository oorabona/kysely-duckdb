/**
 * Basic Usage Example
 * 
 * This example demonstrates the fundamental features of kysely-duckdb,
 * including database creation, schema definition, CRUD operations,
 * joins, transactions, and DuckDB-specific features.
 */

import { Kysely, sql, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

// Define your database schema
interface DatabaseSchema {
  person: {
    id: number
    name: string
    email: string
    // default value filled by DB on insert
    created_at: ColumnType<Date, Date | undefined, Date>
  }
  post: {
    id: number
    title: string
    content: string
    author_id: number
    published: boolean
    tags: string[]
    metadata: unknown
  }
}

async function main() {
  console.log('🚀 Basic Usage Demo\n')
  
  // Create DuckDB database instance
  const database = await DuckDBInstance.create(':memory:')

  // Create Kysely instance with DuckDB dialect
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({
      database,
    }),
  })

  try {
    // ===============================
    // 1. SCHEMA CREATION
    // ===============================
    console.log('1. Creating database schema...')
    
    await db.schema
      .createTable('person')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('name', 'varchar(255)', col => col.notNull())
      .addColumn('email', 'varchar(255)', col => col.unique())
      .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
      .execute()

    await db.schema
      .createTable('post')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('title', 'varchar(255)', col => col.notNull())
      .addColumn('content', 'text')
      .addColumn('author_id', 'integer')
      .addColumn('published', 'boolean', col => col.defaultTo(false))
      .addColumn('tags', sql`varchar[]`)
      .addColumn('metadata', 'json', col => col.defaultTo('{}'))
      .execute()

    console.log('✅ Tables created successfully\n')

    // ===============================
    // 2. INSERT OPERATIONS
    // ===============================
    console.log('2. Inserting data...')
    
    const person1 = await db
      .insertInto('person')
      .values({
        id: 1,
        name: 'John Doe',
        email: 'john@example.com',
      })
      .returningAll()
      .executeTakeFirstOrThrow()

    console.log(`✅ Inserted person: ${person1.name} (ID: ${person1.id})`)

    const post1 = await db
      .insertInto('post')
      .values({
        id: 1,
        title: 'Getting Started with DuckDB and Kysely',
        content: 'This is a great combination for type-safe SQL queries! DuckDB provides excellent analytical capabilities while Kysely ensures type safety.',
        author_id: person1.id,
        published: true,
        tags: sql`ARRAY['duckdb', 'kysely', 'typescript', 'tutorial']`,
        metadata: JSON.stringify({
          views: 150,
          likes: 25,
          categories: ['tutorial', 'database'],
          reading_time: 5
        }),
      })
      .returningAll()
      .executeTakeFirstOrThrow()

    console.log(`✅ Inserted post: "${post1.title}" (ID: ${post1.id})\n`)

    // ===============================
    // 3. QUERY OPERATIONS
    // ===============================
    console.log('3. Querying data...')
    
    const publishedPosts = await db
      .selectFrom('post')
      .innerJoin('person', 'person.id', 'post.author_id')
      .select([
        'post.id',
        'post.title',
        'post.content',
        'post.tags',
        'post.published',
        'person.name as author_name',
        'person.email as author_email',
        'person.created_at as author_joined'
      ])
      .where('post.published', '=', true)
      .orderBy('post.id')
      .execute()

    console.log(`📚 Found ${publishedPosts.length} published posts:`)
    console.table(publishedPosts)

    // ===============================
    // 4. AGGREGATIONS
    // ===============================
    console.log('\n4. Running aggregations...')
    
    const postStats = await db
      .selectFrom('post')
      .select([
        eb => eb.fn.count('id').as('total_posts'),
        eb => eb.fn.count('id').filterWhere('published', '=', true).as('published_posts'),
        eb => eb.fn.count('id').filterWhere('published', '=', false).as('draft_posts'),
      ])
      .executeTakeFirstOrThrow()

    console.log('📊 Post statistics:')
    console.table([postStats])

    // ===============================
    // 5. DUCKDB ARRAY OPERATIONS
    // ===============================
    console.log('\n5. DuckDB array operations...')
    
    const postsWithDuckDBTag = await db
      .selectFrom('post')
      .select(['title', 'tags'])
      .where(() => sql<boolean>`tags && ARRAY['duckdb']::text[]`, '=', true)
      .execute()

    console.log('🏷️  Posts containing "duckdb" tag:')
    console.table(postsWithDuckDBTag)

    // ===============================
    // 6. JSON OPERATIONS
    // ===============================
    console.log('\n6. JSON operations...')
    
    const postsWithViews = await db
      .selectFrom('post')
      .select([
        'title',
        eb => sql<number>`json_extract(metadata, '$.views')`.as('views'),
        eb => sql<number>`json_extract(metadata, '$.likes')`.as('likes'),
        eb => sql<string[]>`json_extract(metadata, '$.categories')`.as('categories')
      ])
      .where(eb => sql`json_extract(metadata, '$.views')`, '>', 100)
      .execute()

    console.log('👀 Posts with more than 100 views:')
    console.table(postsWithViews)

    // ===============================
    // 7. TRANSACTIONS
    // ===============================
    console.log('\n7. Transaction example...')
    
    await db.transaction().execute(async trx => {
      const person2 = await trx
        .insertInto('person')
        .values({
          id: 2,
          name: 'Jane Smith',
          email: 'jane@example.com',
        })
        .returningAll()
        .executeTakeFirstOrThrow()

      await trx
        .insertInto('post')
        .values({
          id: 2,
          title: 'Advanced DuckDB Features',
          content: 'Exploring spatial extensions, vector operations, and advanced analytics with DuckDB. This database is perfect for analytical workloads.',
          author_id: person2.id,
          published: false,
          tags: sql`ARRAY['duckdb', 'advanced', 'spatial', 'analytics']`,
          metadata: JSON.stringify({ 
            draft: true, 
            estimated_reading_time: 8,
            complexity: 'advanced'
          }),
        })
        .execute()

      console.log(`✅ Transaction completed: Added ${person2.name} and their draft post`)
    })

    // ===============================
    // 8. ADVANCED QUERIES
    // ===============================
    console.log('\n8. Advanced query patterns...')
    
    // Window functions
    const authorStats = await db
      .selectFrom('person')
      .leftJoin('post', 'post.author_id', 'person.id')
      .select([
        'person.name',
        'person.email',
        eb => eb.fn.count('post.id').as('total_posts'),
        eb => eb.fn.count('post.id').filterWhere('post.published', '=', true).as('published_posts'),
        eb => sql<number>`ROW_NUMBER() OVER (ORDER BY COUNT(post.id) DESC)`.as('author_rank')
      ])
      .groupBy(['person.id', 'person.name', 'person.email'])
      .orderBy('total_posts', 'desc')
      .execute()

    console.log('👥 Author statistics:')
    console.table(authorStats)

    // ===============================
    // 9. FINAL SUMMARY
    // ===============================
    const finalSummary = await db
      .selectFrom('person')
      .select((eb) => [
        eb.fn.count('id').as('total_authors'),
        sql`(SELECT COUNT(*) FROM post)`.as('total_posts'),
        sql`(SELECT COUNT(*) FROM post WHERE published = true)`.as('published_posts')
      ])
      .executeTakeFirstOrThrow()

    console.log('\n📈 Final summary:')
    console.table([finalSummary])

    console.log('\n✅ Basic usage demo completed successfully!')

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
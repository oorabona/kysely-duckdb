/**
 * Advanced Performance Monitoring Example
 * 
 * This example demonstrates how to use the built-in performance monitoring
 * capabilities of kysely-duckdb for production applications.
 */

import { Kysely } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { 
  DuckDbDialect, 
  globalPerformanceMonitor,
  formatPerformanceStats,
  checkEnvironment
} from '@oorabona/kysely-duckdb'

interface DatabaseSchema {
  products: {
    id: number
    name: string
    price: number
    category_id: number
    created_at: Date
  }
  categories: {
    id: number
    name: string
  }
}

async function performanceMonitoringExample() {
  console.log('🔍 Running Performance Monitoring Example...\n')

  // 1. Environment Check
  console.log('1. Checking Environment Compatibility...')
  const envCheck = await checkEnvironment()
  
  if (!envCheck.overall) {
    console.warn('⚠️  Environment Issues Detected:')
    envCheck.allWarnings.forEach(warning => console.warn(`   - ${warning}`))
  } else {
    console.log('✅ Environment is fully compatible')
  }
  
  console.log(`   Node.js: ${envCheck.node.version}`)
  if (envCheck.duckdb) {
    console.log(`   DuckDB: ${envCheck.duckdb.version}`)
  }
  console.log()

  // 2. Setup Database with Performance Monitoring
  const database = await DuckDBInstance.create(':memory:')
  const db = new Kysely<DatabaseSchema>({
    dialect: new DuckDbDialect({ database }),
    log: (event) => {
      // Use Kysely's built-in logging alongside our custom monitoring
      const { query, queryDurationMillis } = event
      const duration = queryDurationMillis || 0
      const performanceIcon = duration > 100 ? '🐌' : duration > 50 ? '⚡' : '🚀'
      console.log(`${performanceIcon} Query: ${duration}ms - ${query.sql.replace(/\s+/g, ' ').trim()}`)
    }
  })

  // 3. Create Schema and Insert Test Data
  console.log('2. Setting up test data...')
  
  await db.schema
    .createTable('categories')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(255)', col => col.notNull())
    .execute()

  await db.schema
    .createTable('products')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(255)', col => col.notNull())
    .addColumn('price', 'decimal', col => col.notNull())
    .addColumn('category_id', 'integer', col => col.references('categories.id'))
    .addColumn('created_at', 'timestamp', col => col.defaultTo('CURRENT_TIMESTAMP'))
    .execute()

  // Insert categories
  await db.insertInto('categories')
    .values([
      { id: 1, name: 'Electronics' },
      { id: 2, name: 'Books' },
      { id: 3, name: 'Clothing' }
    ])
    .execute()

  // Insert products (larger dataset to demonstrate performance)
  const products = []
  for (let i = 1; i <= 1000; i++) {
    products.push({
      id: i,
      name: `Product ${i}`,
      price: Math.floor(Math.random() * 100) + 10,
      category_id: (i % 3) + 1,
      created_at: new Date()
    })
  }

  // Batch insert for better performance
  console.log('📦 Inserting 1000 products...')
  const batchSize = 100
  for (let i = 0; i < products.length; i += batchSize) {
    const batch = products.slice(i, i + batchSize)
    await db.insertInto('products').values(batch).execute()
  }

  // 4. Run Various Query Types
  console.log('\n3. Running performance test queries...')

  // Simple select
  await db.selectFrom('products').selectAll().limit(10).execute()

  // Join query
  await db
    .selectFrom('products')
    .innerJoin('categories', 'categories.id', 'products.category_id')
    .select(['products.name', 'products.price', 'categories.name as category'])
    .where('products.price', '>', 50)
    .orderBy('products.price', 'desc')
    .limit(20)
    .execute()

  // Aggregation query
  await db
    .selectFrom('products')
    .innerJoin('categories', 'categories.id', 'products.category_id')
    .select([
      'categories.name as category',
      eb => eb.fn.count('products.id').as('product_count'),
      eb => eb.fn.avg('products.price').as('avg_price'),
      eb => eb.fn.max('products.price').as('max_price')
    ])
    .groupBy('categories.name')
    .execute()

  // Complex analytical query
  await db
    .selectFrom('products')
    .select([
      'name',
      'price'
    ])
    .orderBy('price', 'desc')
    .limit(10)
    .execute()

  // Intentionally slow query for demonstration
  console.log('🐌 Running intentionally slow query...')
  await db
    .selectFrom('products as p1')
    .innerJoin('products as p2', 'p1.category_id', 'p2.category_id')
    .select(['p1.name', 'p2.name as related_product'])
    .where('p1.id', '<', eb => eb.ref('p2.id'))
    .limit(50)
    .execute()

  // 5. Display Performance Statistics
  console.log('\n4. Performance Statistics:')
  console.log('=' .repeat(50))
  
  const stats = globalPerformanceMonitor.getStats()
  console.log(formatPerformanceStats(stats))

  // 6. Recent Query Analysis
  console.log('Recent Queries:')
  const recentQueries = globalPerformanceMonitor.getRecentQueries(5)
  recentQueries.forEach((query, index) => {
    const duration = query.duration.toFixed(2)
    const icon = query.duration > 100 ? '🐌' : query.duration > 50 ? '⚡' : '🚀'
    console.log(`${index + 1}. ${icon} ${duration}ms - ${query.sql.slice(0, 60)}...`)
  })

  // 7. Slow Query Analysis
  const slowQueries = globalPerformanceMonitor.getQueriesByDuration(50)
  if (slowQueries.length > 0) {
    console.log('\n🐌 Slow Queries (>50ms):')
    slowQueries.forEach((query, index) => {
      console.log(`${index + 1}. ${query.duration.toFixed(2)}ms - ${query.sql.slice(0, 80)}...`)
    })
  }

  // 8. Performance Recommendations
  console.log('\n💡 Performance Recommendations:')
  if (stats.averageDuration > 100) {
    console.log('- Consider optimizing queries or adding indexes')
  }
  if (stats.slowQueries.length > stats.totalQueries * 0.1) {
    console.log('- High percentage of slow queries detected')
  }
  if (stats.errorRate > 0.01) {
    console.log('- Error rate is elevated, check query syntax and constraints')
  }

  console.log('- Use EXPLAIN to analyze query plans for slow queries')
  console.log('- Consider using DuckDB\'s built-in profiling: PRAGMA enable_profiling')
  console.log('- Monitor memory usage for large analytical workloads')

  // Cleanup (DuckDB instance cleanup is automatic)
  console.log('\n✅ Performance monitoring example completed!')
}

// Run the example
performanceMonitoringExample().catch(console.error)
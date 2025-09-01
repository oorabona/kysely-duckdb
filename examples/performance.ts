/**
 * Performance Optimization Example
 * 
 * This example demonstrates performance optimization techniques with
 * DuckDB, including indexing, query optimization, parallel processing,
 * and monitoring query performance.
 */

import { Kysely, sql, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

// Database schema for performance examples
interface PerformanceSchema {
  large_table: {
    id: number
    category: string
    value: number
    timestamp: Date
    data: string
    indexed_field: string
  }
  lookup_table: {
    id: number
    name: string
    type: string
  }
  partitioned_sales: {
    id: number
    product_id: number
    amount: number
    sale_date: Date
    region: string
  }
  time_series: {
    timestamp: Date
    metric_name: string
    value: number
    tags: unknown
  }
  perf_test: {
    id: number
    value: number
    category: string
  }
}

async function createLargeDataset(db: Kysely<PerformanceSchema>) {
  console.log('📊 Creating large dataset for performance testing...')
  
  // Create large table with proper types
  await db.schema
    .createTable('large_table')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('category', 'varchar(50)', col => col.notNull())
    .addColumn('value', sql`decimal(10,2)`, col => col.notNull())
    .addColumn('timestamp', 'timestamp', col => col.notNull())
    .addColumn('data', 'text')
    .addColumn('indexed_field', 'varchar(100)', col => col.notNull())
    .execute()

  // Create lookup table
  await db.schema
    .createTable('lookup_table')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(255)', col => col.notNull())
    .addColumn('type', 'varchar(50)', col => col.notNull())
    .execute()

  // Create partitioned sales table
  await db.schema
    .createTable('partitioned_sales')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('product_id', 'integer', col => col.notNull())
    .addColumn('amount', sql`decimal(10,2)`, col => col.notNull())
    .addColumn('sale_date', 'date', col => col.notNull())
    .addColumn('region', 'varchar(50)', col => col.notNull())
    .execute()

  // Create time series table
  await db.schema
    .createTable('time_series')
    .addColumn('timestamp', 'timestamp', col => col.notNull())
    .addColumn('metric_name', 'varchar(100)', col => col.notNull())
    .addColumn('value', sql`decimal(15,4)`, col => col.notNull())
    .addColumn('tags', 'json')
    .execute()

  console.log('📝 Generating test data...')

  // Insert large dataset in batches for better performance
  const batchSize = 5000
  const totalRecords = 100000
  const categories = ['electronics', 'clothing', 'books', 'home', 'sports', 'automotive', 'health', 'beauty']
  const regions = ['North', 'South', 'East', 'West', 'Central']

  for (let batch = 0; batch < totalRecords / batchSize; batch++) {
    const batchData = []
    
    for (let i = 0; i < batchSize; i++) {
      const id = batch * batchSize + i + 1
      const category = categories[Math.floor(Math.random() * categories.length)]!
      batchData.push({
        id,
        category,
        value: Math.round((Math.random() * 1000 + 10) * 100) / 100,
        timestamp: new Date(2024, 0, 1 + Math.floor(Math.random() * 365)),
        data: `Sample data for record ${id} with some content to test performance`,
        indexed_field: `idx_${Math.floor(id / 100)}_${categories[id % categories.length]!}`
      })
    }

    await db.insertInto('large_table').values(batchData).execute()

    if ((batch + 1) % 5 === 0) {
      console.log(`  ✓ Inserted ${((batch + 1) * batchSize).toLocaleString()} records`)
    }
  }

  // Insert lookup data
  const lookupData = Array.from({ length: 1000 }, (_, i) => ({
    id: i + 1,
    name: `Item ${i + 1}`,
    type: categories[i % categories.length]!
  }))
  await db.insertInto('lookup_table').values(lookupData).execute()

  // Insert partitioned sales data
  const salesData = Array.from({ length: 50000 }, (_, i) => ({
    id: i + 1,
    product_id: Math.floor(Math.random() * 1000) + 1,
    amount: Math.round((Math.random() * 500 + 10) * 100) / 100,
    sale_date: new Date(2024, Math.floor(Math.random() * 12), Math.floor(Math.random() * 28) + 1),
    region: regions[Math.floor(Math.random() * regions.length)]!
  }))
  await db.insertInto('partitioned_sales').values(salesData).execute()

  // Insert time series data
  const metrics = ['cpu_usage', 'memory_usage', 'disk_io', 'network_traffic', 'response_time']
  const timeSeriesData = []
  
  for (let day = 0; day < 30; day++) {
    for (let hour = 0; hour < 24; hour++) {
      for (const metric of metrics) {
        timeSeriesData.push({
          timestamp: new Date(2024, 0, day + 1, hour),
          metric_name: metric,
          value: Math.random() * 100,
          tags: JSON.stringify({
            server: `server-${Math.floor(Math.random() * 10) + 1}`,
            environment: Math.random() > 0.5 ? 'prod' : 'staging'
          })
        })
      }
    }
  }
  await db.insertInto('time_series').values(timeSeriesData).execute()

  console.log(`✅ Created ${totalRecords.toLocaleString()} records across multiple tables`)
}

async function demonstrateIndexing(db: Kysely<PerformanceSchema>) {
  console.log('\n🏃 Indexing Performance\n')

  // 1. Query without index (baseline)
  console.log('1. Query performance without indexes:')
  
  const startTime = Date.now()
  const resultWithoutIndex = await db
    .selectFrom('large_table')
    .select(['id', 'category', 'value'])
    .where('category', '=', 'electronics')
    .where('value', '>', 500)
    .orderBy('value', 'desc')
    .limit(10)
    .execute()
  
  const timeWithoutIndex = Date.now() - startTime
  console.log(`⏱️  Query time without index: ${timeWithoutIndex}ms`)
  console.log(`📊 Found ${resultWithoutIndex.length} records`)

  // 2. Create indexes
  console.log('\n2. Creating performance indexes...')
  
  await sql`CREATE INDEX idx_large_table_category ON large_table(category)`.execute(db)
  await sql`CREATE INDEX idx_large_table_value ON large_table(value)`.execute(db)
  await sql`CREATE INDEX idx_large_table_compound ON large_table(category, value)`.execute(db)
  await sql`CREATE INDEX idx_large_table_timestamp ON large_table(timestamp)`.execute(db)
  
  console.log('✅ Indexes created')

  // 3. Query with index
  console.log('\n3. Query performance with indexes:')
  
  const startTimeWithIndex = Date.now()
  const resultWithIndex = await db
    .selectFrom('large_table')
    .select(['id', 'category', 'value'])
    .where('category', '=', 'electronics')
    .where('value', '>', 500)
    .orderBy('value', 'desc')
    .limit(10)
    .execute()
  
  const timeWithIndex = Date.now() - startTimeWithIndex
  console.log(`⏱️  Query time with index: ${timeWithIndex}ms`)
  console.log(`📊 Found ${resultWithIndex.length} records`)

  // Report improvement robustly: show speedup when faster, slowdown when slower
  if (timeWithIndex < timeWithoutIndex) {
    const improvementPct = Math.round(((timeWithoutIndex - timeWithIndex) / timeWithoutIndex) * 100)
    const speedup = (timeWithoutIndex / Math.max(1, timeWithIndex)).toFixed(2)
    console.log(`🚀 Improvement: ${improvementPct}% faster (${speedup}x speedup)`) 
  } else if (timeWithIndex > timeWithoutIndex) {
    const slowdownPct = Math.round(((timeWithIndex - timeWithoutIndex) / timeWithoutIndex) * 100)
    const slowdown = (timeWithIndex / Math.max(1, timeWithoutIndex)).toFixed(2)
    console.log(`ℹ️  Index did not help here: ${slowdownPct}% slower (${slowdown}x)`) 
  } else {
    console.log('ℹ️  No observed difference with/without index')
  }

  // 4. Query plan analysis
  console.log('\n4. Query plan analysis:')
  
  const queryPlan = await sql`
    EXPLAIN ANALYZE 
    SELECT id, category, value 
    FROM large_table 
    WHERE category = 'electronics' 
    AND value > 500 
    ORDER BY value DESC 
    LIMIT 10
  `.execute(db)
  
  console.log('Query Execution Plan:')
  queryPlan.rows.forEach(row => {
    console.log(row)
  })
}

async function demonstrateQueryOptimization(db: Kysely<PerformanceSchema>) {
  console.log('\n⚡ Query Optimization Techniques\n')

  // 1. Subquery vs JOIN performance
  console.log('1. Subquery vs JOIN comparison:')
  
  // Subquery approach
  let startTime = Date.now()
  const subqueryResult = await db
    .selectFrom('large_table')
    .select(['category', 'value'])
    .where('id', 'in', 
      eb => eb.selectFrom('lookup_table')
        .select('id')
        .where('type', '=', 'electronics')
        .limit(1000)
    )
    .execute()
  
  const subqueryTime = Date.now() - startTime
  console.log(`⏱️  Subquery approach: ${subqueryTime}ms (${subqueryResult.length} records)`)

  // JOIN approach
  startTime = Date.now()
  const joinResult = await db
    .selectFrom('large_table')
    .innerJoin('lookup_table', 'lookup_table.id', 'large_table.id')
    .select(['large_table.category', 'large_table.value'])
    .where('lookup_table.type', '=', 'electronics')
    .execute()
  
  const joinTime = Date.now() - startTime
  console.log(`⏱️  JOIN approach: ${joinTime}ms (${joinResult.length} records)`)

  // 2. Aggregation optimization
  console.log('\n2. Aggregation performance:')
  
  // Pre-filter then aggregate
  startTime = Date.now()
  const optimizedAgg = await db
    .selectFrom('large_table')
    .select([
      'category',
      eb => eb.fn.count('id').as('count'),
      eb => eb.fn.avg('value').as('avg_value'),
      eb => eb.fn.sum('value').as('total_value')
    ])
    .where('timestamp', '>=', new Date('2024-06-01'))
    .groupBy('category')
    .having(eb => eb.fn.count('id'), '>', 100)
    .execute()
  
  const optimizedTime = Date.now() - startTime
  console.log(`⏱️  Optimized aggregation: ${optimizedTime}ms`)
  console.table(optimizedAgg)

  // 3. Column selection optimization
  console.log('\n3. Column selection impact:')
  
  // Select all columns
  startTime = Date.now()
  const allColumns = await db
    .selectFrom('large_table')
    .selectAll()
    .where('category', '=', 'books')
    .limit(1000)
    .execute()
  
  const allColumnsTime = Date.now() - startTime
  console.log(`⏱️  SELECT * time: ${allColumnsTime}ms`)

  // Select specific columns
  startTime = Date.now()
  const specificColumns = await db
    .selectFrom('large_table')
    .select(['id', 'category', 'value'])
    .where('category', '=', 'books')
    .limit(1000)
    .execute()
  
  const specificTime = Date.now() - startTime
  console.log(`⏱️  Specific columns time: ${specificTime}ms`)
  console.log(`💡 Column selection improvement: ${Math.round((allColumnsTime - specificTime) / allColumnsTime * 100)}%`)
}

async function demonstrateParallelProcessing(db: Kysely<PerformanceSchema>) {
  console.log('\n🔄 Parallel Processing\n')

  // 1. Parallel aggregations
  console.log('1. Parallel aggregation queries:')
  
  const startTime = Date.now()
  
  // Run multiple aggregations in parallel
  const [
    categoryStats,
    timeStats,
    valueStats
  ] = await Promise.all([
    db.selectFrom('large_table')
      .select([
        'category',
        eb => eb.fn.count('id').as('count'),
        eb => eb.fn.avg('value').as('avg_value')
      ])
      .groupBy('category')
      .execute(),
    
    db.selectFrom('large_table')
      .select([
        eb => sql<string>`DATE_TRUNC('month', timestamp)`.as('month'),
        eb => eb.fn.count('id').as('records'),
        eb => eb.fn.sum('value').as('total_value')
      ])
      .groupBy(sql`DATE_TRUNC('month', timestamp)`)
      .execute(),
    
    db.selectFrom('large_table')
      .select([
        eb => eb.fn.min('value').as('min_value'),
        eb => eb.fn.max('value').as('max_value'),
        eb => eb.fn.avg('value').as('avg_value'),
        eb => sql<number>`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value)`.as('median_value')
      ])
      .executeTakeFirstOrThrow()
  ])
  
  const parallelTime = Date.now() - startTime
  console.log(`⏱️  Parallel queries completed in: ${parallelTime}ms`)
  console.log(`📊 Category stats: ${categoryStats.length} categories`)
  console.log(`📊 Time periods: ${timeStats.length} months`)
  console.log(`📊 Value statistics:`, valueStats)

  // 2. Batch processing simulation
  console.log('\n2. Batch processing performance:')
  
  const batchSize = 10000
  const results = []
  
  const batchStartTime = Date.now()
  
  for (let offset = 0; offset < 50000; offset += batchSize) {
    const batch = await db
      .selectFrom('large_table')
      .select([
        'id',
        'value',
        eb => sql<number>`${eb.ref('value')} * 1.1`.as('adjusted_value')
      ])
      .orderBy('id')
      .limit(batchSize)
      .offset(offset)
      .execute()
    
    results.push(batch.length)
    
    if (results.length % 2 === 0) {
      console.log(`  ✓ Processed ${results.reduce((a, b) => a + b, 0).toLocaleString()} records`)
    }
  }
  
  const batchTime = Date.now() - batchStartTime
  console.log(`⏱️  Batch processing time: ${batchTime}ms`)
  console.log(`📊 Total records processed: ${results.reduce((a, b) => a + b, 0).toLocaleString()}`)
}

async function demonstrateMemoryOptimization(db: Kysely<PerformanceSchema>) {
  console.log('\n💾 Memory Optimization\n')

  // 1. Memory-efficient aggregations
  console.log('1. Memory-efficient time series aggregation:')
  
  const memoryStartTime = Date.now()
  
  // Use window functions instead of multiple passes
  const efficientTimeSeries = await sql<{
    date: string
    metric_name: string
    avg_value: number
    min_value: number
    max_value: number
    rolling_avg: number
  }>`
    WITH daily AS (
      SELECT 
        DATE(timestamp) AS date,
        metric_name,
        AVG(value) AS avg_value,
        MIN(value) AS min_value,
        MAX(value) AS max_value
      FROM time_series
      WHERE timestamp >= '2024-01-01'
      GROUP BY DATE(timestamp), metric_name
    )
    SELECT 
      date,
      metric_name,
      ROUND(avg_value, 2) AS avg_value,
      ROUND(min_value, 2) AS min_value,
      ROUND(max_value, 2) AS max_value,
      ROUND(AVG(avg_value) OVER (
        PARTITION BY metric_name
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ), 2) AS rolling_avg
    FROM daily
    ORDER BY metric_name, date
    LIMIT 50
  `.execute(db)
  
  const memoryTime = Date.now() - memoryStartTime
  console.log(`⏱️  Memory-efficient query: ${memoryTime}ms`)
  console.table(efficientTimeSeries.rows.slice(0, 10))

  // 2. Streaming large result sets
  console.log('\n2. Memory usage optimization for large datasets:')
  
  // Instead of loading all data, use LIMIT/OFFSET or streaming
  const streamingDemo = await sql<{
    processing_technique: string
    memory_usage: string
    performance_impact: string
  }>`
    SELECT * FROM (
      VALUES 
        ('Small batches (LIMIT/OFFSET)', 'Low - constant memory', 'Good for large datasets'),
        ('Window functions', 'Medium - single pass', 'Excellent for analytics'),
        ('Temporary tables', 'High - stores intermediate', 'Good for complex multi-step'),
        ('CTEs', 'Low - query optimization', 'Excellent for readability')
    ) AS techniques(processing_technique, memory_usage, performance_impact)
  `.execute(db)
  
  console.table(streamingDemo.rows)
}

async function demonstratePerformanceMonitoring(db: Kysely<PerformanceSchema>) {
  console.log('\n📈 Performance Monitoring\n')

  // Create a database instance with performance logging
  const database = await DuckDBInstance.create(':memory:')
  
  const monitoredDb = new Kysely<PerformanceSchema>({
    dialect: new DuckDbDialect({ 
      database
    }),
    // Use Kysely's native logging system for performance monitoring
    log: (event) => {
      const { query, queryDurationMillis } = event
      const duration = queryDurationMillis || 0
      const performanceIcon = duration > 1000 ? '⚠️  SLOW' : '✅'
      
      console.log(`${performanceIcon} ${duration}ms: ${query.sql.replace(/\s+/g, ' ').trim()}`)
      if (query.parameters?.length > 0) {
        console.log(`   Parameters: ${JSON.stringify(query.parameters)}`)
      }
    }
  })

  try {
    console.log('1. Performance monitoring with native logging:')
    
    // Copy some data to the monitored database
    await monitoredDb.schema
      .createTable('perf_test')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('value', sql`decimal(10,2)`)
      .addColumn('category', 'varchar(50)')
      .execute()

    await monitoredDb
      .insertInto('perf_test')
      .values(Array.from({ length: 1000 }, (_, i) => ({
        id: i + 1,
        value: Math.random() * 1000,
        category: `category_${i % 10}`
      })))
      .execute()

    // Run monitored queries
    await monitoredDb
      .selectFrom('perf_test')
      .select((eb) => [
        'category',
        eb.fn.count('id').as('count'),
        eb.fn.avg('value').as('avg_value')
      ])
      .groupBy('category')
      .execute()

    console.log('✅ Performance monitoring demonstrated (see logs above)')

    // 2. Query statistics
    console.log('\n2. Query performance statistics:')
    
    const queryStats = await sql`
      SELECT 
        'Complex Query Performance' as metric,
        'Completed' as status,
        'Use appropriate indexes' as recommendation
    `.execute(db)
    
    console.table(queryStats.rows)

  } finally {
    await monitoredDb.destroy()
    database.closeSync()
  }
}

async function demonstrateOptimizationBestPractices(db: Kysely<PerformanceSchema>) {
  console.log('\n🎯 Performance Best Practices\n')

  const bestPractices = [
    {
      practice: 'Use appropriate indexes',
      impact: 'High',
      description: 'Create indexes on frequently queried columns',
      example: 'CREATE INDEX idx_category ON table(category)'
    },
    {
      practice: 'Limit result sets',
      impact: 'High',
      description: 'Use LIMIT and specific column selection',
      example: 'SELECT id, name FROM table WHERE condition LIMIT 100'
    },
    {
      practice: 'Optimize JOINs',
      impact: 'Medium',
      description: 'Use appropriate JOIN types and order',
      example: 'INNER JOIN on indexed columns'
    },
    {
      practice: 'Use window functions',
      impact: 'Medium',
      description: 'Replace correlated subqueries with window functions',
      example: 'ROW_NUMBER() OVER (PARTITION BY col ORDER BY col2)'
    },
    {
      practice: 'Batch operations',
      impact: 'High',
      description: 'Insert/update in batches rather than row-by-row',
      example: 'INSERT INTO table VALUES (...), (...), (...)'
    },
    {
      practice: 'Use CTEs for readability',
      impact: 'Low',
      description: 'Common Table Expressions for complex queries',
      example: 'WITH cte AS (SELECT ...) SELECT * FROM cte'
    },
    {
      practice: 'Proper data types',
      impact: 'Medium',
      description: 'Use appropriate data types to save space',
      example: 'Use INTEGER instead of TEXT for numeric IDs'
    },
    {
      practice: 'Monitor query plans',
      impact: 'High',
      description: 'Use EXPLAIN ANALYZE to understand performance',
      example: 'EXPLAIN ANALYZE SELECT * FROM table WHERE condition'
    }
  ]

  console.table(bestPractices)

  // Demonstrate some anti-patterns vs optimized patterns
  console.log('\n❌ Common Anti-patterns vs ✅ Optimized Patterns:\n')

  console.log('1. SELECT optimization:')
  console.log('❌ SELECT * FROM large_table WHERE condition')
  console.log('✅ SELECT id, name FROM large_table WHERE indexed_column = value LIMIT 100')

  console.log('\n2. JOIN optimization:')
  console.log('❌ SELECT * FROM table1 t1 WHERE t1.id IN (SELECT id FROM table2 WHERE condition)')
  console.log('✅ SELECT t1.* FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.id WHERE t2.condition')

  console.log('\n3. Aggregation optimization:')
  console.log('❌ Multiple separate aggregation queries')
  console.log('✅ Single query with multiple aggregations and window functions')

  console.log('\n4. Data loading optimization:')
  console.log('❌ INSERT INTO table VALUES (1, \'a\'); INSERT INTO table VALUES (2, \'b\');')
  console.log('✅ INSERT INTO table VALUES (1, \'a\'), (2, \'b\'), (3, \'c\');')
}

async function main() {
  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<PerformanceSchema>({
    dialect: new DuckDbDialect({ database }),
  })

  try {
    console.log('⚡ Performance Optimization and Monitoring Demo\n')

    // Create large test dataset
    await createLargeDataset(db)

    // Demonstrate various optimization techniques
    await demonstrateIndexing(db)
    await demonstrateQueryOptimization(db)
    await demonstrateParallelProcessing(db)
    await demonstrateMemoryOptimization(db)
    await demonstratePerformanceMonitoring(db)
    await demonstrateOptimizationBestPractices(db)

    console.log('\n✅ Performance optimization demo complete!')
    console.log('\n💡 Key Takeaways:')
    console.log('• Index frequently queried columns')
    console.log('• Limit result sets and select specific columns')
    console.log('• Use window functions instead of correlated subqueries')
    console.log('• Batch operations for better throughput')
    console.log('• Monitor query plans with EXPLAIN ANALYZE')
    console.log('• Consider parallel processing for large datasets')

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
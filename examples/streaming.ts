/**
 * Streaming Example
 * 
 * This example demonstrates streaming large result sets with backpressure
 * handling to avoid memory issues when processing large datasets.
 */

import { Kysely, sql, type ColumnType } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'
import { Readable, Transform, Writable, pipeline } from 'node:stream'
import { promisify } from 'node:util'
import { createWriteStream } from 'node:fs'
import { promises as fs } from 'node:fs'

const pipelineAsync = promisify(pipeline)

// Database schema for streaming examples
interface StreamingSchema {
  large_dataset: {
    id: number
    category: string
    value: number
    timestamp: Date
    data: unknown
  }
  processed_data: {
    id: ColumnType<number, number | undefined, number>
    category: string
    total_value: number
    avg_value: number
    count: number
  }
}

// Create a readable stream from query results
class QueryResultStream extends Readable {
  private currentOffset = 0
  private readonly batchSize: number
  private readonly query: any
  private hasMoreData = true

  constructor(private db: Kysely<any>, query: any, batchSize = 1000) {
    super({ objectMode: true })
    this.query = query
    this.batchSize = batchSize
  }

  async _read() {
    if (!this.hasMoreData) {
      this.push(null) // Signal end of stream
      return
    }

    try {
      const results = await this.query
        .limit(this.batchSize)
        .offset(this.currentOffset)
        .execute()

      if (results.length === 0) {
        this.hasMoreData = false
        this.push(null)
        return
      }

      // Push each row individually for fine-grained control
      for (const row of results) {
        this.push(row)
      }

      this.currentOffset += results.length

      // If we got fewer results than batch size, we're at the end
      if (results.length < this.batchSize) {
        this.hasMoreData = false
      }

    } catch (error) {
      this.emit('error', error)
    }
  }
}

// Transform stream for data processing
class DataProcessorTransform extends Transform {
  private processed = 0

  constructor(options = {}) {
    super({ ...options, objectMode: true })
  }

  _transform(chunk: any, _encoding: any, callback: Function) {
    try {
      // Simulate some processing
      const processedChunk = {
        ...chunk,
        processed_at: new Date(),
        processed_value: chunk.value * 1.1, // 10% markup
        category_upper: chunk.category.toUpperCase()
      }

      this.processed++
      
      // Log progress every 1000 records
      if (this.processed % 1000 === 0) {
        console.log(`🔄 Processed ${this.processed} records...`)
      }

      callback(null, processedChunk)
    } catch (error) {
      callback(error)
    }
  }

  _flush(callback: Function) {
    console.log(`✅ Processing complete. Total records: ${this.processed}`)
    callback()
  }
}

// CSV writer stream
class CsvWriterStream extends Writable {
  private isFirstRow = true
  private writeStream: NodeJS.WritableStream

  constructor(filename: string) {
    super({ objectMode: true })
    this.writeStream = createWriteStream(filename, { encoding: 'utf8' })
  }

  _write(chunk: any, _encoding: any, callback: Function) {
    try {
      if (this.isFirstRow) {
        // Write CSV header
        const headers = Object.keys(chunk).join(',')
        this.writeStream.write(headers + '\n')
        this.isFirstRow = false
      }

      // Write CSV row
      const values = Object.values(chunk).map(v => 
        typeof v === 'string' ? `"${v.replace(/"/g, '""')}"` : v
      ).join(',')
      
      this.writeStream.write(values + '\n')
      callback()
    } catch (error) {
      callback(error)
    }
  }

  _final(callback: Function) {
    this.writeStream.end()
    callback()
  }
}

async function createLargeDataset(db: Kysely<StreamingSchema>) {
  console.log('📊 Creating large dataset for streaming demo...')
  
  // Create table
  await db.schema
    .createTable('large_dataset')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('category', 'varchar(50)')
    .addColumn('value', sql`decimal(10,2)`)
    .addColumn('timestamp', 'timestamp')
    .addColumn('data', 'json')
    .execute()

  const categories = ['electronics', 'clothing', 'books', 'home', 'sports']
  const batchSize = 1000
  const totalRecords = 10000

  console.log(`📝 Inserting ${totalRecords.toLocaleString()} records in batches...`)

  for (let batch = 0; batch < totalRecords / batchSize; batch++) {
    const batchData = []
    
    for (let i = 0; i < batchSize; i++) {
      const id = batch * batchSize + i + 1
      batchData.push({
        id,
        category: categories[Math.floor(Math.random() * categories.length)]!,
        value: Math.round((Math.random() * 1000 + 10) * 100) / 100,
        timestamp: new Date(2024, 0, 1 + Math.floor(Math.random() * 365)),
        data: JSON.stringify({
          batch: batch + 1,
          index: i,
          random: Math.random(),
          metadata: { processed: false }
        })
      })
    }

    await db
      .insertInto('large_dataset')
      .values(batchData)
      .execute()

    if ((batch + 1) % 5 === 0) {
      console.log(`  ✓ Inserted ${((batch + 1) * batchSize).toLocaleString()} records`)
    }
  }

  console.log(`✅ Dataset created with ${totalRecords.toLocaleString()} records`)
}

async function demonstrateBasicStreaming(db: Kysely<StreamingSchema>) {
  console.log('\n🌊 Basic Streaming Demo\n')

  const query = db
    .selectFrom('large_dataset')
    .selectAll()
    .where('value', '>', 500)
    .orderBy('timestamp')

  // Count total records first
  const totalCount = await db
    .selectFrom('large_dataset')
    .select(eb => eb.fn.count('id').as('count'))
    .where('value', '>', 500)
    .executeTakeFirstOrThrow()

  const total = Number((totalCount as any)['count'])
  console.log(`📊 Streaming ${total} records (value > 500)...`)

  const stream = new QueryResultStream(db, query, 500) // Smaller batches for demo
  let processedCount = 0

  return new Promise((resolve, reject) => {
    stream.on('data', (record) => {
      processedCount++
      
      // Log progress
      if (processedCount % 1000 === 0) {
        console.log(`📈 Streamed ${processedCount} records...`)
      }
    })

    stream.on('end', () => {
      console.log(`✅ Streaming complete. Processed ${processedCount} records`)
      resolve(undefined)
    })

    stream.on('error', reject)
  })
}

async function demonstratePipelineProcessing(db: Kysely<StreamingSchema>) {
  console.log('\n🔄 Pipeline Processing Demo\n')

  const sourceQuery = db
    .selectFrom('large_dataset')
    .selectAll()
    .where('category', '=', 'electronics')
    .orderBy('id')

  const sourceStream = new QueryResultStream(db, sourceQuery, 1000)
  const transformStream = new DataProcessorTransform()
  const outputStream = new CsvWriterStream('./processed_electronics.csv')

  console.log('🚀 Starting pipeline: Query → Transform → CSV Export')

  try {
    await pipelineAsync(
      sourceStream,
      transformStream,
      outputStream
    )

    // Check the output file
    const stats = await fs.stat('./processed_electronics.csv')
    console.log(`✅ Pipeline complete! Output file size: ${(stats.size / 1024).toFixed(1)} KB`)

    // Clean up
    await fs.unlink('./processed_electronics.csv')

  } catch (error) {
    console.error('❌ Pipeline failed:', error)
  }
}

async function demonstrateAggregationStreaming(db: Kysely<StreamingSchema>) {
  console.log('\n📊 Aggregation Streaming Demo\n')

  // Create aggregation target table
  await db.schema
    .createTable('processed_data')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('category', 'varchar(50)')
    .addColumn('total_value', sql`decimal(15,2)`)
    .addColumn('avg_value', sql`decimal(10,2)`)
    .addColumn('count', 'integer')
    .execute()

  // Stream aggregated data in chunks
  const categories = await db
    .selectFrom('large_dataset')
    .select('category')
    .distinct()
    .execute()

  console.log(`🔢 Processing aggregations for ${categories.length} categories...`)

  let aggId = 1
  for (const { category } of categories) {
    const aggregation = await db
      .selectFrom('large_dataset')
      .select([
        sql`${category}`.as('category'),
        eb => eb.fn.sum('value').as('total_value'),
        eb => eb.fn.avg('value').as('avg_value'),
        eb => eb.fn.count('id').as('count')
      ])
      .where('category', '=', category)
      .executeTakeFirstOrThrow()

    await db
      .insertInto('processed_data')
      .values({
        id: aggId++,
        category: category,
        total_value: Number(aggregation.total_value),
        avg_value: Number(aggregation.avg_value),
        count: Number(aggregation.count)
      })
      .execute()

    console.log(`  ✓ ${category}: ${aggregation.count} records, avg value: $${Number(aggregation.avg_value).toFixed(2)}`)
  }

  // Display results
  const results = await db
    .selectFrom('processed_data')
    .selectAll()
    .orderBy('total_value', 'desc')
    .execute()

  console.log('\n📋 Aggregation Results:')
  console.table(results)
}

async function demonstrateMemoryEfficientExport(db: Kysely<StreamingSchema>) {
  console.log('\n💾 Memory-Efficient Export Demo\n')

  // Export large dataset to Parquet with streaming
  console.log('📤 Exporting to Parquet format...')
  
  await sql`
    COPY (
      SELECT 
        category,
        COUNT(*) as record_count,
        SUM(value) as total_value,
        AVG(value) as avg_value,
        MIN(value) as min_value,
        MAX(value) as max_value,
        DATE_TRUNC('month', timestamp) as month
      FROM large_dataset 
      GROUP BY category, DATE_TRUNC('month', timestamp)
      ORDER BY category, month
    ) TO './dataset_summary.parquet' (FORMAT 'parquet')
  `.execute(db)

  // Verify the export
  const parquetStats = await sql`
    SELECT COUNT(*) as row_count 
    FROM './dataset_summary.parquet'
  `.execute(db)

  console.log(`✅ Exported ${(parquetStats.rows[0] as any).row_count} summary records to Parquet`)

  // Export to CSV with compression
  console.log('📤 Exporting to compressed CSV...')
  
  await sql`
    COPY (
      SELECT * FROM large_dataset 
      WHERE value > 100 
      ORDER BY timestamp
    ) TO './filtered_dataset.csv.gz' 
    (FORMAT 'csv', HEADER true, COMPRESSION 'gzip')
  `.execute(db)

  console.log('✅ Exported filtered data to compressed CSV')

  // Clean up export files
  const filesToClean = ['./dataset_summary.parquet', './filtered_dataset.csv.gz']
  for (const file of filesToClean) {
    try {
      await fs.unlink(file)
    } catch {
      // Ignore if file doesn't exist
    }
  }
}

async function main() {
  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<StreamingSchema>({
    dialect: new DuckDbDialect({
      database,
      // Optimize for large data processing
      config: {
        threads: 4,
        max_memory: '2GB',
        preserve_insertion_order: false
      }
    }),
  })

  try {
    console.log('🌊 Streaming and Large Data Processing Demo\n')

    // Create large dataset
    await createLargeDataset(db)

    // Demonstrate basic streaming
    await demonstrateBasicStreaming(db)

    // Demonstrate pipeline processing
    await demonstratePipelineProcessing(db)

    // Demonstrate aggregation streaming
    await demonstrateAggregationStreaming(db)

    // Demonstrate memory-efficient export
    await demonstrateMemoryEfficientExport(db)

    console.log('\n✅ All streaming examples completed!')

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
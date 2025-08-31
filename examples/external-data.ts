/**
 * External Data Integration Example
 * 
 * This example shows how to work with CSV, JSON, and Parquet files
 * directly without importing them into DuckDB first.
 */

import { Kysely, sql } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'
import { promises as fs } from 'node:fs'

// Schema for our external data sources
interface ExternalDataSchema {
  users: {
    id: number
    name: string
    email: string
    age: number
    city: string
  }
  orders: {
    id: number
    user_id: number
    product: string
    amount: number
    order_date: string
  }
  products: {
    id: number
    name: string
    category: string
    price: number
  }
}

async function createSampleData() {
  // Create sample CSV file
  const csvData = `id,name,email,age,city
1,John Doe,john@example.com,30,New York
2,Jane Smith,jane@example.com,25,Los Angeles
3,Bob Wilson,bob@example.com,35,Chicago
4,Alice Brown,alice@example.com,28,Houston`

  await fs.writeFile('./sample_users.csv', csvData)

  // Create sample JSON file (newline delimited)
  const jsonData = [
    { id: 1, user_id: 1, product: 'Laptop', amount: 999.99, order_date: '2024-01-15' },
    { id: 2, user_id: 2, product: 'Mouse', amount: 29.99, order_date: '2024-01-16' },
    { id: 3, user_id: 1, product: 'Keyboard', amount: 79.99, order_date: '2024-01-17' },
    { id: 4, user_id: 3, product: 'Monitor', amount: 299.99, order_date: '2024-01-18' },
  ]

  const ndjsonData = jsonData.map(obj => JSON.stringify(obj)).join('\n')
  await fs.writeFile('./sample_orders.ndjson', ndjsonData)

  console.log('✅ Sample data files created')
}

async function main() {
  // Create sample data files
  await createSampleData()

  // Create DuckDB instance
  const database = await DuckDBInstance.create(':memory:')

  // Create Kysely instance with external data mappings
  const db = new Kysely<ExternalDataSchema>({
    dialect: new DuckDbDialect({
      database,
      tableMappings: {
        // Map CSV file as a table
        users: {
          source: './sample_users.csv',
          options: {
            header: true,
            delim: ',',
            auto_detect: true
          }
        },
        
        // Map NDJSON file as a table
        orders: {
          source: './sample_orders.ndjson',
          options: {
            format: 'newline_delimited',
            auto_detect: true
          }
        }
      }
    })
  })

  try {
    console.log('\n📊 External Data Integration Demo\n')

    // 1. Query CSV file directly
    console.log('1. Users from CSV file:')
    const users = await db
      .selectFrom('users')
      .selectAll()
      .execute()
    
    console.table(users)

    // 2. Query JSON file directly
    console.log('\n2. Orders from NDJSON file:')
    const orders = await db
      .selectFrom('orders')
      .selectAll()
      .execute()
    
    console.table(orders)

    // 3. Join external data sources
    console.log('\n3. User orders (joined across files):')
    const userOrders = await db
      .selectFrom('users')
      .innerJoin('orders', 'users.id', 'orders.user_id')
      .select([
        'users.name',
        'users.email',
        'users.city',
        'orders.product',
        'orders.amount',
        'orders.order_date'
      ])
      .execute()
    
    console.table(userOrders)

    // 4. Aggregations across external sources
    console.log('\n4. Order statistics by city:')
    const cityStats = await db
      .selectFrom('users')
      .innerJoin('orders', 'users.id', 'orders.user_id')
      .groupBy('users.city')
      .select([
        'users.city',
        eb => eb.fn.count('orders.id').as('total_orders'),
        eb => eb.fn.sum('orders.amount').as('total_amount'),
        eb => eb.fn.avg('orders.amount').as('avg_order_value')
      ])
      .orderBy('total_amount', 'desc')
      .execute()
    
    console.table(cityStats)

    // 5. Read Parquet files (if available)
    console.log('\n5. Working with Parquet files:')
    
    // Create a temporary Parquet file from our data
    await sql`
      COPY (
        SELECT users.*, orders.product, orders.amount, orders.order_date
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
      ) TO './user_orders.parquet' (FORMAT 'parquet')
    `.execute(db)

    // Now read the Parquet file
    const parquetData = await sql`
      SELECT * FROM './user_orders.parquet'
      WHERE amount > 100
      ORDER BY amount DESC
    `.execute(db)
    
    console.table(parquetData.rows)

    // 6. Multiple file patterns
    console.log('\n6. Reading multiple files with patterns:')
    
    // Create multiple CSV files
    await fs.writeFile('./data_2024_01.csv', 'id,value\n1,100\n2,200')
    await fs.writeFile('./data_2024_02.csv', 'id,value\n3,300\n4,400')
    
    // Read all files matching pattern
    const multiFileData = await sql`
      SELECT * FROM './data_2024_*.csv'
      ORDER BY id
    `.execute(db)
    
    console.table(multiFileData.rows)

    // 7. Advanced CSV options
    console.log('\n7. Advanced CSV reading options:')
    
    // Create CSV with custom delimiter and no header
    const customCsv = `1|John Doe|30
2|Jane Smith|25
3|Bob Wilson|35`
    
    await fs.writeFile('./custom.csv', customCsv)
    
    const customData = await sql`
      SELECT * FROM read_csv('./custom.csv', 
        columns = {
          'id': 'INTEGER',
          'name': 'VARCHAR',
          'age': 'INTEGER'
        },
        delim = '|',
        header = false
      )
    `.execute(db)
    
    console.table(customData.rows)

    // 8. JSON array format
    console.log('\n8. Reading JSON arrays:')
    
    const jsonArray = JSON.stringify([
      { product: 'Laptop', category: 'Electronics', price: 999.99 },
      { product: 'Book', category: 'Education', price: 19.99 },
      { product: 'Coffee', category: 'Food', price: 4.99 }
    ])
    
    await fs.writeFile('./products.json', jsonArray)
    
    const productsFromJson = await sql`
      SELECT * FROM read_json('./products.json', 
        format = 'array',
        auto_detect = true
      )
      WHERE price > 10
    `.execute(db)
    
    console.table(productsFromJson.rows)

    console.log('\n✅ External data integration complete!')

  } finally {
    await db.destroy()
    database.closeSync()
    
    // Cleanup sample files
    const filesToClean = [
      './sample_users.csv',
      './sample_orders.ndjson', 
      './user_orders.parquet',
      './data_2024_01.csv',
      './data_2024_02.csv',
      './custom.csv',
      './products.json'
    ]
    
    for (const file of filesToClean) {
      try {
        await fs.unlink(file)
      } catch {
        // Ignore if file doesn't exist
      }
    }
  }
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { main }
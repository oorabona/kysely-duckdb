/**
 * DuckDB Extensions Example
 * 
 * This example demonstrates the use of JSON, Vector, and Spatial extensions
 * available in DuckDB through kysely-duckdb.
 */

import { Kysely, sql } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect, JsonFunctions, VectorFunctions } from '@oorabona/kysely-duckdb'

// Database schema for extension examples
interface ExtensionSchema {
  documents: {
    id: number
    title: string
    content: string
    metadata: unknown
    embedding: number[]
    location: unknown
  }
  products: {
    id: number
    name: string
    details: unknown
    price: number
  }
  locations: {
    id: number
    name: string
    coordinates: unknown
    properties: unknown
  }
}

async function main() {
  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<ExtensionSchema>({
    dialect: new DuckDbDialect({
      database,
    }),
  })

  try {
    console.log('🧩 DuckDB Extensions Demo\n')

    // Create tables for examples
    await db.schema
      .createTable('documents')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('title', 'varchar(255)')
      .addColumn('content', 'text')
      .addColumn('metadata', 'json')
      .addColumn('embedding', sql`double[]`)
      .addColumn('location', 'json')
      .execute()

    await db.schema
      .createTable('products')
      .addColumn('id', 'integer', col => col.primaryKey())
      .addColumn('name', 'varchar(255)')
      .addColumn('details', 'json')
      .addColumn('price', sql`decimal(10,2)`)
      .execute()

    // Insert sample data
    await db.insertInto('documents').values([
      {
        id: 1,
        title: 'Introduction to Machine Learning',
        content: 'Machine learning is a subset of artificial intelligence...',
        metadata: JSON.stringify({
          author: 'Alice Johnson',
          category: 'Technology',
          tags: ['ML', 'AI', 'Technology'],
          published: '2024-01-15',
          views: 1250,
          rating: 4.8
        }),
        embedding: sql`ARRAY[0.1, 0.8, 0.3, 0.6, 0.2]::double[]`,
        location: JSON.stringify({ lat: 40.7128, lng: -74.0060, city: 'New York' })
      },
      {
        id: 2,
        title: 'Web Development Best Practices',
        content: 'Building modern web applications requires...',
        metadata: JSON.stringify({
          author: 'Bob Smith',
          category: 'Web Development',
          tags: ['HTML', 'CSS', 'JavaScript'],
          published: '2024-02-10',
          views: 890,
          rating: 4.5
        }),
        embedding: sql`ARRAY[0.3, 0.2, 0.9, 0.1, 0.7]::double[]`,
        location: JSON.stringify({ lat: 34.0522, lng: -118.2437, city: 'Los Angeles' })
      },
      {
        id: 3,
        title: 'Database Design Principles',
        content: 'Effective database design is crucial for...',
        metadata: JSON.stringify({
          author: 'Carol Davis',
          category: 'Database',
          tags: ['SQL', 'Database', 'Design'],
          published: '2024-01-20',
          views: 2100,
          rating: 4.9
        }),
        embedding: sql`ARRAY[0.7, 0.4, 0.1, 0.8, 0.5]::double[]`,
        location: JSON.stringify({ lat: 41.8781, lng: -87.6298, city: 'Chicago' })
      }
    ]).execute()

    await db.insertInto('products').values([
      {
        id: 1,
        name: 'Laptop Pro',
        details: JSON.stringify({
          brand: 'TechCorp',
          specs: {
            cpu: 'Intel i7',
            ram: '16GB',
            storage: '512GB SSD'
          },
          features: ['Touch Screen', 'Backlit Keyboard'],
          warranty: { years: 2, type: 'Full' }
        }),
        price: 1299.99
      },
      {
        id: 2,
        name: 'Wireless Headphones',
        details: JSON.stringify({
          brand: 'AudioMax',
          specs: {
            driver: '40mm',
            frequency: '20Hz-20kHz',
            battery: '30 hours'
          },
          features: ['Noise Cancelling', 'Bluetooth 5.0'],
          colors: ['Black', 'White', 'Blue']
        }),
        price: 199.99
      }
    ]).execute()

    // =================
    // JSON EXTENSION
    // =================
    console.log('📄 JSON Extension Examples\n')

    // 1. JSON extraction and filtering
    console.log('1. Extract JSON data and filter:')
    const jsonQuery1 = await db
      .selectFrom('documents')
      .select([
        'title',
        sql`json_extract(metadata, '$.author')`.as('author'),
        sql`json_extract(metadata, '$.category')`.as('category'),
        sql`json_extract(metadata, '$.rating')`.as('rating'),
        sql`json_extract(metadata, '$.views')`.as('views')
      ])
      .where(sql`json_extract(metadata, '$.views')`, '>', 1000)
      .execute()
    
    console.table(jsonQuery1)

    // 2. JSON validation and type checking
    console.log('\n2. JSON validation and types:')
    const jsonQuery2 = await db
      .selectFrom('products')
      .select([
        'name',
        sql`json_valid(details)`.as('is_valid_json'),
        sql`json_type(details)`.as('json_type'),
        sql`json_type(details, '$.specs')`.as('specs_type')
      ])
      .execute()
    
    console.table(jsonQuery2)

    // 3. JSON array operations
    console.log('\n3. JSON array operations:')
    const jsonQuery3 = await db
      .selectFrom('documents')
      .select([
        'title',
        sql`json_array_length(metadata, '$.tags')`.as('tag_count'),
        sql`json_extract(metadata, '$.tags')`.as('tags')
      ])
      .execute()
    
    console.table(jsonQuery3)

    // 4. JSON object manipulation
    console.log('\n4. JSON object keys and structure:')
    const jsonQuery4 = await db
      .selectFrom('products')
      .select([
        'name',
        sql`json_keys(details)`.as('top_level_keys'),
        sql`json_keys(details, '$.specs')`.as('specs_keys'),
        sql`json_structure(details)`.as('structure')
      ])
      .execute()
    
    console.table(jsonQuery4)

    // 5. JSON aggregation
    console.log('\n5. JSON aggregation functions:')
    const jsonQuery5 = await db
      .selectFrom('documents')
      .select((eb) => [
        sql`json_extract(metadata, '$.category')`.as('category'),
        eb.fn.count('id').as('document_count'),
        sql`json_group_array(title)`.as('titles'),
        sql`AVG(CAST(json_extract(metadata, '$.rating') AS DOUBLE))`.as('avg_rating')
      ])
      .groupBy(sql`json_extract(metadata, '$.category')`)
      .execute()
    
    console.table(jsonQuery5)

    // =================
    // VECTOR EXTENSION
    // =================
    console.log('\n\n🔢 Vector Extension Examples\n')

    // 1. Vector similarity search
    console.log('1. Find similar documents using cosine similarity:')
    const queryEmbedding = [0.2, 0.7, 0.4, 0.5, 0.3]
    const queryVecExpr = sql`ARRAY[${sql.raw(queryEmbedding.join(', '))}]::DOUBLE[]`
    
    const vectorQuery1 = await db
      .selectFrom('documents')
      .select([
        'title',
        'embedding',
        VectorFunctions.cosineSimilarity(sql`embedding`, queryVecExpr).as('similarity'),
        VectorFunctions.cosineDistance(sql`embedding`, queryVecExpr).as('distance')
      ])
      .orderBy('similarity', 'desc')
      .execute()
    
    console.table(vectorQuery1)

    // 2. Vector distance calculations
    console.log('\n2. Different distance metrics:')
    const vectorQuery2 = await db
      .selectFrom('documents')
      .select([
        'title',
        VectorFunctions.l2Distance(sql`embedding`, queryVecExpr).as('euclidean_distance'),
        VectorFunctions.l1Distance(sql`embedding`, queryVecExpr).as('manhattan_distance'),
        VectorFunctions.dotProduct(sql`embedding`, queryVecExpr).as('dot_product')
      ])
      .execute()
    
    console.table(vectorQuery2)

    // 3. Vector operations
    console.log('\n3. Vector mathematical operations:')
    const vectorQuery3 = await db
      .selectFrom('documents')
      .select([
        'title',
        VectorFunctions.vectorMagnitude(sql`embedding`).as('magnitude'),
        VectorFunctions.vectorDims(sql`embedding`).as('dimensions'),
        VectorFunctions.normalize(sql`embedding`).as('normalized')
      ])
      .where('id', '=', 1)
      .execute()
    
    console.table(vectorQuery3)

    // 4. Vector arithmetic
    console.log('\n4. Vector arithmetic:')
    const addVector = [0.1, 0.1, 0.1, 0.1, 0.1]
    
    const vectorQuery4 = await db
      .selectFrom('documents')
      .select([
        'title',
        'embedding as original',
        VectorFunctions.vectorAdd(sql`embedding`, sql`ARRAY[${sql.raw(addVector.join(', '))}]::DOUBLE[]`).as('added'),
        VectorFunctions.vectorMultiply(sql`embedding`, 2.0).as('doubled')
      ])
      .where('id', '=', 1)
      .execute()
    
    console.table(vectorQuery4)

    // 5. Create utility vectors
    console.log('\n5. Utility vector functions:')
    const utilityQuery = await sql<{
      zero_vector: number[]
      random_vector: number[]
    }>`SELECT 
        ${VectorFunctions.zeroVector(5)}::DOUBLE[] as zero_vector,
        ${VectorFunctions.randomVector(3)}::DOUBLE[] as random_vector
    `.execute(db)
    
    console.table(utilityQuery.rows)

    // =================
    // ADVANCED EXAMPLES
    // =================
    console.log('\n\n⚡ Advanced Combined Examples\n')

    // 1. Semantic search with metadata filtering
    console.log('1. Semantic search with JSON filtering:')
    const advancedQuery1 = await db
      .selectFrom('documents')
      .select([
        'title',
        sql`json_extract(metadata, '$.author')`.as('author'),
        sql`json_extract(metadata, '$.category')`.as('category'),
        VectorFunctions.cosineSimilarity(sql`embedding`, queryVecExpr).as('similarity')
      ])
      .where(sql`json_extract(metadata, '$.rating')`, '>', 4.7)
      .where(sql`json_contains(metadata, '"Technology"')`, '=', true)
      .orderBy('similarity', 'desc')
      .execute()
    
    console.table(advancedQuery1)

    // 2. Product recommendation based on features and price
    console.log('\n2. Product analysis with JSON and numeric operations:')
    const advancedQuery2 = await db
      .selectFrom('products')
      .select([
        'name',
        'price',
        sql`json_extract(details, '$.brand')`.as('brand'),
        sql`json_array_length(details, '$.features')`.as('feature_count'),
        sql`json_keys(details, '$.specs')`.as('spec_categories'),
        sql`json_exists(details, '$.warranty.years')`.as('has_warranty')
      ])
      .where('price', '<', 500)
      .execute()
    
    console.table(advancedQuery2)

    console.log('\n✅ Extensions demo complete!')

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
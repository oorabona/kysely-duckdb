# Kysely-DuckDB Examples

This directory contains comprehensive examples demonstrating all features of kysely-duckdb. Each example is fully functional and includes detailed comments explaining the concepts and patterns.

## Running Examples

```bash
# Install dependencies
pnpm install

# Run a specific example
pnpm tsx examples/basic-usage.ts

# Run all examples
pnpm run examples:all

# Build all examples (TypeScript check)
pnpm build:examples
```

## Examples Overview

### 🚀 [Basic Usage](./basic-usage.ts)
**What it demonstrates:** Getting started with kysely-duckdb
- Database connection and configuration
- Basic CRUD operations (Create, Read, Update, Delete)
- Schema creation and management
- Simple queries and data manipulation
- Array operations with DuckDB
- Subqueries and joins

**Key concepts:** Essential patterns for everyday database operations

---

### 📁 [External Data](./external-data.ts)
**What it demonstrates:** Working with external files without importing
- CSV file mapping and querying
- JSON file processing with nested data
- Parquet file integration
- Glob patterns for multiple files
- Custom column mappings
- Performance comparison between formats

**Key concepts:** DuckDB's strength in analyzing external data sources directly

---

### 🧩 [Extensions](./extensions.ts)
**What it demonstrates:** DuckDB extensions for specialized data types
- **JSON Extension:** Extraction, validation, and manipulation of JSON data
- **Vector Extension:** Similarity search, distance calculations, vector operations
- **Combined Examples:** Semantic search with metadata filtering

**Key concepts:** Using SQL template literals for extension functions instead of helper classes

---

### 🔄 [Migrations](./migrations.ts)
**What it demonstrates:** Database schema evolution and versioning
- File-based and in-memory migration providers
- SQL and TypeScript migration files
- Migration execution and rollback
- Status tracking and validation
- Migration utilities and generators

**Key concepts:** Professional database schema management with version control

---

### 🔌 [Plugins](./plugins.ts)
**What it demonstrates:** Extending Kysely functionality
- **CaseConverterPlugin:** Automatic camelCase ↔ snake_case conversion
- **Custom Plugins:** Query timing, row counting, security validation
- **Native Logging:** Using Kysely's built-in logging instead of custom plugins
- Plugin chaining and composition

**Key concepts:** Extensible architecture for cross-cutting concerns

---

### 🌊 [Streaming](./streaming.ts)
**What it demonstrates:** Efficient handling of large datasets
- Streaming large result sets with backpressure
- Chunked data processing
- Memory-efficient aggregations
- Export to various formats (Parquet, CSV)
- Progress monitoring and performance metrics

**Key concepts:** Scalable data processing patterns for production workloads

---

### ⚡ [Advanced Queries](./advanced-queries.ts)
**What it demonstrates:** Complex SQL patterns and analytics
- **CTEs (Common Table Expressions):** Complex queries with reusable subqueries
- **Window Functions:** Ranking, running totals, moving averages
- **Recursive Queries:** Hierarchical data processing
- **Advanced Joins:** Self-joins, correlated subqueries
- **Analytics Functions:** Statistical analysis, time series, business intelligence

**Key concepts:** OLAP patterns and analytical SQL techniques

---

### � [SERIAL-like Auto-Increment](./serial.ts)
**What it demonstrates:** Emulating SERIAL/IDENTITY with DuckDB
- Creating a SEQUENCE
- Using DEFAULT nextval('sequence') on primary keys
- Inserting without specifying ids

**Key concepts:** DuckDB does not have auto_increment, use next_val

---

### �📈 [Performance](./performance.ts)
**What it demonstrates:** Optimization and monitoring
- Performance monitoring and profiling
- Query optimization techniques
- Memory management strategies
- Indexing best practices
- Benchmarking different approaches

**Key concepts:** Production-ready performance optimization

---

### 📝 [Native Logging](./native-logging.ts)
**What it demonstrates:** Built-in query logging and monitoring
- Simple query logging
- Performance monitoring with timing
- Custom log formatting
- Production-ready logging patterns

**Key concepts:** Using Kysely's native logging instead of custom plugins

## Common Patterns

### Date Handling
```typescript
// ✅ Use ISO date strings (recommended)
hire_date: '2024-01-15'

// ✅ Use SQL casting for explicit types
hire_date: sql`'2024-01-15'::date`

// ❌ Avoid JavaScript Date objects (causes DuckDB type inference issues)
hire_date: new Date('2024-01-15')
```

### Null Values
```typescript
// ✅ Use typed nulls for DuckDB
manager_id: sql`NULL::integer`

// ❌ Avoid plain null in complex queries
manager_id: null
```

### Extension Functions
```typescript
// ✅ Use sql template literals (current approach)
sql`json_extract(metadata, '$.author')`.as('author')
sql`array_cosine_similarity(embedding, ${queryVector}::DOUBLE[])`.as('similarity')

// ❌ Avoid helper function classes (removed)
JsonFunctions.jsonExtract(...)
VectorFunctions.cosineSimilarity(...)
```

### Plugin Usage
```typescript
// ✅ Use Kysely's native logging
const db = new Kysely({
  dialect: new DuckDbDialect({ database }),
  log: (event) => console.log(event.query.sql)
})

// ❌ Avoid custom logger plugins (removed for native approach)
plugins: [new LoggerPlugin()]
```

## DuckDB specifics that matter here

- Foreign keys: ON DELETE CASCADE is not supported. Use plain REFERENCES and handle deletions in application logic.
- Primary keys: INTEGER PRIMARY KEY is not auto-generated. Insert explicit ids (as done in examples) or manage sequences in your app.
- Arrays:
  - Define columns via raw type: addColumn('tags', sql`VARCHAR[]`).
  - Insert with ARRAY[...] and casts when needed, e.g. sql`ARRAY['a','b']::VARCHAR[]`.
  - For defaults, prefer typed literals: defaultTo(sql`[]::VARCHAR[]`).
- JSON:
  - Default values should be typed: defaultTo(sql`'{}'::JSON`).
  - Extract with json_extract; for filtering/sorting, cast to the right type if needed.
- Dates/Timestamps:
  - Prefer ISO strings ('YYYY-MM-DD') or explicit casts like sql`'2024-01-15'::date`.
  - Avoid passing new Date(...) directly to inserts; casting avoids binder ambiguities.
- UUID results:
  - By default, result rows keep native DuckDB UUID values.
  - To stringify automatically: new DuckDbDialect({ database, uuidAsString: true }).
- External data:
  - Call dialect.setupTableMappings(db) before querying external CSV/JSON/Parquet views.
- Streaming:
  - Results are read in chunks using DuckDB's streaming API; design pipelines without relying on identity/trigger side-effects.
- Migrations:
  - File-based generator uses strictly increasing timestamps for deterministic order.
  - TS migrations import sql from 'kysely' and avoid ON DELETE CASCADE; array/JSON defaults are typed as above.

## File Organization

- **Each example is self-contained** with its own data setup and cleanup
- **Comprehensive comments** explain every concept and pattern
- **Error handling** demonstrates best practices
- **TypeScript types** show proper interface definitions
- **Performance considerations** included where relevant

## Best Practices Demonstrated

1. **Type Safety:** Proper TypeScript interfaces for database schemas
2. **Error Handling:** Graceful handling of database operations
3. **Resource Management:** Proper connection cleanup and memory management
4. **Performance:** Efficient queries and data processing patterns
5. **Maintainability:** Clear code structure and documentation
6. **Production Ready:** Real-world patterns and configurations

## Architecture Decisions

These examples follow the architectural decisions documented in [ARCHITECTURAL_DECISION.md](../ARCHITECTURAL_DECISION.md):

- **Standard Kysely Approach:** No automatic type conversion, developers handle data types explicitly
- **Native DuckDB Features:** Direct use of DuckDB's native capabilities
- **SQL Template Literals:** Direct SQL usage instead of abstraction layers
- **Kysely Integration:** Leveraging Kysely's built-in features over custom implementations

## Next Steps

After exploring these examples:

1. **Choose relevant patterns** for your use case
2. **Adapt the examples** to your data schema
3. **Combine multiple patterns** for complex applications
4. **Review the main documentation** in the root README.md
5. **Check the test suite** for additional patterns and edge cases
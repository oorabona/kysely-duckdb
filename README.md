# kysely-duckdb

[![CI](https://github.com/oorabona/kysely-duckdb/actions/workflows/ci.yml/badge.svg)](https://github.com/oorabona/kysely-duckdb/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/%40oorabona%2Fkysely-duckdb.svg)](https://www.npmjs.com/package/@oorabona/kysely-duckdb)
[![Coverage](https://img.shields.io/badge/Coverage-100%25-brightgreen.svg)](https://github.com/oorabona/kysely-duckdb)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9+-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modern DuckDB dialect for [Kysely](https://kysely.dev/) built with TypeScript and the latest [@duckdb/node-api](https://www.npmjs.com/package/@duckdb/node-api).

## Features

- 🦆 **Modern DuckDB Support**: Uses the latest `@duckdb/node-api` package (not the deprecated `duckdb` package)
- 🔒 **Type Safety**: Full TypeScript support with comprehensive type definitions and strict typing (no `any` types)
- ✅ **Input Validation**: Built-in Valibot schemas for runtime configuration validation
- 🚀 **Performance**: Optimized for speed with an async engine and transaction-scoped connections (no pooling by default)
- 🧩 **Extensions**: Built-in support for DuckDB extensions (JSON, Vector, Spatial)
- 🔄 **Migrations**: Complete migration system with SQL and TypeScript support
- 🔌 **Plugins**: Extensible plugin system (includes case conversion). Use Kysely's native logging.
- 🌊 **Streaming**: Async-iterable streaming of large result sets in chunks
- 📦 **External Data**: Direct querying of CSV, JSON, Parquet files without imports
- 🧪 **Production Ready**: 100% test coverage with comprehensive integration tests
- 📈 **Analytics Focused**: Optimized for OLAP workloads, data analytics, and ETL processes
- 🪶 **Lightweight**: Minimal bundle impact with tree-shakeable Valibot validation (1-2 KB)
- 📊 **Observability**: Built-in performance monitor, query metrics formatter, and native logging helpers

## Installation

```bash
# Using npm
npm install kysely @oorabona/kysely-duckdb @duckdb/node-api

# Using pnpm
pnpm add kysely @oorabona/kysely-duckdb @duckdb/node-api

# Using yarn
yarn add kysely @oorabona/kysely-duckdb @duckdb/node-api
```

## Quick Start

```typescript
import { Kysely, sql } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

interface DatabaseSchema {
  users: {
    id: number
    name: string
    email: string
    created_at: Date
  }
}

// Create database connection
const database = await DuckDBInstance.create(':memory:') // or './my-database.db'

// Create Kysely instance
const db = new Kysely<DatabaseSchema>({
  dialect: new DuckDbDialect({
    database,
    // UUIDs are preserved as native DuckDB values by default. Set to true to get strings.
    // uuidAsString: true,
  }),
})

// Create table and insert data
await db.schema
  .createTable('users')
  .addColumn('id', 'integer', col => col.primaryKey())
  .addColumn('name', 'varchar(255)', col => col.notNull())
  .addColumn('email', 'varchar(255)', col => col.unique())
  .addColumn('created_at', 'timestamp', col => col.defaultTo(sql`CURRENT_TIMESTAMP`))
  .execute()

// Use it!
const users = await db
  .selectFrom('users')
  .selectAll()
  .where('name', 'like', '%john%')
  .execute()
```

## Working with External Data

DuckDB excels at working with external data sources. You can easily map CSV, JSON, and Parquet files:

```typescript
const dialect = new DuckDbDialect({
  database,
  tableMappings: {
    // Simple CSV mapping
    users: { source: './users.csv', options: { header: true } },

    // JSON with custom options
    events: {
      source: './events.json',
      options: {
        format: 'newline_delimited',
        header: true,
      },
    },

    // Parquet files
    sales: { source: './sales.parquet' },

    // Multiple files with glob patterns
    logs: { source: './logs/*.csv', options: { header: true, delim: ',' } },
  },
})

const db = new Kysely({ dialect })

// Apply view mappings once after creating Kysely
await dialect.setupTableMappings(db)

// Now you can query external files as tables
const userEvents = await db
  .selectFrom('events')
  .innerJoin('users', 'users.id', 'events.user_id')
  .selectAll()
  .execute()
```

## Extensions

### Spatial Extension

```typescript
import { SpatialFunctions, loadSpatialExtension } from '@oorabona/kysely-duckdb'

// Load the spatial extension
await loadSpatialExtension(db)

// Use spatial functions
const nearbyPlaces = await db
  .selectFrom('places')
  .select(['name', 'location'])
  .where(
    eb => SpatialFunctions.stWithin(
      eb.ref('location'),
      SpatialFunctions.stBuffer(
        SpatialFunctions.stPoint(latitude, longitude),
        1000 // 1km buffer
      )
    ),
    '=',
    true
  )
  .execute()
```

### JSON Extension

```typescript
import { loadJsonExtension, jsonExtract, jsonContains, jsonArrayLength } from '@oorabona/kysely-duckdb'
import { sql } from 'kysely'

// Load JSON extension (no-op on newer DuckDB versions where it is built-in)
await loadJsonExtension(db)

const users = await db
  .selectFrom('users')
  .select([
    'id',
    'name',
    jsonExtract(sql.ref('profile'), '$.age').as('age'),
    jsonExtract(sql.ref('profile'), '$.city').as('city'),
    jsonArrayLength(sql.ref('profile'), '$.items').as('items_count'),
  ])
  .where(jsonContains(sql.ref('profile'), sql.lit('true'), '$.active'), '=', true)
  .execute()

// Note: DuckDB json_contains supports two arguments (haystack, needle).
// This dialect's helper adds an optional path: when provided, we apply json_contains
// to json_extract(haystack, path) and the needle under the hood.
// Why keep loadJsonExtension? On many Node builds, JSON is compiled in and available,
// so calling it is effectively a no-op. But on other environments or future versions,
// the JSON extension may require an explicit INSTALL/LOAD. This helper makes your code
// portable and explicit while remaining safe and idempotent.
```

### Vector Extension

```typescript
import { sql } from 'kysely'
import { cosineSimilarity, loadVectorExtensions } from '@oorabona/kysely-duckdb'

// Load vector-related extensions (e.g., VSS)
await loadVectorExtensions(db)

// Find similar embeddings
const queryEmbedding = [0.1, 0.8, 0.3, 0.6, 0.2]
const similar = await db
  .selectFrom('documents')
  .select([
    'title',
    'content',
    cosineSimilarity(sql.ref('embedding'), sql.val(queryEmbedding)).as('similarity'),
  ])
  .orderBy('similarity', 'desc')
  .limit(10)
  .execute()
```

## Plugins

### Case Converter Plugin

Automatically converts between camelCase (TypeScript) and snake_case (SQL):

```typescript
import { CaseConverterPlugin } from '@oorabona/kysely-duckdb'

const db = new Kysely({
  dialect: new DuckDbDialect({ database }),
  plugins: [
    new CaseConverterPlugin({
      toSnakeCase: true,  // Convert to snake_case in queries
      toCamelCase: true,  // Convert to camelCase in results
    })
  ]
})

// Write camelCase in TypeScript
const users = await db
  .selectFrom('users')
  .select(['firstName', 'lastName', 'createdAt']) // Becomes first_name, last_name, created_at
  .execute()

// Get camelCase results
console.log(users[0].firstName) // Converted from first_name
```

### Native Logging

Use Kysely's built-in logging for query monitoring:

```typescript
const db = new Kysely({
  dialect: new DuckDbDialect({ database }),
  log: (event) => {
    const { query, queryDurationMillis } = event
    const duration = queryDurationMillis || 0
    const performanceIcon = duration > 1000 ? '⚠️  SLOW' : '✅'
    console.log(`${performanceIcon} ${duration}ms: ${query.sql.replace(/\s+/g, ' ').trim()}`)
  }
})
```

## Performance Monitoring

The driver automatically streams timing data into a global performance monitor that you can interrogate at runtime. You can also create your own monitors for targeted dashboards.

```typescript
import {
  globalPerformanceMonitor,
  PerformanceMonitor,
  formatPerformanceStats,
} from '@oorabona/kysely-duckdb'

// Inspect the global metrics captured by the dialect
const stats = globalPerformanceMonitor.getStats()
console.log(formatPerformanceStats(stats))

// Or plug a custom monitor into your app lifecycle
const monitor = new PerformanceMonitor({ slowQueryThreshold: 500 })

function recordQuery(sql: string, duration: number, rows: number): void {
  monitor.recordQuery({ sql, duration, rowCount: rows })
}

// Later, expose metrics in observability pipelines
app.get('/metrics/db', (_req, res) => {
  res.type('text/plain').send(formatPerformanceStats(monitor.getStats()))
})
```

The monitor keeps a rolling buffer of queries, highlights slow statements, tracks error rate, and also exposes helpers like `getRecentQueries()` for ad-hoc diagnostics.

## Migrations

```typescript
import { Migrator } from 'kysely'
import { FileMigrationProvider } from '@oorabona/kysely-duckdb'

const migrator = new Migrator({
  db,
  provider: new FileMigrationProvider('./migrations'),
})

// Run migrations
const result = await migrator.migrateToLatest()
console.log(`Executed ${result.results?.length} migrations`)

// Get status
const migrations = await migrator.getMigrations()
const pending = migrations.filter(m => !m.executedAt)
console.log(`${pending.length} pending migrations`)
```

### Migration Files

Create migration files in your migrations folder:

```typescript
// migrations/001_create_users.ts
import type { Kysely } from 'kysely'

export async function up(db: Kysely<any>): Promise<void> {
  await db.schema
    .createTable('users')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(255)', col => col.notNull())
    .addColumn('email', 'varchar(255)', col => col.unique())
    .addColumn('created_at', 'timestamp', col => col.defaultTo('CURRENT_TIMESTAMP'))
    .execute()
}

export async function down(db: Kysely<any>): Promise<void> {
  await db.schema.dropTable('users').execute()
}
```

Or use SQL files:

```sql
-- migrations/001_create_users.sql

-- migrate:up
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- migrate:down
DROP TABLE users;
```

## API Reference

### DuckDbDialect

```typescript
import type { DuckDBInstance } from '@duckdb/node-api'

interface DuckDbDialectConfig {
  database: DuckDBInstance
  tableMappings?: Record<string, string | TableMapping>
  config?: Record<string, unknown>
  /**
   * UUID conversion behavior. When false (default), DuckDB UUID values are returned as
   * native DuckDB runtime objects. When true, UUID columns are converted to strings.
   */
  uuidAsString?: boolean
}
```

### Available Extensions

- `loadSpatialExtension()` - Load DuckDB spatial extension
- JSON helpers: `jsonExtract`, `jsonExtractString`, `jsonExists`, `jsonValue`, `jsonType`, `jsonValid`, `jsonArrayLength`, `jsonContains`, `jsonKeys`, `jsonStructure`, `jsonTransform`, `jsonTransformStrict`, `json`, aggregates (`jsonGroupArray`, `jsonGroupObject`, `jsonGroupStructure`, `jsonMerge`)
- Vector helpers via `sql` template literals or exported functions

### Migration Utilities

- `FileMigrationProvider` - File-based migration provider
- `InMemoryMigrationProvider` - In-memory provider for testing
- Use Kysely's built-in `Migrator` class

## Configuration

### Advanced DuckDB Configuration

```typescript
const db = new Kysely({
  dialect: new DuckDbDialect({
    database,
    // Preserve database-native types by default. To always get UUIDs as strings, set:
    // uuidAsString: true,
    config: {
      // Performance settings
      threads: 4,
      max_memory: '2GB',
      
      // Enable extensions
      enable_external_access: true,
      allow_unsigned_extensions: true,
      
      // Optimization settings
      force_compression: 'zstd',
      preserve_insertion_order: false,
    }
  }),
})

```

### UUID handling

- By default, kysely-duckdb preserves DuckDB-native UUID values in result rows. This keeps raw DB types intact.
- If your app prefers plain string UUIDs, enable conversion:

```ts
import { Kysely } from 'kysely'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

const db = new Kysely({
  dialect: new DuckDbDialect({ database, uuidAsString: true }),
})

## Project Structure

| Path | Purpose |
| --- | --- |
| `src/dialect` | Core dialect implementation (adapter, driver, query compiler, introspector) plus helpers for external table mappings and extension loading. |
| `src/extensions` | Tree-shakeable helpers for JSON, spatial, and vector extensions, including idempotent loaders. |
| `src/internal` | Cross-cutting utilities such as the performance monitor and environment/version checks. |
| `src/migrations` | File-based migration providers and utilities shared by TypeScript and SQL workflows. |
| `src/plugins` | First-class Kysely plugins (currently a case-converter for camelCase ⟷ snake_case bridging). |
| `src/types` | Strongly-typed DuckDB data model definitions and dialect configuration contracts. |
| `src/validation` | Valibot schemas for runtime validation of dialect configuration and table mappings. |
| `examples/` | End-to-end runnable examples that mirror the sections of this guide (basic usage, streaming, performance, monitoring, etc.). |
| `tests/integration` | Coverage-oriented suites exercising complex data types, migrations, and dialect behaviour against real DuckDB engines. |
| `tests/unit` | Fast feedback for adapters, providers, and helpers (including failure modes and UUID conversions). |
| `docs/` | Supplemental architectural notes, ADRs, and deep dives into release workflows. |

```

Notes:
- Conversion applies only to UUID-typed columns in results. Parameters can be strings or UUID expressions.
- The underlying DuckDB UUID runtime class may vary by environment; conversion uses duck-typing with fallbacks.

### Configuration Validation

Runtime validation helps catch misconfigured external table mappings or unsupported options before queries execute. The package ships Valibot schemas that mirror the dialect configuration.

```typescript
import { parse } from 'valibot'
import { ConnectionConfigSchema } from '@oorabona/kysely-duckdb'

const config = parse(ConnectionConfigSchema, {
  uuidAsString: true,
  tableMappings: {
    users: { source: './data/users.parquet' },
    logs: './data/logs/*.csv',
  },
})

const db = new Kysely({
  dialect: new DuckDbDialect({
    database,
    ...config,
  }),
})
```

For finer control you can also import `TableMappingSchema` or `LoggerConfigSchema` and embed them inside your own configuration layers.

## Performance Tips

- **Use column-store**: DuckDB is optimized for analytical queries
- **Batch operations**: Group INSERTs for better performance
- **File formats**: Prefer Parquet over CSV for large datasets
- **Memory management**: Set appropriate `max_memory` limits
- **Indexing**: Use appropriate indexes for frequent WHERE clauses

## Troubleshooting

### Common Issues

**Import errors with @duckdb/node-api:**
```bash
# Make sure you're using the correct import
import { DuckDBInstance } from '@duckdb/node-api'
# NOT: import Database from 'duckdb'
```

**Extension not found:**
```bash
# Install required extensions
INSTALL spatial;
LOAD spatial;
```

**Memory issues with large datasets:**
```typescript
// Increase memory limit
const db = new Kysely({
  dialect: new DuckDbDialect({
    database,
    config: { max_memory: '8GB' }
  })
})
```

## Examples

See the [examples](./examples) directory for complete working examples:
- [Basic Usage](./examples/basic-usage.ts) - Getting started with kysely-duckdb
- [External Data](./examples/external-data.ts) - Working with CSV, JSON, Parquet files
- [Extensions](./examples/extensions.ts) - JSON, Vector, and Spatial functions
- [Migrations](./examples/migrations.ts) - Database schema migrations
- [Plugins](./examples/plugins.ts) - Case conversion and custom plugins
- [Streaming](./examples/streaming.ts) - Handling large datasets efficiently
- [Advanced Queries](./examples/advanced-queries.ts) - CTEs, window functions, analytics
- [Performance](./examples/performance.ts) - Optimization and monitoring
- [Performance Monitoring](./examples/performance-monitoring.ts) - Collecting query metrics and exporting stats
- [Native Logging](./examples/native-logging.ts) - Built-in query logging
- [Serial Support](./examples/serial.ts) - Working with auto-incrementing identifiers

## Security

kysely-duckdb prioritizes security with multiple layers of protection:

### SQL Injection Protection

**Built-in Protection**: Kysely uses prepared statements with parameter binding, providing native protection against SQL injection attacks.

```typescript
// ✅ SAFE: Parameters are automatically bound
const result = await db
  .selectFrom('users')
  .where('username', '=', userInput) // Safely parameterized
  .execute()

// ✅ SAFE: Template literals with sql`` also use parameters
const query = sql`SELECT * FROM users WHERE id = ${userId}`
```

**Best Practices**:
- Always use Kysely's query builder for user-controlled data
- Use `sql.ref()` for dynamic identifiers (table/column names)
- Avoid string concatenation for building queries

### Input Validation

**Runtime Validation**: Built-in Valibot schemas validate configuration at runtime:

```typescript
import { parse } from 'valibot'
import { ConnectionConfigSchema } from '@oorabona/kysely-duckdb'

// Validate configuration before use
const config = parse(ConnectionConfigSchema, {
  uuidAsString: true,
  tableMappings: {
    users: './data/users.csv'
  }
})
```

**Path Security**: When using table mappings with external files:

```typescript
// ⚠️ Application should validate file paths
function validatePath(path: string): boolean {
  // No parent directory traversal
  if (path.includes('..')) return false

  // Must be in allowed directory
  const allowedDir = '/app/data'
  if (!path.startsWith(allowedDir)) return false

  return true
}
```

### Security Testing

Comprehensive security test suite covering:
- SQL injection attempts (parameterized queries, UNION attacks, stacked queries)
- Path traversal prevention
- Unicode and special character handling
- XSS prevention in stored data
- Large input handling

See `tests/security/` for detailed security test documentation.

### Security Reporting

If you discover a security vulnerability, create a private security advisory on GitHub.

## Requirements

- Node.js 20.0.0 or higher
- TypeScript 5.9 or higher (for development)
- DuckDB 1.1.0 or higher (automatically installed with @duckdb/node-api)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

### Releases & Changelog

Versioning is automated with [`@oorabona/release-it-preset`](https://github.com/oorabona/release-it-preset). The recommended flow is:

1. Run `pnpm release` (dry-runs by default) or use the *Release* GitHub Action to pick the increment.
2. Review the generated changelog and dist artifacts.
3. Approve the publish workflow (`Publish`), which tags, updates the GitHub release, and publishes to npm with provenance enabled.

For exceptional cases see the additional workflows (`Hotfix`, `Retry Publish`, `Republish`) under `.github/workflows/`.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release notes.
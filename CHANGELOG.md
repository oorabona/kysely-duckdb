# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.3.1] - 2025-09-01

- refactor: improve regex for extracting changelog entries to handle subsections and edge cases
- release-it should now be 100% working

## [v0.3.0] - 2025-08-31

### Added
- UUID handling toggle: `uuidAsString` (default: false) to control whether UUIDs are returned as native DuckDB runtime objects or stringified
- JSON helpers improvements:
  - `jsonContains(haystack, needle, path?)` with path support via `json_extract`
  - `jsonArrayLength(json, path?)` and `jsonKeys(json, path?)` path overloads
- Resilient `loadJsonExtension(db)` utility that works with both Kysely instances and generic DBs
  - Tries `LOAD json` first; on failure, falls back to `INSTALL json` (best-effort) then `LOAD json`
  - When `LOAD` succeeds, attempts a best-effort `INSTALL` (ignored if unsupported/already available)
- Strict TypeScript config for tests/examples and additional micro-tests
- 100% test coverage across statements, branches, functions, and lines

### Changed
- README updates:
  - Documented `uuidAsString` behavior and rationale
  - Modernized JSON examples using helpers; clarified `jsonContains` path behavior
  - Clarified why `loadJsonExtension` remains useful; updated coverage badge to 100%
- `loadJsonExtension` behavior refined: `LOAD`-first strategy with `INSTALL`+`LOAD` fallback, and execution path selection for Kysely vs non-Kysely DBs
- Driver result processing hardened around JSON parsing, LIST→array conversion, and optional UUID stringification

### Fixed
- JSON integration aligned with DuckDB function signatures (no unsupported 3-arg `json_contains`), with helper doing path extraction internally
- Tests stabilized under strict TS (index access guards, narrowed errors, ColumnType defaults)
- `loadJsonExtension` no longer throws on non-Kysely DB mocks; executes raw SQL strings when needed

## [v0.2.0] - Unreleased

### Added
- 🎯 **Complete rewrite** using modern `@duckdb/node-api`
- 🧩 **Full extension support** for JSON, Vector, and Spatial operations
- 🔄 **Comprehensive migration system** with SQL and TypeScript support
- 🔌 **Plugin ecosystem** including CaseConverter and Logger plugins
- 📦 **External data integration** for CSV, JSON, and Parquet files
- 🌊 **Streaming support** for large result sets
- 🧪 **Production-ready** with extensive test coverage
- 📈 **Analytics optimization** for OLAP workloads

### Performance Improvements
- ⚡ Native Promise support without callback overhead
- 🚀 Optimized connection pooling
- 💾 Efficient memory management
- 📊 Batch operation optimizations

### Developer Experience
- 🔒 **Full TypeScript support** with comprehensive type definitions
- 📚 **Extensive documentation** with working examples
- 🧪 **99.73% test coverage** with integration tests
- 🔧 **Developer tools** including linting and formatting
- 📖 **Complete API reference** with JSDoc comments

### Extension Support
- **JSON Extension**: Complete JSON manipulation and querying
  - `JsonFunctions.jsonExtract()` - Extract values from JSON
  - `JsonFunctions.jsonValid()` - Validate JSON strings
  - `JsonFunctions.jsonType()` - Get JSON value types
  - `JsonFunctions.jsonKeys()` - Extract object keys
  - `JsonFunctions.jsonArrayLength()` - Get array length
  - `JsonReader.readJson()` - Read JSON files directly

- **Vector Extension**: Vector operations and similarity search
  - `VectorFunctions.cosineSimilarity()` - Cosine similarity calculation
  - `VectorFunctions.l2Distance()` - Euclidean distance
  - `VectorFunctions.dotProduct()` - Vector dot product
  - `VectorFunctions.normalize()` - Vector normalization
  - `knnCosine()`, `knnL2()` - K-nearest neighbor search
  - `similaritySearch()`, `radiusSearch()` - Advanced search functions

- **Spatial Extension**: Geospatial operations and analysis
  - `SpatialFunctions.stPoint()` - Create points
  - `SpatialFunctions.stDistance()` - Calculate distances
  - `SpatialFunctions.stWithin()` - Spatial containment
  - `SpatialFunctions.stBuffer()` - Create buffers
  - `SpatialFunctions.stIntersects()` - Intersection testing

### Migration System
- **File-based migrations** with automatic discovery
- **SQL migrations** with up/down support using comments
- **TypeScript migrations** with full type safety
- **Migration utilities** for generation and management
- **Rollback support** with comprehensive error handling

### Plugin System
- **CaseConverterPlugin**: Automatic camelCase ↔ snake_case conversion
- **LoggerPlugin**: Query logging with performance metrics
- **PerformanceLoggerPlugin**: Advanced performance monitoring
- **Extensible architecture** for custom plugins

### External Data Sources
- **Direct CSV querying** without import requirements
- **JSON file integration** with schema inference
- **Parquet support** for analytical workloads
- **Glob patterns** for multiple file processing
- **Custom options** for file format handling

## [v0.1.0] - Initial Release

### Added
- Basic DuckDB dialect for Kysely using new DuckDB NEO (@duckdb/node-api)
- Core query compilation
- Transaction support
- Initial TypeScript types

---

## Support

- 📖 [Documentation](README.md)
- 🐛 [Issues](https://github.com/oorabona/kysely-duckdb/issues)
- 💬 [Discussions](https://github.com/oorabona/kysely-duckdb/discussions)

---

*For more details, see the [full documentation](README.md) and [examples](examples/).*
[Unreleased]: git+https://github.com/oorabona/kysely-duckdb.git/compare/v0.3.1...HEAD
[v0.3.0]: git+https://github.com/oorabona/kysely-duckdb.git/releases/tag/v0.3.0
[v0.3.1]: git+https://github.com/oorabona/kysely-duckdb.git/releases/tag/v0.3.1
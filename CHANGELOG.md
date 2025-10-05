# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.5.0] - 2025-10-05

### Added
- **Input validation with Valibot**: Configuration validation schemas for enhanced type safety and runtime validation
- **Strict TypeScript types**: Replaced 53 occurrences of `any` with proper types for improved type safety
- **Externalized logger configuration**: `createLogger()` function with configurable debug mode and prefix
- **Comprehensive security test suite**: 39 new security tests covering SQL injection, path traversal, and edge cases
- **Security documentation**: Complete security section in README with best practices and examples

### Changed
- **Logger improvement**: Configuration now externalized with `LoggerOptions` interface supporting `debugEnabled` and `prefix` options
- **Type safety**: Introduced `DuckDBUUIDConstructor`, `DuckDBUUIDObject`, `DuckDBColumnType`, and `LoggerArgs` types
- **Bundle size optimization**: Added Valibot (1-2 KB) instead of heavier alternatives like Zod (14 KB)
- **Test suite**: Expanded from 559 to 598 tests while maintaining 100% code coverage

### Security
- **Input validation**: Added validation schemas for connection configuration and table mappings
- **Type safety**: Eliminated unsafe `any` types to prevent runtime type errors
- **Security testing**: Comprehensive test suite documenting SQL injection protection, path validation, and input sanitization
  - SQL injection prevention tests (13 tests)
  - Path traversal and file access security (12 tests)
  - Input validation edge cases (14 tests)

## [v0.4.1] - 2025-10-05

### Changed
- upgrade all dependencies to their latest version ([a97d2b0](https://github.com/oorabona/kysely-duckdb/commit/a97d2b0))
- update hotfix and release workflows to use release-it-preset commands for consistency ([cddbcf8](https://github.com/oorabona/kysely-duckdb/commit/cddbcf8))
- update CI workflows to include validation and caching steps, and add retry publish functionality ([3891bdb](https://github.com/oorabona/kysely-duckdb/commit/3891bdb))
- update CI workflows and add hotfix and release configurations ([ca6dcc4](https://github.com/oorabona/kysely-duckdb/commit/ca6dcc4))
- remove outdated changelog scripts and update release configuration with @oorabona/release-it-preset ([48d34a7](https://github.com/oorabona/kysely-duckdb/commit/48d34a7))
- version bumps of all deps to their latest versions ([763a2ce](https://github.com/oorabona/kysely-duckdb/commit/763a2ce))
- add pnpm workspace configuration to specify only built dependencies ([8ebaab3](https://github.com/oorabona/kysely-duckdb/commit/8ebaab3))
- bump @duckdb/node-api from 1.3.2-alpha.26 to 1.3.3 (#5) (deps) ([19a6025](https://github.com/oorabona/kysely-duckdb/commit/19a6025))
- bump @duckdb/node-api from 1.3.2-alpha.26 to 1.3.3 (deps) ([9e47a6d](https://github.com/oorabona/kysely-duckdb/commit/9e47a6d))
- bump @types/node from 22.18.0 to 24.3.0 (#2) (deps-dev) ([8d326af](https://github.com/oorabona/kysely-duckdb/commit/8d326af))
- bump @types/node from 22.18.0 to 24.3.0 (deps-dev) ([3b1b167](https://github.com/oorabona/kysely-duckdb/commit/3b1b167))

## [v0.4.0] - 2025-09-01

- feat: add SERIAL-like auto-increment example and enhance README with new examples
- refactor: improve performance reporting in examples and update migration scripts for DuckDB compatibility
- fix: ensure external data mappings are set up before querying in external-data example

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
[Unreleased]: https://github.com/oorabona/kysely-duckdb/compare/v0.5.0...HEAD
[v0.3.0]: git+https://github.com/oorabona/kysely-duckdb.git/releases/tag/v0.3.0
[v0.3.1]: git+https://github.com/oorabona/kysely-duckdb.git/releases/tag/v0.3.1
[v0.4.0]: git+https://github.com/oorabona/kysely-duckdb.git/releases/tag/v0.4.0
[v0.4.1]: https://github.com/oorabona/kysely-duckdb/releases/tag/v0.4.1
[v0.5.0]: https://github.com/oorabona/kysely-duckdb/releases/tag/v0.5.0
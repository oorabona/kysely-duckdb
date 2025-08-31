# Contributing to kysely-duckdb

Thank you for your interest in contributing to kysely-duckdb! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- Node.js 20.0.0 or higher
- pnpm (recommended package manager)
- TypeScript 5.3 or higher
- Git

### Setup Instructions

1. Fork and clone the repository:
```bash
git clone https://github.com/oorabona/kysely-duckdb.git
cd kysely-duckdb
```

2. Install dependencies:
```bash
pnpm install
```

3. Run tests to ensure everything works:
```bash
pnpm test
```

4. Start development:
```bash
pnpm dev
```

### Examples

The examples are written as if the package were external, importing from `@oorabona/kysely-duckdb`.

- For local development and CI typechecking, TypeScript path mapping resolves `@oorabona/kysely-duckdb` to the local `src/`.
- To run examples directly with tsx during development, you can run any of:

```bash
# Typecheck examples (no emit)
pnpm build:examples

# Run a single example (tsx executes source with ESM)
pnpm tsx examples/basic-usage.ts

# Run all examples
pnpm run examples:all
```

If you prefer running examples against the built output, build first:

```bash
pnpm build && pnpm tsx examples/basic-usage.ts
```

## Code Standards

### Quality Requirements

- **Test Coverage**: Maintain 100% test coverage
- **No Mocking**: Tests must use real DuckDB instances
- **Type Safety**: Full TypeScript support with strict mode
- **Performance**: All changes should maintain or improve performance

### Code Style

We use [Biome](https://biomejs.dev/) for formatting and linting, driven via pnpm scripts:

```bash
# Format code (writes changes)
pnpm format

# Check formatting
pnpm format:check

# Lint (no writes)
pnpm lint:check

# Lint with writes (safe autofixes)
pnpm lint
```

### Testing Standards

1. **Integration Tests**: Test against real DuckDB
2. **Unit Tests**: Cover all edge cases and error paths  
3. **Performance Tests**: Ensure no regressions
4. **Memory Tests**: Check for memory leaks

```bash
# Run all tests
pnpm test

# Run tests with coverage
pnpm test:coverage

# Run specific test file
pnpm test dialect.test.ts
```

CI runs separate jobs for lint, typecheck (including tests/examples), tests (with coverage), and build packing. Examples are typechecked before build thanks to the TS path mapping.

## Contribution Types

### Bug Reports

Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md):

- Clear description of the issue
- Minimal reproduction case
- Environment details
- Expected vs actual behavior

### Feature Requests

Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md):

- Clear use case description
- Proposed API design
- Backward compatibility considerations
- Performance implications

### Pull Requests

1. **Branch Naming**: Use `feature/description` or `fix/description`
2. **Commit Messages**: Follow conventional commits format
3. **Description**: Include motivation, changes, and testing
4. **Tests**: Add comprehensive tests for new functionality

## Development Workflow

### 1. Planning

- Open an issue to discuss significant changes
- Get maintainer feedback before starting large features
- Consider backward compatibility

### 2. Implementation

- Write failing tests first (TDD approach)
- Implement feature with comprehensive error handling
- Maintain 99%+ test coverage
- Document public APIs with JSDoc

### 3. Testing

```bash
# Run full test suite with coverage
pnpm test:coverage

# Test specific file or pattern
pnpm test tests/unit/driver.test.ts
```

### 4. Documentation

- Update README.md for new features
- Add examples for complex functionality
- Update TypeScript definitions
- Add JSDoc comments to public APIs

## Architecture Guidelines

### Core Principles

1. **Type Safety First**: Everything should be typed
2. **Performance**: Optimize for analytical workloads
3. **Simplicity**: Keep APIs simple and intuitive
4. **Extensibility**: Support DuckDB extensions
5. **Compatibility**: Maintain Kysely compatibility

### Project Structure

```
src/
├── dialect/          # Core dialect implementation
├── extensions/       # DuckDB extensions (JSON, Vector, Spatial)
├── migrations/       # Migration system
├── plugins/         # Kysely plugins
└── types/           # Type definitions

tests/
├── unit/            # Unit tests
├── integration/     # Integration tests
└── fixtures/        # Test fixtures
```

### Adding New Features

1. **Extensions**: Add to `src/extensions/`
2. **Plugins**: Add to `src/plugins/`
3. **Core Features**: Modify `src/dialect/`
4. **Types**: Update `src/types/`

## Testing Guidelines

### Test Categories

1. **Unit Tests**: Test individual functions/classes
2. **Integration Tests**: Test complete workflows
3. **Performance Tests**: Ensure no regressions
4. **Coverage Tests**: Maintain 99%+ coverage

### Writing Good Tests

```typescript
describe('Feature', () => {
  let database: DuckDBInstance
  let db: Kysely<TestSchema>

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    db = new Kysely({ dialect: new DuckDbDialect({ database }) })
  })

  afterEach(async () => {
    await db.destroy()
    database.closeSync()
  })

  it('should handle specific case', async () => {
    // Arrange
    await setupTestData(db)
    
    // Act
    const result = await performOperation(db)
    
    // Assert
    expect(result).toEqual(expectedResult)
  })
})
```

### Coverage Requirements

- **Statements**: 99%+
- **Branches**: 98%+
- **Functions**: 100%
- **Lines**: 99%+

## Performance Guidelines

### Optimization Principles

1. **Batch Operations**: Group database operations
2. **Memory Management**: Avoid memory leaks
3. **Connection Efficiency**: Reuse connections
4. **Query Optimization**: Use efficient SQL patterns

### Benchmarking

```bash
# Run performance tests
pnpm test:performance

# Memory usage analysis
pnpm test:memory

# Profile specific operations
pnpm profile
```

## Release Process

### Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes

### Changelog

We maintain the changelog via the chosen release tooling. If using Changesets, create a changeset for any user-visible change and let automation generate the changelog during release. Otherwise, keep [CHANGELOG.md](CHANGELOG.md) updated manually.

### Release Checklist

- [ ] All tests pass (CI green)
- [ ] Coverage targets met
- [ ] Documentation updated
- [ ] Examples updated
- [ ] Changelog updated (or changesets added)
- [ ] Version bumped

## Community

### Communication

- **Issues**: For bugs and feature requests
- **Discussions**: For questions and ideas
- **Discord**: For real-time chat (link in README)

### Code Review

All contributions require code review:

1. **Automated Checks**: CI must pass
2. **Manual Review**: Maintainer approval required
3. **Testing**: Comprehensive test coverage
4. **Documentation**: Updated as needed

## Getting Help

- **Documentation**: Check README and examples
- **Issues**: Search existing issues first
- **Discussions**: Use GitHub Discussions for questions
- **Discord**: Join our community Discord

## Recognition

Contributors are recognized in:

- **README**: Major contributors listed
- **Changelog**: Contributors credited
- **Releases**: Special thanks in release notes

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to kysely-duckdb! 🦆
import { describe, expect, it } from 'vitest'
import {
  createDuckDbMigrator,
  DuckDbDialect,
  DuckDbIntrospector,
  DuckDbMigrationUtils,
  JsonFunctions,
  SpatialFunctions,
  VectorFunctions,
} from '../../src/index.js'

describe('Index Exports', () => {
  it('should export DuckDbDialect', () => {
    expect(DuckDbDialect).toBeDefined()
    expect(typeof DuckDbDialect).toBe('function')
  })

  it('should export DuckDbIntrospector', () => {
    expect(DuckDbIntrospector).toBeDefined()
    expect(typeof DuckDbIntrospector).toBe('function')
  })

  it('should export createDuckDbMigrator', () => {
    expect(createDuckDbMigrator).toBeDefined()
    expect(typeof createDuckDbMigrator).toBe('function')
  })

  it('should export DuckDbMigrationUtils', () => {
    expect(DuckDbMigrationUtils).toBeDefined()
    expect(typeof DuckDbMigrationUtils).toBe('object')
  })

  it('should export JsonFunctions', () => {
    expect(JsonFunctions).toBeDefined()
    expect(typeof JsonFunctions).toBe('object')
  })

  it('should export SpatialFunctions', () => {
    expect(SpatialFunctions).toBeDefined()
    expect(typeof SpatialFunctions).toBe('object')
  })

  it('should export VectorFunctions', () => {
    expect(VectorFunctions).toBeDefined()
    expect(typeof VectorFunctions).toBe('object')
  })
})

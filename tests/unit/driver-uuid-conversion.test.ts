import { describe, expect, it } from 'vitest'
import { __test } from '../../src/dialect/duckdb-driver.js'

describe('DuckDbDriver UUID conversion', () => {
  it('returns string as-is', () => {
    expect(__test.convertUuidToString('123e4567-e89b-12d3-a456-426614174000')).toBe(
      '123e4567-e89b-12d3-a456-426614174000',
    )
  })

  it('falls back to toString when available', () => {
    const obj = { toString: () => 'abc-def' }
    expect(__test.convertUuidToString(obj)).toBe('abc-def')
  })

  it('attempts hugeint fallback if present', () => {
    const hugeint = { toString: (radix?: number) => (radix === 16 ? '1'.repeat(32) : 'n/a') }
    const obj: any = { hugeint }
    expect(__test.convertUuidToString(obj)).toMatch(/-/)
  })
})

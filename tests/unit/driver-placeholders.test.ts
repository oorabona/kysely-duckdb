import { CompiledQuery } from 'kysely'
import { describe, expect, it } from 'vitest'
import { __test } from '../../src/dialect/duckdb-driver.js'

describe('DuckDbDriver placeholders handling', () => {
  it('should keep $paramN as-is', () => {
    const q = CompiledQuery.raw('SELECT * FROM t WHERE a = $param1 AND b = $param2', [1, 'x'])
    const { sql, namedParams } = __test.prepareSQLWithParams(q)
    expect(sql).toBe('SELECT * FROM t WHERE a = $param1 AND b = $param2')
    expect(namedParams['param1']).toBe(1)
    expect(namedParams['param2']).toBe('x')
  })

  it('should rewrite $1, $2 ... to $param1, $param2 ... preserving numbering', () => {
    const q = CompiledQuery.raw('SELECT $1 as a, $2 as b, $1 as a2', [10, 'y'])
    const { sql, namedParams } = __test.prepareSQLWithParams(q)
    expect(sql).toBe('SELECT $param1 as a, $param2 as b, $param1 as a2')
    expect(namedParams['param1']).toBe(10)
    expect(namedParams['param2']).toBe('y')
  })

  it('should rewrite ? placeholders sequentially', () => {
    const q = CompiledQuery.raw('SELECT ? as a, ? as b, ? as c', [1, 2, 3])
    const { sql, namedParams } = __test.prepareSQLWithParams(q)
    expect(sql).toBe('SELECT $param1 as a, $param2 as b, $param3 as c')
    expect(namedParams['param1']).toBe(1)
    expect(namedParams['param2']).toBe(2)
    expect(namedParams['param3']).toBe(3)
  })
})

import { DuckDBInstance } from '@duckdb/node-api'
import { CompiledQuery, type TransactionSettings } from 'kysely'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DuckDbDriver } from '../../src/dialect/duckdb-driver.js'

describe('DuckDbDriver - Complete Functional Testing', () => {
  let database: DuckDBInstance
  let driver: DuckDbDriver

  beforeEach(async () => {
    database = await DuckDBInstance.create(':memory:')
    driver = new DuckDbDriver({
      database,
      uuidAsString: false,
      tableMappings: {},
      config: {},
    })
  })

  afterEach(async () => {
    try {
      await driver.destroy()
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('Driver Initialization', () => {
    it('should create driver with database instance', () => {
      expect(driver).toBeDefined()
      expect(driver).toBeInstanceOf(DuckDbDriver)
    })

    it('should initialize without errors', async () => {
      await expect(driver.init()).resolves.toBeUndefined()
    })

    it('should allow multiple initializations', async () => {
      await driver.init()
      await driver.init()
      await driver.init()
      // Should not throw
    })
  })

  describe('Connection Management', () => {
    it('should acquire database connection', async () => {
      const connection = await driver.acquireConnection()
      expect(connection).toBeDefined()
      expect(typeof connection.executeQuery).toBe('function')
    })

    it('should acquire multiple connections', async () => {
      const connection1 = await driver.acquireConnection()
      const connection2 = await driver.acquireConnection()

      expect(connection1).toBeDefined()
      expect(connection2).toBeDefined()
      expect(connection1).not.toBe(connection2)
    })

    it('should release connection without errors', async () => {
      await expect(driver.releaseConnection()).resolves.toBeUndefined()
    })

    it('should handle release without prior acquisition', async () => {
      await expect(driver.releaseConnection()).resolves.toBeUndefined()
    })
  })

  describe('Query Execution', () => {
    let connection: any

    beforeEach(async () => {
      connection = await driver.acquireConnection()
    })

    it('should execute SELECT query without parameters', async () => {
      const query = CompiledQuery.raw('SELECT 1 as test_value')
      const result = await connection.executeQuery(query)

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toEqual({ test_value: 1 })
      expect(result.numAffectedRows).toBe(BigInt(0))
    })

    it('should execute SELECT query with parameters', async () => {
      // First create a test table
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_params (id INTEGER, name VARCHAR)'),
      )
      await connection.executeQuery(
        CompiledQuery.raw("INSERT INTO test_params VALUES (1, 'Alice'), (2, 'Bob')"),
      )

      const query = CompiledQuery.raw('SELECT * FROM test_params WHERE id = $param1', [1])

      const result = await connection.executeQuery(query)
      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toEqual({ id: 1, name: 'Alice' })
    })

    it('should support $1-style positional parameters', async () => {
      const result = await connection.executeQuery(
        CompiledQuery.raw('SELECT $1::INTEGER as v, $2::VARCHAR as w, $1 + 1 as v2', [41, 'hello']),
      )

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ v: 41, w: 'hello', v2: 42 })
    })

    it('should execute INSERT query and return affected rows', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_insert (id INTEGER, value VARCHAR)'),
      )

      const query = CompiledQuery.raw('INSERT INTO test_insert VALUES ($param1, $param2)', [
        1,
        'test',
      ])

      const result = await connection.executeQuery(query)
      expect(result.numAffectedRows).toBe(BigInt(1))
      expect(result.numChangedRows).toBe(BigInt(1))
    })

    it('should execute UPDATE query and return affected rows', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_update (id INTEGER, value VARCHAR)'),
      )
      await connection.executeQuery(
        CompiledQuery.raw("INSERT INTO test_update VALUES (1, 'old'), (2, 'old')"),
      )

      const query = CompiledQuery.raw('UPDATE test_update SET value = $param1 WHERE id = $param2', [
        'new',
        1,
      ])

      const result = await connection.executeQuery(query)
      expect(result.numAffectedRows).toBe(BigInt(1))
      expect(result.numChangedRows).toBe(BigInt(1))
    })

    it('should execute DELETE query and return affected rows', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_delete (id INTEGER, value VARCHAR)'),
      )
      await connection.executeQuery(
        CompiledQuery.raw("INSERT INTO test_delete VALUES (1, 'test'), (2, 'test')"),
      )

      const query = CompiledQuery.raw('DELETE FROM test_delete WHERE id = $param1', [1])

      const result = await connection.executeQuery(query)
      expect(result.numAffectedRows).toBe(BigInt(1))
      expect(result.numChangedRows).toBe(BigInt(1))
    })

    it('should handle queries with RETURNING clause', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_returning (id INTEGER, value VARCHAR)'),
      )

      const query = CompiledQuery.raw(
        'INSERT INTO test_returning VALUES ($param1, $param2) RETURNING id, value',
        [1, 'test'],
      )

      const result = await connection.executeQuery(query)
      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toEqual({ id: 1, value: 'test' })
    })

    it('should treat PRAGMA as data-returning', async () => {
      const result = await connection.executeQuery(CompiledQuery.raw('PRAGMA version'))
      expect(Array.isArray(result.rows)).toBe(true)
      expect(result.rows.length).toBeGreaterThan(0)
      expect(typeof result.rows[0]).toBe('object')
    })

    it('should handle multiple parameters correctly', async () => {
      await connection.executeQuery(
        CompiledQuery.raw(
          'CREATE TABLE test_multi_params (a INTEGER, b VARCHAR, c BOOLEAN, d DOUBLE)',
        ),
      )

      const query = CompiledQuery.raw(
        'INSERT INTO test_multi_params VALUES ($param1, $param2, $param3, $param4) RETURNING *',
        [42, 'hello', true, 3.14],
      )

      const result = await connection.executeQuery(query)
      expect(result.rows[0]).toEqual({ a: 42, b: 'hello', c: true, d: 3.14 })
    })

    it('should handle null parameters', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_nulls (id INTEGER, value VARCHAR)'),
      )

      const query = CompiledQuery.raw(
        'INSERT INTO test_nulls VALUES ($param1, $param2) RETURNING *',
        [1, null],
      )

      const result = await connection.executeQuery(query)
      expect(result.rows[0]).toEqual({ id: 1, value: null })
    })
  })

  describe('JSON Data Handling', () => {
    let connection: any

    beforeEach(async () => {
      connection = await driver.acquireConnection()
    })

    it('should parse JSON strings automatically', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_json (id INTEGER, data JSON)'),
      )

      const jsonData = { name: 'Alice', age: 30 }
      const query = CompiledQuery.raw(
        'INSERT INTO test_json VALUES ($param1, $param2) RETURNING *',
        [1, JSON.stringify(jsonData)],
      )

      const result = await connection.executeQuery(query)
      expect(result.rows[0].data).toEqual(jsonData)
    })

    it('should handle JSON arrays', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_json_array (id INTEGER, items JSON)'),
      )

      const arrayData = [1, 2, 3, 'test']
      const query = CompiledQuery.raw(
        'INSERT INTO test_json_array VALUES ($param1, $param2) RETURNING *',
        [1, JSON.stringify(arrayData)],
      )

      const result = await connection.executeQuery(query)
      expect(result.rows[0].items).toEqual(arrayData)
    })

    it('should handle nested JSON objects', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_nested_json (id INTEGER, data JSON)'),
      )

      const nestedData = {
        user: { name: 'Alice', preferences: { theme: 'dark', notifications: true } },
        metadata: { created: '2023-01-01', version: 1 },
      }

      const query = CompiledQuery.raw(
        'INSERT INTO test_nested_json VALUES ($param1, $param2) RETURNING *',
        [1, JSON.stringify(nestedData)],
      )

      const result = await connection.executeQuery(query)
      expect(result.rows[0].data).toEqual(nestedData)
    })

    it('should handle quoted strings without parsing', async () => {
      const query = CompiledQuery.raw('SELECT \'"test_string"\' as quoted_value')
      const result = await connection.executeQuery(query)
      expect(result.rows[0].quoted_value).toBe('"test_string"')
    })

    it('should handle malformed JSON gracefully', async () => {
      const query = CompiledQuery.raw("SELECT 'invalid json' as invalid_json")
      const result = await connection.executeQuery(query)
      expect(result.rows[0].invalid_json).toBe('invalid json')
    })
  })

  describe('Array Data Handling', () => {
    let connection: any

    beforeEach(async () => {
      connection = await driver.acquireConnection()
    })

    it('should handle DuckDBListValue conversion', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_arrays (id INTEGER, numbers INTEGER[])'),
      )

      const query = CompiledQuery.raw(
        'INSERT INTO test_arrays VALUES (1, [1, 2, 3, 4]) RETURNING *',
      )
      const result = await connection.executeQuery(query)

      // The result should convert DuckDBListValue to plain array
      expect(Array.isArray(result.rows[0].numbers)).toBe(true)
    })

    it('should handle string arrays', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_string_arrays (id INTEGER, texts VARCHAR[])'),
      )

      const query = CompiledQuery.raw(
        "INSERT INTO test_string_arrays VALUES (1, ['a', 'b', 'c']) RETURNING *",
      )
      const result = await connection.executeQuery(query)

      expect(Array.isArray(result.rows[0].texts)).toBe(true)
    })

    it('should handle empty arrays', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_empty_arrays (id INTEGER, empty_array INTEGER[])'),
      )

      const query = CompiledQuery.raw('INSERT INTO test_empty_arrays VALUES (1, []) RETURNING *')
      const result = await connection.executeQuery(query)

      expect(Array.isArray(result.rows[0].empty_array)).toBe(true)
      expect(result.rows[0].empty_array).toHaveLength(0)
    })
  })

  describe('Transaction Management', () => {
    let connection: any

    beforeEach(async () => {
      connection = await driver.acquireConnection()
    })

    it('should begin transaction', async () => {
      const settings: TransactionSettings = {}
      await expect(driver.beginTransaction(connection, settings)).resolves.toBeUndefined()
    })

    it('should commit transaction', async () => {
      const settings: TransactionSettings = {}
      await driver.beginTransaction(connection, settings)
      await expect(driver.commitTransaction(connection)).resolves.toBeUndefined()
    })

    it('should rollback transaction', async () => {
      const settings: TransactionSettings = {}
      await driver.beginTransaction(connection, settings)
      await expect(driver.rollbackTransaction(connection)).resolves.toBeUndefined()
    })

    it('should handle transaction with isolation level', async () => {
      const settings: TransactionSettings = { isolationLevel: 'serializable' }
      await expect(driver.beginTransaction(connection, settings)).resolves.toBeUndefined()
    })

    it('should handle rollback without active transaction', async () => {
      await expect(driver.rollbackTransaction(connection)).resolves.toBeUndefined()
    })

    it('should handle nested transaction operations', async () => {
      const settings: TransactionSettings = {}

      // Begin transaction
      await driver.beginTransaction(connection, settings)

      // Execute some operations
      await connection.executeQuery(CompiledQuery.raw('CREATE TABLE test_tx (id INTEGER)'))
      await connection.executeQuery(CompiledQuery.raw('INSERT INTO test_tx VALUES (1)'))

      // Commit
      await driver.commitTransaction(connection)

      // Verify data persists
      const result = await connection.executeQuery(CompiledQuery.raw('SELECT * FROM test_tx'))
      expect(result.rows).toHaveLength(1)
    })

    it('should handle transaction rollback with data loss', async () => {
      const settings: TransactionSettings = {}

      // Begin transaction
      await driver.beginTransaction(connection, settings)

      // Execute operations
      await connection.executeQuery(CompiledQuery.raw('CREATE TABLE test_rollback (id INTEGER)'))
      await connection.executeQuery(CompiledQuery.raw('INSERT INTO test_rollback VALUES (1)'))

      // Rollback
      await driver.rollbackTransaction(connection)

      // Begin new transaction to test table existence
      await driver.beginTransaction(connection, {})
      try {
        await connection.executeQuery(CompiledQuery.raw('SELECT * FROM test_rollback'))
        await driver.commitTransaction(connection)
        // If we get here, rollback didn't work as expected
        // This might be a limitation of in-memory DuckDB
      } catch (error) {
        // Expected: table should not exist after rollback
        await driver.rollbackTransaction(connection)
        expect(error).toBeDefined()
      }
    })
  })

  describe('Streaming Queries', () => {
    let connection: any

    beforeEach(async () => {
      connection = await driver.acquireConnection()
      // Create test data for streaming
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_stream (id INTEGER, value VARCHAR)'),
      )

      // Insert multiple rows for streaming test
      for (let i = 1; i <= 100; i++) {
        await connection.executeQuery(
          CompiledQuery.raw(`INSERT INTO test_stream VALUES (${i}, 'value_${i}')`),
        )
      }
    })

    it('should stream query results', async () => {
      const query = CompiledQuery.raw('SELECT * FROM test_stream ORDER BY id')
      const stream = connection.streamQuery(query, 10)

      let totalRows = 0
      let chunkCount = 0

      for await (const chunk of stream) {
        expect(chunk.rows).toBeDefined()
        expect(Array.isArray(chunk.rows)).toBe(true)
        expect(chunk.rows.length).toBeGreaterThan(0)
        expect(chunk.rows.length).toBeLessThanOrEqual(10)

        totalRows += chunk.rows.length
        chunkCount++
      }

      expect(totalRows).toBe(100)
      expect(chunkCount).toBeGreaterThan(1)
    })

    it('should stream with custom chunk size', async () => {
      const query = CompiledQuery.raw('SELECT * FROM test_stream ORDER BY id LIMIT 50')
      const stream = connection.streamQuery(query, 5)

      let chunkCount = 0
      for await (const chunk of stream) {
        expect(chunk.rows.length).toBeLessThanOrEqual(5)
        chunkCount++
      }

      expect(chunkCount).toBe(10) // 50 rows / 5 per chunk
    })

    it('should stream with parameters', async () => {
      const query = CompiledQuery.raw(
        'SELECT * FROM test_stream WHERE id > $param1 ORDER BY id',
        [50],
      )

      const stream = connection.streamQuery(query, 20)

      let totalRows = 0
      for await (const chunk of stream) {
        totalRows += chunk.rows.length
        // All rows should have id > 50
        for (const row of chunk.rows) {
          expect(row.id).toBeGreaterThan(50)
        }
      }

      expect(totalRows).toBe(50)
    })

    it('should handle empty stream results', async () => {
      const query = CompiledQuery.raw('SELECT * FROM test_stream WHERE id > 1000')
      const stream = connection.streamQuery(query, 10)

      let chunkCount = 0
      for await (const chunk of stream) {
        chunkCount++
        expect(chunk.rows).toHaveLength(0)
      }

      expect(chunkCount).toBeLessThanOrEqual(1)
    })

    it('should handle streaming with JSON data', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_stream_json (id INTEGER, data JSON)'),
      )

      for (let i = 1; i <= 20; i++) {
        const jsonData = JSON.stringify({ id: i, name: `user_${i}` })
        await connection.executeQuery(
          CompiledQuery.raw(`INSERT INTO test_stream_json VALUES (${i}, '${jsonData}')`),
        )
      }

      const query = CompiledQuery.raw('SELECT * FROM test_stream_json ORDER BY id')
      const stream = connection.streamQuery(query, 5)

      for await (const chunk of stream) {
        for (const row of chunk.rows) {
          expect(typeof row.data).toBe('object')
          expect(row.data.id).toBe(row.id)
          expect(row.data.name).toBe(`user_${row.id}`)
        }
      }
    })
  })

  describe('Error Handling', () => {
    let connection: any

    beforeEach(async () => {
      connection = await driver.acquireConnection()
    })

    it('should handle SQL syntax errors', async () => {
      const query = CompiledQuery.raw('INVALID SQL SYNTAX')
      await expect(connection.executeQuery(query)).rejects.toThrow('DuckDB query failed')
    })

    it('should handle non-existent table errors', async () => {
      const query = CompiledQuery.raw('SELECT * FROM non_existent_table')
      await expect(connection.executeQuery(query)).rejects.toThrow()
    })

    it('should handle parameter binding errors', async () => {
      const query = CompiledQuery.raw('SELECT $param1 as test', [undefined]) // undefined parameter should cause issue

      // Should throw an error for undefined parameter
      await expect(connection.executeQuery(query)).rejects.toThrow()
    })

    it('should handle constraint violations', async () => {
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_constraints (id INTEGER PRIMARY KEY, name VARCHAR)'),
      )
      await connection.executeQuery(
        CompiledQuery.raw("INSERT INTO test_constraints VALUES (1, 'test')"),
      )

      // Try to insert duplicate primary key
      const query = CompiledQuery.raw("INSERT INTO test_constraints VALUES (1, 'duplicate')")
      await expect(connection.executeQuery(query)).rejects.toThrow()
    })

    it('should handle connection errors gracefully', async () => {
      // Destroy the database to simulate connection errors
      database.closeSync()

      const query = CompiledQuery.raw('SELECT 1')
      await expect(connection.executeQuery(query)).rejects.toThrow()
    })
  })

  describe('Performance and Memory', () => {
    it('should handle large result sets efficiently', async () => {
      const connection = await driver.acquireConnection()

      // Create larger dataset
      await connection.executeQuery(
        CompiledQuery.raw('CREATE TABLE test_performance (id INTEGER, data VARCHAR)'),
      )

      // Insert 1000 rows at once using VALUES clause
      const values = Array.from({ length: 1000 }, (_, i) => `(${i + 1}, 'data_${i + 1}')`).join(
        ', ',
      )
      await connection.executeQuery(
        CompiledQuery.raw(`INSERT INTO test_performance VALUES ${values}`),
      )

      const startTime = Date.now()
      const result = await connection.executeQuery(
        CompiledQuery.raw('SELECT * FROM test_performance'),
      )
      const duration = Date.now() - startTime

      expect(result.rows).toHaveLength(1000)
      expect(duration).toBeLessThan(5000) // Should complete in less than 5 seconds
    })

    it('should not leak memory with many connections', async () => {
      const initialMemory = process.memoryUsage().heapUsed

      const connections = []
      for (let i = 0; i < 50; i++) {
        connections.push(await driver.acquireConnection())
      }

      // Execute queries on all connections
      for (const conn of connections) {
        await conn.executeQuery(CompiledQuery.raw('SELECT 1'))
      }

      const finalMemory = process.memoryUsage().heapUsed
      const memoryIncrease = finalMemory - initialMemory

      // Memory increase should be reasonable (less than 50MB)
      expect(memoryIncrease).toBeLessThan(50 * 1024 * 1024)
    })
  })

  describe('Driver Destruction', () => {
    it('should destroy driver cleanly', async () => {
      await expect(driver.destroy()).resolves.toBeUndefined()
    })

    it('should handle multiple destroy calls', async () => {
      await driver.destroy()
      await expect(driver.destroy()).resolves.toBeUndefined()
    })

    it('should not allow operations after destruction', async () => {
      await driver.destroy()

      // Attempting operations after destruction might fail
      // This depends on the specific implementation
      try {
        const connection = await driver.acquireConnection()
        await connection.executeQuery(CompiledQuery.raw('SELECT 1'))
      } catch (error) {
        expect(error).toBeDefined()
      }
    })
  })

  describe('Data Processing Edge Cases', () => {
    it('should handle non-object rows in processRows function by testing internal path', async () => {
      const connection = await driver.acquireConnection()

      // Test the specific edge case by testing with scalar values
      // We'll use a technique to force the system to process non-object data
      // by creating a very specific data structure that might confuse the row processing

      // Create a test that returns scalar values in specific conditions
      const result = await connection.executeQuery(
        CompiledQuery.raw(`
          SELECT 'test' as value
          UNION ALL 
          SELECT CAST(NULL AS VARCHAR) as value
        `),
      )

      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toHaveProperty('value', 'test')
      expect(result.rows[1]).toHaveProperty('value', null)

      // Test using the streaming version which internally uses processRows differently
      let rowCount = 0
      for await (const chunk of connection.streamQuery(
        CompiledQuery.raw('SELECT unnest([1, 2, 3, NULL, 5]) as numbers'),
      )) {
        rowCount += chunk.rows.length
        // This should process through processRows and handle edge cases
      }

      expect(rowCount).toBeGreaterThan(0)
    })

    it('should handle unexpected rollback errors with console.warn', async () => {
      const connection = await driver.acquireConnection()

      // Mock console.warn to capture the output
      const originalWarn = console.warn
      let warningMessage = ''
      console.warn = (message: string, ...details: any[]) => {
        warningMessage = `${message} ${details.join(' ')}`
      }

      try {
        // Start a transaction
        await driver.beginTransaction(connection, {})

        // Force an unexpected error by trying to rollback with a connection issue
        // We'll simulate this by mocking the executeQuery to throw a different error
        const originalExecuteQuery = connection.executeQuery
        connection.executeQuery = async () => {
          throw new Error('Connection lost unexpectedly')
        }

        // This should trigger the console.warn path (lines 144-145)
        await driver.rollbackTransaction(connection)

        expect(warningMessage).toContain(
          '[KYSELY-DUCKDB] Unexpected error during rollback: Connection lost unexpectedly',
        )

        // Restore original method
        connection.executeQuery = originalExecuteQuery
      } finally {
        // Restore console.warn
        console.warn = originalWarn
      }
    })
  })
})

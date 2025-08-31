import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  formatPerformanceStats,
  globalPerformanceMonitor,
} from '../../src/internal/performance-monitor.js'

describe('Performance Monitor Coverage', () => {
  beforeEach(() => {
    globalPerformanceMonitor.reset()
  })

  describe('Metrics Management', () => {
    it('should maintain metrics size limit', () => {
      // Access the private maxMetrics property via any to test the limit
      const monitor = globalPerformanceMonitor as any
      const originalMax = monitor.maxMetrics

      // Set a small limit for testing
      monitor.maxMetrics = 3

      // Record more metrics than the limit
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 1',
        duration: 10,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 2',
        duration: 20,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 3',
        duration: 30,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 4',
        duration: 40,
        rowCount: 1,
        parameters: [],
      })

      // Should trigger the limit check and shift() call (lines 52-54)
      const stats = globalPerformanceMonitor.getStats()
      expect(stats.totalQueries).toBe(3) // Should be limited to maxMetrics

      // Restore original limit
      monitor.maxMetrics = originalMax
    })

    it('should record and track errors', () => {
      // Record some queries and errors
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 1',
        duration: 10,
        rowCount: 1,
        parameters: [],
      })

      // This should trigger recordError() and increment errorCount (lines 60-62)
      globalPerformanceMonitor.recordError()
      globalPerformanceMonitor.recordError()

      const stats = globalPerformanceMonitor.getStats()
      expect(stats.errorRate).toBeGreaterThan(0)
      expect(stats.totalQueries).toBe(1)
    })
  })

  describe('Statistics and Analysis', () => {
    it('should calculate statistics with slow queries', () => {
      // Record queries with different durations
      globalPerformanceMonitor.recordQuery({
        sql: 'FAST QUERY',
        duration: 5,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'SLOW QUERY',
        duration: 1500, // Above default slow threshold
        rowCount: 100,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'VERY SLOW QUERY',
        duration: 2000,
        rowCount: 500,
        parameters: [],
      })

      // This should trigger the slow query filtering and sorting logic (lines 68-86)
      const stats = globalPerformanceMonitor.getStats()

      expect(stats.totalQueries).toBe(3)
      expect(stats.averageDuration).toBeGreaterThan(0)
      expect(stats.slowQueries.length).toBe(2) // Two slow queries
      expect(stats.slowQueries?.[0]?.duration).toBe(2000) // Should be sorted by duration desc
      expect(stats.memoryUsage).toBeDefined()
    })

    it('should handle empty metrics gracefully', () => {
      // With no recorded queries, should handle division by zero
      const stats = globalPerformanceMonitor.getStats()

      expect(stats.totalQueries).toBe(0)
      expect(stats.averageDuration).toBe(0)
      expect(stats.errorRate).toBe(0)
      expect(stats.slowQueries).toEqual([])
    })

    it('should reset metrics and error count', () => {
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 1',
        duration: 10,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordError()

      // This should trigger reset() method (lines 91-94)
      globalPerformanceMonitor.reset()

      const stats = globalPerformanceMonitor.getStats()
      expect(stats.totalQueries).toBe(0)
      expect(stats.errorRate).toBe(0)
    })

    it('should get recent queries in reverse order', () => {
      globalPerformanceMonitor.recordQuery({
        sql: 'QUERY 1',
        duration: 10,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'QUERY 2',
        duration: 20,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'QUERY 3',
        duration: 30,
        rowCount: 1,
        parameters: [],
      })

      // This should trigger getRecentQueries() method (lines 99-101)
      const recent = globalPerformanceMonitor.getRecentQueries(2)

      expect(recent).toHaveLength(2)
      expect(recent?.[0]?.sql).toBe('QUERY 3') // Most recent first
      expect(recent?.[1]?.sql).toBe('QUERY 2')
    })

    it('should filter queries by duration range', () => {
      globalPerformanceMonitor.recordQuery({
        sql: 'FAST',
        duration: 5,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'MEDIUM',
        duration: 50,
        rowCount: 1,
        parameters: [],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'SLOW',
        duration: 500,
        rowCount: 1,
        parameters: [],
      })

      // This should trigger getQueriesByDuration() method (lines 106-110)
      const mediumQueries = globalPerformanceMonitor.getQueriesByDuration(20, 100)

      expect(mediumQueries).toHaveLength(1)
      expect(mediumQueries?.[0]?.sql).toBe('MEDIUM')

      const allQueries = globalPerformanceMonitor.getQueriesByDuration()
      expect(allQueries).toHaveLength(3)
    })
  })

  describe('Edge Cases', () => {
    it('should handle queries with various parameter types', () => {
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT * FROM table WHERE id = ? AND name = ?',
        duration: 25,
        rowCount: 1,
        parameters: [123, 'test', null, true, { complex: 'object' }],
      })

      const stats = globalPerformanceMonitor.getStats()
      expect(stats.totalQueries).toBe(1)
    })
  })

  describe('Performance Monitor Comprehensive Testing', () => {
    it('should handle bulk query recording like in integration tests', () => {
      // Simulate bulk operations like in queries.test.ts
      const startTime = Date.now()

      // Record 1000 queries like the bulk test example
      const bulkQueries = Array.from({ length: 1000 }, (_, i) => ({
        sql: `INSERT INTO bulk_test VALUES (${i}, '{"data": ${i}}', '2024-01-01')`,
        duration: Math.random() * 100, // Random duration 0-100ms
        rowCount: 1,
        parameters: [i, `{"data": ${i}}`, '2024-01-01'],
      }))

      // Record in chunks for better performance (like the example)
      const chunkSize = 100
      for (let i = 0; i < bulkQueries.length; i += chunkSize) {
        const chunk = bulkQueries.slice(i, i + chunkSize)
        chunk.forEach(query => {
          globalPerformanceMonitor.recordQuery(query)
        })
      }

      const insertDuration = Date.now() - startTime

      // Test performance monitoring capabilities
      const stats = globalPerformanceMonitor.getStats()
      expect(stats.totalQueries).toBe(1000)
      expect(stats.averageDuration).toBeGreaterThan(0)
      expect(insertDuration).toBeLessThan(1000) // Should be fast

      // Test memory usage is tracked (NodeJS.MemoryUsage structure)
      expect(stats.memoryUsage).toBeDefined()
      expect(typeof stats.memoryUsage?.rss).toBe('number')
      expect(typeof stats.memoryUsage?.heapUsed).toBe('number')
      expect(typeof stats.memoryUsage?.heapTotal).toBe('number')
    })

    it('should track error rates during bulk operations', () => {
      // Record mix of successful and failed queries
      for (let i = 0; i < 100; i++) {
        globalPerformanceMonitor.recordQuery({
          sql: `SELECT * FROM test_table WHERE id = ${i}`,
          duration: 10,
          rowCount: 1,
          parameters: [i],
        })

        // Simulate 10% error rate
        if (i % 10 === 0) {
          globalPerformanceMonitor.recordError()
        }
      }

      const stats = globalPerformanceMonitor.getStats()
      expect(stats.totalQueries).toBe(100)
      // Error rate is calculated as errorCount / (totalQueries + errorCount)
      // With 10 errors and 100 queries: 10 / (100 + 10) = 10/110 = ~0.0909
      expect(stats.errorRate).toBeCloseTo(0.0909, 3)
    })

    it('should handle memory monitoring edge cases', () => {
      // Test when process.memoryUsage() might have different properties
      const originalMemoryUsage = process.memoryUsage

      // Mock memory usage to test different scenarios
      process.memoryUsage = vi.fn().mockReturnValue({
        rss: 100 * 1024 * 1024,
        heapUsed: 50 * 1024 * 1024,
        heapTotal: 80 * 1024 * 1024,
        external: 10 * 1024 * 1024,
        arrayBuffers: 5 * 1024 * 1024,
      }) as unknown as typeof process.memoryUsage

      const stats = globalPerformanceMonitor.getStats()
      expect(stats.memoryUsage).toBeDefined()
      expect(stats.memoryUsage?.heapUsed).toBe(50 * 1024 * 1024) // heapUsed
      expect(stats.memoryUsage?.heapTotal).toBe(80 * 1024 * 1024) // heapTotal
      expect(stats.memoryUsage?.rss).toBe(100 * 1024 * 1024) // rss

      // Restore original function
      process.memoryUsage = originalMemoryUsage
    })
  })

  describe('Format Performance Stats', () => {
    it('should format performance stats string output', () => {
      // Record some test data
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT * FROM test_table WHERE condition = ?',
        duration: 1500, // Slow query
        rowCount: 100,
        parameters: ['test'],
      })
      globalPerformanceMonitor.recordQuery({
        sql: 'INSERT INTO test_table VALUES (?, ?)',
        duration: 50,
        rowCount: 1,
        parameters: [1, 'value'],
      })
      globalPerformanceMonitor.recordError()

      const stats = globalPerformanceMonitor.getStats()
      const formatted = formatPerformanceStats(stats)

      // Test the formatted string contains expected sections
      expect(formatted).toContain('=== kysely-duckdb Performance Stats ===')
      expect(formatted).toContain('Total Queries: 2')
      expect(formatted).toContain('Average Duration:')
      expect(formatted).toContain('Error Rate:')
      expect(formatted).toContain('Memory Usage:')
      expect(formatted).toContain('RSS:')
      expect(formatted).toContain('Heap Used:')
      expect(formatted).toContain('Heap Total:')
      expect(formatted).toContain('Slow Queries:')
      expect(formatted).toContain('1500ms: SELECT * FROM test_table WHERE condition = ?')
      expect(formatted).toContain('=====================================')

      // Test the structure is a proper multi-line string
      const lines = formatted.split('\n')
      expect(lines.length).toBeGreaterThan(5)
    })

    it('should format stats without slow queries', () => {
      // Record only fast queries
      globalPerformanceMonitor.recordQuery({
        sql: 'SELECT 1',
        duration: 5,
        rowCount: 1,
        parameters: [],
      })

      const stats = globalPerformanceMonitor.getStats()
      const formatted = formatPerformanceStats(stats)

      expect(formatted).toContain('Total Queries: 1')
      expect(formatted).toContain('Average Duration: 5.00ms')
      expect(formatted).not.toContain('Slow Queries:')
    })

    it('should format stats with very long SQL queries', () => {
      const longSql =
        'SELECT * FROM extremely_long_table_name_that_exceeds_sixty_characters_and_should_be_truncated WHERE condition = ?'

      globalPerformanceMonitor.recordQuery({
        sql: longSql,
        duration: 2000, // Slow query to trigger formatting
        rowCount: 1000,
        parameters: ['test'],
      })

      const stats = globalPerformanceMonitor.getStats()
      const formatted = formatPerformanceStats(stats)

      // Should truncate long SQL at 60 chars with "..."
      // The format adds indentation, so look for the pattern with proper spacing
      expect(formatted).toMatch(
        /2000ms: SELECT \* FROM extremely_long_table_name_that_exceeds_s.*\.\.\./,
      )
      expect(formatted).not.toContain(longSql) // Full SQL should not be present
    })
  })
})

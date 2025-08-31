/**
 * Performance monitoring utilities for kysely-duckdb
 */

export interface QueryMetrics {
  sql: string
  duration: number
  rowCount: number
  timestamp: Date
  parameters?: any[]
}

export interface PerformanceStats {
  totalQueries: number
  averageDuration: number
  slowQueries: QueryMetrics[]
  errorRate: number
  memoryUsage?: NodeJS.MemoryUsage
}

/**
 * Performance monitor for query execution
 */
export class PerformanceMonitor {
  private metrics: QueryMetrics[] = []
  private readonly maxMetrics: number
  private readonly slowQueryThreshold: number
  private errorCount = 0

  constructor(
    options: {
      maxMetrics?: number
      slowQueryThreshold?: number
    } = {},
  ) {
    this.maxMetrics = options.maxMetrics ?? 1000
    this.slowQueryThreshold = options.slowQueryThreshold ?? 1000 // 1 second
  }

  /**
   * Record a query execution
   */
  recordQuery(metrics: Omit<QueryMetrics, 'timestamp'>): void {
    const fullMetrics: QueryMetrics = {
      ...metrics,
      timestamp: new Date(),
    }

    this.metrics.push(fullMetrics)

    // Maintain metrics size limit
    if (this.metrics.length > this.maxMetrics) {
      this.metrics.shift()
    }
  }

  /**
   * Record a query error
   */
  recordError(): void {
    this.errorCount++
  }

  /**
   * Get performance statistics
   */
  getStats(): PerformanceStats {
    const totalQueries = this.metrics.length
    const averageDuration =
      totalQueries > 0 ? this.metrics.reduce((sum, m) => sum + m.duration, 0) / totalQueries : 0

    const slowQueries = this.metrics
      .filter(m => m.duration > this.slowQueryThreshold)
      .sort((a, b) => b.duration - a.duration)
      .slice(0, 10) // Top 10 slowest

    const errorRate = totalQueries > 0 ? this.errorCount / (totalQueries + this.errorCount) : 0

    return {
      totalQueries,
      averageDuration,
      slowQueries,
      errorRate,
      memoryUsage: process.memoryUsage(),
    }
  }

  /**
   * Reset all metrics
   */
  reset(): void {
    this.metrics = []
    this.errorCount = 0
  }

  /**
   * Get recent queries (last N queries)
   */
  getRecentQueries(count = 10): QueryMetrics[] {
    return this.metrics.slice(-count).reverse()
  }

  /**
   * Get queries by duration range
   */
  getQueriesByDuration(minMs = 0, maxMs = Infinity): QueryMetrics[] {
    return this.metrics
      .filter(m => m.duration >= minMs && m.duration <= maxMs)
      .sort((a, b) => b.duration - a.duration)
  }
}

/**
 * Global performance monitor instance
 */
export const globalPerformanceMonitor = new PerformanceMonitor()

/**
 * Format performance statistics for logging
 */
export function formatPerformanceStats(stats: PerformanceStats): string {
  const lines = [
    '=== kysely-duckdb Performance Stats ===',
    `Total Queries: ${stats.totalQueries}`,
    `Average Duration: ${stats.averageDuration.toFixed(2)}ms`,
    `Error Rate: ${(stats.errorRate * 100).toFixed(2)}%`,
    '',
  ]

  if (stats.memoryUsage) {
    lines.push(
      'Memory Usage:',
      `  RSS: ${(stats.memoryUsage.rss / 1024 / 1024).toFixed(2)}MB`,
      `  Heap Used: ${(stats.memoryUsage.heapUsed / 1024 / 1024).toFixed(2)}MB`,
      `  Heap Total: ${(stats.memoryUsage.heapTotal / 1024 / 1024).toFixed(2)}MB`,
      '',
    )
  }

  if (stats.slowQueries.length > 0) {
    lines.push(
      'Slow Queries:',
      ...stats.slowQueries
        .slice(0, 5)
        .map(q => `  ${q.duration}ms: ${q.sql.slice(0, 60)}${q.sql.length > 60 ? '...' : ''}`),
      '',
    )
  }

  lines.push('=====================================')
  return lines.join('\n')
}

/**
 * Internal logger for kysely-duckdb
 * Only logs in development mode or when KYSELY_DEBUG is set
 */
/** biome-ignore-all lint/complexity/useLiteralKeys: contradicts biome error about index based access */

export interface Logger {
  debug(...args: any[]): void
  info(...args: any[]): void
  warn(...args: any[]): void
  error(...args: any[]): void
}

class DuckDbLogger implements Logger {
  private readonly isDebugEnabled: boolean

  constructor() {
    this.isDebugEnabled =
      process.env['NODE_ENV'] === 'development' ||
      process.env['KYSELY_DEBUG'] === 'true' ||
      process.env['KYSELY_DEBUG'] === '1'
  }

  debug(...args: any[]): void {
    if (this.isDebugEnabled) {
      console.log('[KYSELY-DUCKDB]', ...args)
    }
  }

  info(...args: any[]): void {
    if (this.isDebugEnabled) {
      console.info('[KYSELY-DUCKDB]', ...args)
    }
  }

  warn(...args: any[]): void {
    console.warn('[KYSELY-DUCKDB]', ...args)
  }

  error(...args: any[]): void {
    console.error('[KYSELY-DUCKDB]', ...args)
  }
}

export const logger = new DuckDbLogger()

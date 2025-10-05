/**
 * Internal logger for kysely-duckdb
 * Only logs in development mode or when KYSELY_DEBUG is set
 */
/** biome-ignore-all lint/complexity/useLiteralKeys: contradicts biome error about index based access */

import type { LoggerArgs } from '../types/duckdb-bindings.js'

export interface Logger {
  debug(...args: LoggerArgs): void
  info(...args: LoggerArgs): void
  warn(...args: LoggerArgs): void
  error(...args: LoggerArgs): void
}

export interface LoggerOptions {
  debugEnabled?: boolean
  prefix?: string
}

class DuckDbLogger implements Logger {
  private readonly isDebugEnabled: boolean
  private readonly prefix: string

  constructor(options: LoggerOptions = {}) {
    this.isDebugEnabled =
      options.debugEnabled ??
      (process.env['NODE_ENV'] === 'development' ||
        process.env['KYSELY_DEBUG'] === 'true' ||
        process.env['KYSELY_DEBUG'] === '1')
    this.prefix = options.prefix ?? '[KYSELY-DUCKDB]'
  }

  debug(...args: LoggerArgs): void {
    if (this.isDebugEnabled) {
      console.log(this.prefix, ...args)
    }
  }

  info(...args: LoggerArgs): void {
    if (this.isDebugEnabled) {
      console.info(this.prefix, ...args)
    }
  }

  warn(...args: LoggerArgs): void {
    console.warn(this.prefix, ...args)
  }

  error(...args: LoggerArgs): void {
    console.error(this.prefix, ...args)
  }
}

export const logger = new DuckDbLogger()

export function createLogger(options: LoggerOptions): Logger {
  return new DuckDbLogger(options)
}

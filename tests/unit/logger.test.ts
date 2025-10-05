import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createLogger } from '../../src/internal/logger.js'

describe('Logger', () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>
  let consoleInfoSpy: ReturnType<typeof vi.spyOn>
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    consoleInfoSpy = vi.spyOn(console, 'info').mockImplementation(() => {})
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    consoleLogSpy.mockRestore()
    consoleInfoSpy.mockRestore()
    consoleWarnSpy.mockRestore()
    consoleErrorSpy.mockRestore()
  })

  describe('createLogger', () => {
    it('should create logger with custom debug enabled', () => {
      const logger = createLogger({ debugEnabled: true })

      logger.debug('test message')
      expect(consoleLogSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'test message')
    })

    it('should create logger with custom prefix', () => {
      const logger = createLogger({ prefix: '[CUSTOM]', debugEnabled: true })

      logger.info('test info')
      expect(consoleInfoSpy).toHaveBeenCalledWith('[CUSTOM]', 'test info')
    })

    it('should create logger with debug disabled', () => {
      const logger = createLogger({ debugEnabled: false })

      logger.debug('should not log')
      expect(consoleLogSpy).not.toHaveBeenCalled()
    })

    it('should always log warnings regardless of debug setting', () => {
      const logger = createLogger({ debugEnabled: false })

      logger.warn('warning message')
      expect(consoleWarnSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'warning message')
    })

    it('should always log errors regardless of debug setting', () => {
      const logger = createLogger({ debugEnabled: false })

      logger.error('error message')
      expect(consoleErrorSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'error message')
    })

    it('should use default options when none provided', () => {
      const logger = createLogger({})

      logger.warn('test')
      expect(consoleWarnSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'test')
    })

    it('should handle multiple arguments in log methods', () => {
      const logger = createLogger({ debugEnabled: true })

      logger.debug('message', { foo: 'bar' }, 123, true)
      expect(consoleLogSpy).toHaveBeenCalledWith(
        '[KYSELY-DUCKDB]',
        'message',
        { foo: 'bar' },
        123,
        true,
      )
    })

    it('should handle Error objects in logs', () => {
      const logger = createLogger({ debugEnabled: true })
      const error = new Error('test error')

      logger.error('An error occurred:', error)
      expect(consoleErrorSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'An error occurred:', error)
    })

    it('should handle Date objects in logs', () => {
      const logger = createLogger({ debugEnabled: true })
      const date = new Date('2025-01-01')

      logger.debug('Date:', date)
      expect(consoleLogSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'Date:', date)
    })

    it('should handle complex objects in logs', () => {
      const logger = createLogger({ debugEnabled: true })
      const obj = { nested: { value: 123 }, array: [1, 2, 3] }

      logger.info('Complex object:', obj)
      expect(consoleInfoSpy).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'Complex object:', obj)
    })
  })
})

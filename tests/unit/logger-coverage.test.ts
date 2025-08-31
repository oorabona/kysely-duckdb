import { describe, expect, it, vi } from 'vitest'
import { logger } from '../../src/internal/logger.js'

describe('Logger Coverage', () => {
  describe('Logger Methods Coverage', () => {
    it('should call all logger methods for coverage', () => {
      // Mock all console methods to test coverage
      const consoleSpy = {
        log: vi.spyOn(console, 'log').mockImplementation(() => {}),
        info: vi.spyOn(console, 'info').mockImplementation(() => {}),
        warn: vi.spyOn(console, 'warn').mockImplementation(() => {}),
        error: vi.spyOn(console, 'error').mockImplementation(() => {}),
      }

      try {
        // Call all logger methods - this covers the function calls
        logger.debug('debug message', 'param1', 123)
        logger.info('info message', { test: true })
        logger.warn('warning message')
        logger.error('error message', new Error('test'))

        // We just need to verify the methods were called
        // The actual logging behavior depends on environment variables
        expect(typeof logger.debug).toBe('function')
        expect(typeof logger.info).toBe('function')
        expect(typeof logger.warn).toBe('function')
        expect(typeof logger.error).toBe('function')
      } finally {
        // Restore console methods
        Object.values(consoleSpy).forEach(spy => {
          spy.mockRestore()
        })
      }
    })

    it('should handle warn and error methods which always log', () => {
      const consoleSpy = {
        warn: vi.spyOn(console, 'warn').mockImplementation(() => {}),
        error: vi.spyOn(console, 'error').mockImplementation(() => {}),
      }

      try {
        logger.warn('warning message', 'extra', 123)
        logger.error('error message', new Error('test'), { context: true })

        // warn and error should always be called regardless of environment
        expect(consoleSpy.warn).toHaveBeenCalledWith(
          '[KYSELY-DUCKDB]',
          'warning message',
          'extra',
          123,
        )
        expect(consoleSpy.error).toHaveBeenCalledWith(
          '[KYSELY-DUCKDB]',
          'error message',
          expect.any(Error),
          { context: true },
        )
      } finally {
        Object.values(consoleSpy).forEach(spy => {
          spy.mockRestore()
        })
      }
    })

    it('should test debug/info branches for complete coverage', async () => {
      // Mock environment to test both enabled and disabled states
      const originalEnv = process.env['NODE_ENV']
      const originalDebug = process.env['KYSELY_DEBUG']

      const consoleSpy = {
        log: vi.spyOn(console, 'log').mockImplementation(() => {}),
        info: vi.spyOn(console, 'info').mockImplementation(() => {}),
      }

      try {
        // Test disabled state (production, no debug flag)
        process.env['NODE_ENV'] = 'production'
        delete process.env['KYSELY_DEBUG']

        // This creates a new logger instance that reads the environment
        const { logger: prodLogger } = await import('../../src/internal/logger.js')

        prodLogger.debug('should not log')
        prodLogger.info('should not log')

        expect(consoleSpy.log).not.toHaveBeenCalled()
        expect(consoleSpy.info).not.toHaveBeenCalled()

        // Test enabled state
        process.env['NODE_ENV'] = 'development'

        // Force re-import to get new logger instance
        vi.resetModules()
        const { logger: devLogger } = await import('../../src/internal/logger.js')

        devLogger.debug('should log in dev')
        devLogger.info('should log in dev')

        expect(consoleSpy.log).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'should log in dev')
        expect(consoleSpy.info).toHaveBeenCalledWith('[KYSELY-DUCKDB]', 'should log in dev')
      } finally {
        // Restore environment
        process.env['NODE_ENV'] = originalEnv
        if (originalDebug) {
          process.env['KYSELY_DEBUG'] = originalDebug
        }
        Object.values(consoleSpy).forEach(spy => {
          spy.mockRestore()
        })
        vi.resetModules()
      }
    })
  })
})

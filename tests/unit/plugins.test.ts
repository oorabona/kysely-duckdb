import { describe, expect, it } from 'vitest'
import { CaseConverterPlugin } from '../../src/plugins/case-converter.js'

describe('Plugins', () => {
  describe('CaseConverterPlugin', () => {
    it('should create plugin with default options', () => {
      const plugin = new CaseConverterPlugin()
      expect(plugin).toBeDefined()
      expect(plugin.transformQuery).toBeDefined()
      expect(plugin.transformResult).toBeDefined()
    })

    it('should support enabling/disabling strategies', () => {
      const snakeCasePlugin = new CaseConverterPlugin({
        toSnakeCase: true,
        toCamelCase: true,
      })

      expect(snakeCasePlugin).toBeDefined()
      expect(snakeCasePlugin.transformQuery).toBeDefined()
      expect(snakeCasePlugin.transformResult).toBeDefined()
    })

    it('should support disabling transformations', () => {
      const camelCasePlugin = new CaseConverterPlugin({
        toSnakeCase: false,
        toCamelCase: false,
      })

      expect(camelCasePlugin).toBeDefined()
    })
  })
})

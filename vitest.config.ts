import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/',
        'scripts/',
        'dist/',
        'tests/',
        '*.config.*',
        '**/*.d.ts',
        '**/types*',
        'examples/',
        'docs/',
      ],
      thresholds: {
        branches: 90,
        functions: 90,
        lines: 90,
        statements: 90,
      },
    },
    include: ['tests/**/*.test.ts'],
    exclude: ['node_modules/', 'dist/', '**/*.d.ts'],
    testTimeout: 10000,
    hookTimeout: 10000,
    teardownTimeout: 5000,
    isolate: true,
    pool: 'forks',
  },
  esbuild: {
    target: 'node20',
  },
})

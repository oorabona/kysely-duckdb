import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['cjs', 'esm'],
  dts: true,
  splitting: false,
  sourcemap: true,
  clean: true,
  treeshake: true,
  minify: false,
  target: 'node20',
  external: ['@duckdb/node-api', 'kysely'],
  outDir: 'dist',
  banner: {
    js: '// kysely-duckdb - DuckDB dialect for Kysely',
  },
  esbuildOptions: options => {
    options.conditions = ['node']
  },
})

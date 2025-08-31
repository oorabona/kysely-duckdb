import type {
  ColumnMetadata,
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  Kysely,
  SchemaMetadata,
  TableMetadata,
} from 'kysely'

/**
 * DuckDB database introspector
 */
export class DuckDbIntrospector implements DatabaseIntrospector {
  readonly #db: Kysely<any>

  constructor(db: Kysely<any>) {
    this.#db = db
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    const result = await this.#db
      .selectFrom('information_schema.schemata' as any)
      .select(['schema_name as name'])
      .execute()

    return result.map(row => ({
      name: row.name as string,
    }))
  }

  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    let query = this.#db
      .selectFrom('information_schema.tables' as any)
      .select(['table_name as name', 'table_schema as schema', 'table_type as type'])
      .where('table_schema', '!=', 'information_schema')
      .where('table_schema', '!=', 'pg_catalog')

    if (options.withInternalKyselyTables !== true) {
      query = query.where('table_name', '!=', 'kysely_migration')
      query = query.where('table_name', '!=', 'kysely_migration_lock')
    }

    const result = await query.execute()

    return await Promise.all(
      result.map(async row => {
        const columns = await this.getColumns({
          schema: row.schema as string,
          table: row.name as string,
        })

        return {
          name: row.name as string,
          schema: row.schema as string,
          columns,
          isView: (row.type as string).toLowerCase() === 'view',
        }
      }),
    )
  }

  async getMetadata(options?: DatabaseMetadataOptions): Promise<DatabaseMetadata> {
    const tables = await this.getTables(options)

    return {
      tables,
    }
  }

  private async getColumns(table: { schema: string; table: string }): Promise<ColumnMetadata[]> {
    const result = await this.#db
      .selectFrom('information_schema.columns' as any)
      .select([
        'column_name as name',
        'data_type as dataType',
        'is_nullable as isNullable',
        'column_default as defaultValue',
        'character_maximum_length as maxLength',
        'numeric_precision as precision',
        'numeric_scale as scale',
      ])
      .where('table_schema', '=', table.schema)
      .where('table_name', '=', table.table)
      .orderBy('ordinal_position')
      .execute()

    return result.map(row => ({
      name: row.name as string,
      dataType: row.dataType as string,
      isNullable: (row.isNullable as string) === 'YES',
      defaultValue: row.defaultValue as string | null,
      hasDefaultValue: row.defaultValue !== null,
      isAutoIncrementing: false, // DuckDB doesn't have traditional auto-increment
    }))
  }
}

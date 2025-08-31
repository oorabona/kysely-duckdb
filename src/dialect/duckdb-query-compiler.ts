import { type DataTypeNode, DefaultQueryCompiler } from 'kysely'

/**
 * DuckDB-specific query compiler
 */
export class DuckDbQueryCompiler extends DefaultQueryCompiler {
  protected override visitDataType(node: DataTypeNode): void {
    // Handle DuckDB-specific data types using string comparison to avoid type conflicts
    const dataType = String(node.dataType)

    // Handle special DuckDB types
    if (dataType === 'uuid') {
      this.append('UUID')
      return
    }

    if (dataType === 'json') {
      this.append('JSON')
      return
    }

    if (dataType === 'geometry') {
      this.append('GEOMETRY')
      return
    }

    if (dataType === 'vector') {
      this.append('VECTOR')
      return
    }

    // Handle types with parameters like varchar(255)
    const typeWithParamsMatch = dataType.match(/^(\w+)(\(.+\))$/)
    if (typeWithParamsMatch) {
      const [, baseType, params] = typeWithParamsMatch
      if (baseType && params) {
        const typeMap: Record<string, string> = {
          varchar: 'VARCHAR',
          char: 'VARCHAR',
          text: 'VARCHAR',
          string: 'VARCHAR',
          decimal: 'DECIMAL',
          numeric: 'DECIMAL',
        }

        const mappedType = typeMap[baseType.toLowerCase()]
        if (mappedType) {
          this.append(`${mappedType}${params}`)
          return
        }

        // Fall back to uppercase for unknown types with parameters
        this.append(`${baseType.toUpperCase()}${params}`)
        return
      }
    }

    // Handle array types like varchar[]
    if (dataType.endsWith('[]')) {
      const baseType = dataType.slice(0, -2)
      const typeMap: Record<string, string> = {
        varchar: 'VARCHAR',
        text: 'VARCHAR',
        string: 'VARCHAR',
        char: 'VARCHAR',
        integer: 'INTEGER',
        int: 'INTEGER',
        bigint: 'BIGINT',
        boolean: 'BOOLEAN',
        bool: 'BOOLEAN',
        double: 'DOUBLE',
        float: 'REAL',
      }

      const mappedBaseType = typeMap[baseType.toLowerCase()] || baseType.toUpperCase()
      this.append(`${mappedBaseType}[]`)
      return
    }

    // Map simple types to DuckDB equivalents
    const typeMap: Record<string, string> = {
      integer: 'INTEGER',
      int: 'INTEGER',
      bigint: 'BIGINT',
      smallint: 'SMALLINT',
      tinyint: 'TINYINT',
      boolean: 'BOOLEAN',
      bool: 'BOOLEAN',
      real: 'REAL',
      float: 'REAL',
      double: 'DOUBLE',
      decimal: 'DECIMAL',
      numeric: 'DECIMAL',
      varchar: 'VARCHAR',
      text: 'VARCHAR',
      string: 'VARCHAR',
      char: 'VARCHAR',
      blob: 'BLOB',
      bytea: 'BLOB',
      date: 'DATE',
      time: 'TIME',
      timestamp: 'TIMESTAMP',
      timestamptz: 'TIMESTAMPTZ',
      interval: 'INTERVAL',
      hugeint: 'HUGEINT',
      uhugeint: 'UHUGEINT',
      utinyint: 'UTINYINT',
      usmallint: 'USMALLINT',
      uinteger: 'UINTEGER',
      ubigint: 'UBIGINT',
    }

    const mappedType = typeMap[dataType.toLowerCase()]
    if (mappedType) {
      this.append(mappedType)
      return
    }

    // Fall back to default behavior by converting dataType to uppercase
    this.append(dataType.toUpperCase())
  }

  // Let Kysely handle column definitions and default values properly

  /**
   * Compile DuckDB's INSERT OR REPLACE statement
   */
  protected visitInsertOrReplace(): string {
    return 'INSERT OR REPLACE'
  }

  /**
   * Compile DuckDB's INSERT OR IGNORE statement
   */
  protected visitInsertOrIgnore(): string {
    return 'INSERT OR IGNORE'
  }
}

import { type ChDataType } from '@clickhouse-schema-data-types/index'

export interface SchemaValue {
type: ChDataType
}
export type ChSchemaDefinition = {[key: string]: SchemaValue}
/**
 * ChSchemaOptions is used to define the options for a clickhouse table schema.
 *
 * @param database is the database to use for the table
 * @param table_name is the name of the table in clickhouse
 * @param on_cluster is the name of the cluster to use for the table
 * @param primary_key is the primary key for the table. if not specified, order_by must be specified
 * @param order_by is the order by clause for the table. if not specified, primary_key must be specified
 * @param engine is the engine to use for the table, default is MergeTree()
 * @param partition_by is the partition expression for the table. Can be any valid ClickHouse expression (e.g., "toYYYYMM(date_column)", "(toMonday(StartDate), EventType)", "sipHash64(userId) % 16")
 * @param additional_options is an string array of options that are appended to the end of the create table query
 */
export interface ChSchemaOptions<T extends ChSchemaDefinition> {
  database?: string
  table_name: string
  on_cluster?: string
  primary_key?: keyof T
  order_by?: keyof T
  engine?: string
  partition_by?: string
  additional_options?: string[]
}

/**
 * IClickhouseSchema is an interface that represents a clickhouse schema.
 */
interface IClickhouseSchema<T extends ChSchemaDefinition> {
  GetOptions: () => ChSchemaOptions<T>
  GetCreateTableQuery: () => string
  GetCreateTableQueryAsList: () => string[]
}

/**
 * ClickhouseSchema is a class that represents a clickhouse schema.
 * @param schema is the schema definition
 * @param options is the options for the schema
 */
export class ClickhouseSchema<SchemaDefinition extends ChSchemaDefinition> implements IClickhouseSchema<SchemaDefinition> {
  readonly schema: SchemaDefinition
  private readonly options: ChSchemaOptions<SchemaDefinition>

  constructor (schema: SchemaDefinition, options: ChSchemaOptions<SchemaDefinition>) {
    this.schema = schema
    this.options = options
  }

  GetOptions (): ChSchemaOptions<SchemaDefinition> {
    return this.options
  }

  /**
   * Determines if the engine requires a sorting key.
   * In ClickHouse, MergeTree family engines require ORDER BY or PRIMARY KEY.
   */
  private requiresOrderKey (engine: string): boolean {
    return /MergeTree/i.test(engine)
  }

  GetCreateTableQuery (): string {
    const engine = this.options.engine ?? 'MergeTree()'

    // Only enforce key requirement for MergeTree engines
    if (this.requiresOrderKey(engine) && this.options.primary_key === undefined && this.options.order_by === undefined) {
      throw new Error('One of order_by or primary_key must be specified')
    }

    const columns = Object.entries(this.schema as ChSchemaDefinition)
      .map(([name, field]) => {
        let defaultStr = ''
        if (field.type.default !== undefined) {
          // Use type-specific default SQL formatting if available
          const defaultSql = field.type.getDefaultSql?.()
          if (defaultSql !== undefined) {
            defaultStr = ` DEFAULT ${defaultSql}`
          } else {
            // Fallback to default behavior: JSON.stringify with quote replacement
            defaultStr = ` DEFAULT ${JSON.stringify(field.type.default)}`.replace(/"/g, "'")
          }
        }
        return `${name} ${field.type}${defaultStr}`
      }
      )
      .join(',\n')

    let additionalOptions = ''
    if (this.options.additional_options !== undefined) {
      additionalOptions = `${this.options.additional_options.join('\n')}`
    }
    const createTableQuery = [
      `CREATE TABLE IF NOT EXISTS ${this.options.database !== undefined ? `${this.options.database}.` : ''}${this.options.table_name}${this.options.on_cluster !== undefined ? ` ON CLUSTER ${this.options.on_cluster}` : ''}`,
      `(\n${columns}\n)`,
      `ENGINE = ${engine}`,
      this.options.partition_by !== undefined ? `PARTITION BY ${this.options.partition_by}` : '',
      this.options.order_by !== undefined ? `ORDER BY ${this.options.order_by.toString()}` : '',
      this.options.primary_key !== undefined ? `PRIMARY KEY ${this.options.primary_key.toString()}` : '',
      additionalOptions
    ].filter(part => part.trim().length > 0).join('\n')

    return `${createTableQuery};`
  }

  /**
   *
   * @returns the create table query as a list of strings
   */
  GetCreateTableQueryAsList (): string[] {
    return this.GetCreateTableQuery().split('\n')
  }

  /**
   *
   * @returns the create table query as a string
   */
  toString (): string {
    return this.GetCreateTableQuery()
  }
}
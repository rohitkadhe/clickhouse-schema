import { type ChDataType } from '@clickhouse-schema-data-types/index'

type IsType<T, U> = T extends U ? true : false

type OmitKeysOfTypes<T extends ChSchemaDefinition, U> = {
  [K in keyof T as IsType<T[K]['type']['typeStr'], U> extends true ? never : K]: T[K]
}

export interface SchemaValue {
  type: ChDataType
}

export type ChSchemaDefinition = Record<string, SchemaValue>

export interface ChSchemaOptions<T extends ChSchemaDefinition> {
  database?: string
  table_name: string
  on_cluster?: string
  primary_key?: keyof OmitKeysOfTypes<T, "Object('JSON')">
  order_by?: keyof OmitKeysOfTypes<T, "Object('JSON')">
  engine?: string
  additional_options?: string[]
}

interface IClickhouseSchema<T extends ChSchemaDefinition> {
  GetOptions: () => ChSchemaOptions<T>
  GetCreateTableQuery: () => string
  GetCreateTableQueryAsList: () => string[]
}

export class ClickhouseSchema<SchemaDefinition extends ChSchemaDefinition>
  implements IClickhouseSchema<SchemaDefinition> {

  readonly schema: SchemaDefinition
  private readonly options: ChSchemaOptions<SchemaDefinition>

  constructor(schema: SchemaDefinition, options: ChSchemaOptions<SchemaDefinition>) {
    this.schema = schema
    this.options = options
  }

  GetOptions(): ChSchemaOptions<SchemaDefinition> {
    return this.options
  }

  private requiresOrderKey(engine: string): boolean {
    return /MergeTree/i.test(engine)
  }

  GetCreateTableQuery(): string {
    const engine = this.options.engine ?? 'MergeTree()'

    if (
      this.requiresOrderKey(engine) &&
      this.options.primary_key === undefined &&
      this.options.order_by === undefined
    ) {
      throw new Error('One of order_by or primary_key must be specified')
    }

    const columns = Object.entries(this.schema as ChSchemaDefinition)
      .map(([name, field]) => {
        let defaultStr = ''

        if (field.type.default !== undefined) {
          const defaultSql = field.type.getDefaultSql?.()

          if (defaultSql !== undefined) {
            defaultStr = ` DEFAULT ${defaultSql}`
          } else {
            defaultStr = ` DEFAULT ${JSON.stringify(field.type.default)}`.replace(/"/g, "'")
          }
        }

        return `${name} ${field.type}${defaultStr}`
      })
      .join(',\n')

    let additionalOptions = ''
    if (this.options.additional_options !== undefined) {
      additionalOptions = this.options.additional_options.join('\n')
    }

    const createTableQuery = [
      `CREATE TABLE IF NOT EXISTS ${
        this.options.database !== undefined
          ? `${this.options.database}.`
          : ''
      }${this.options.table_name}${
        this.options.on_cluster !== undefined
          ? ` ON CLUSTER ${this.options.on_cluster}`
          : ''
      }`,
      `(\n${columns}\n)`,
      `ENGINE = ${engine}`,
      this.options.order_by !== undefined
        ? `ORDER BY ${this.options.order_by.toString()}`
        : '',
      this.options.primary_key !== undefined
        ? `PRIMARY KEY ${this.options.primary_key.toString()}`
        : '',
      additionalOptions
    ]
      .filter(part => part.trim().length > 0)
      .join('\n')

    return `${createTableQuery};`
  }

  GetCreateTableQueryAsList(): string[] {
    return this.GetCreateTableQuery().split('\n')
  }

  toString(): string {
    return this.GetCreateTableQuery()
  }
}
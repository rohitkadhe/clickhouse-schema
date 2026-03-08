import { type ChDataType } from '@clickhouse-schema-data-types/index'

/**
 * ChNullable is a class that represents a Clickhouse Nullable data type
 * @param T - The inner type of the Nullable. Must be a primitive type
 */
export class ChNullable<T extends ChDataType> implements ChDataType {
  readonly typeStr
  readonly typeScriptType!: T['typeScriptType'] | null
  readonly default?: T['typeScriptType'] | null

  constructor (t: T, defaultVal?: T['typeScriptType'] | null) {
    this.typeStr = `Nullable(${t['typeStr']})`
    this.default = defaultVal
  }

  toString (): string {
    return this.typeStr
  }
}

import { type ChPrimitiveType, type ChDataType } from '@clickhouse-schema-data-types/index'

/**
 * ChTuple is a class that represents a Clickhouse Tuple data type
 * @param V1 - The first element of the tuple
 * @param V2 - The second element of the tuple
 */
export class ChTuple<V1 extends ChDataType, V2 extends ChDataType> implements ChDataType {
  readonly typeStr
  readonly typeScriptType!: [V1['typeScriptType'], V2['typeScriptType']]
  readonly default?: [V1['typeScriptType'], V2['typeScriptType']]

  constructor (v1: V1, v2: V2, defaultValue?: [V1['typeScriptType'], V2['typeScriptType']]) {
    this.typeStr = `Tuple(${v1['typeStr']}, ${v2['typeStr']})`
    this.default = defaultValue
  }

  toString (): string {
    return this.typeStr
  }

  getDefaultSql (): string | undefined {
    if (this.default === undefined) {
      return undefined
    }
    const [x, y] = this.default
    return `(${x}, ${y})`
  }
}

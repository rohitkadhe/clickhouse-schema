import { type ChPrimitiveType, type ChDataType } from '@clickhouse-schema-data-types/index'

/**
 * ChPoint is a class that represents a Clickhouse Point data type
 */
export class ChPoint implements ChDataType {
  readonly typeStr: 'Point' = 'Point' as const
  readonly typeScriptType!: [number, number]
  readonly default?: [number, number]

  constructor (defaultValue?: [number, number]) {
    this.default = defaultValue
  }

  toString (): string {
    return this.typeStr
  }

  getDefaultSql (): string | undefined {
    if (this.default === undefined) {
      return undefined
    }
    // Point type defaults should be formatted as tuple (x, y)
    const [x, y] = this.default
    return `(${x}, ${y})`
  }
}

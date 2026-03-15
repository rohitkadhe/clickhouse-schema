import { type ChDataType } from '@clickhouse-schema-data-types/index'

/**
 * ChRing is a class that represents a Clickhouse Ring data type
 * Ring is an array of Points, where each Point is a tuple of two floats
 */
export class ChRing implements ChDataType {
  readonly typeStr: 'Ring' = 'Ring' as const
  readonly typeScriptType!: Array<[number, number]>
  readonly default?: Array<[number, number]>

  constructor (defaultValue?: Array<[number, number]>) {
    this.default = defaultValue
  }

  toString (): string {
    return this.typeStr
  }

  getDefaultSql (): string | undefined {
    if (this.default === undefined) {
      return undefined
    }
    // Ring type defaults should be formatted as array of tuples: [(x1, y1), (x2, y2), ...]
    const points = this.default.map(([x, y]) => `(${x}, ${y})`).join(', ')
    return `[${points}]`
  }
}



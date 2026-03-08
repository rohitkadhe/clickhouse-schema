import { type ChDataType } from '@clickhouse-schema-data-types/index'

/**
 * ChTuple represents a ClickHouse Tuple with N elements.
 * @template T - a tuple of ChDataType elements
 */
export class ChTuple<T extends ChDataType[]> implements ChDataType {
  readonly typeStr: string
  readonly typeScriptType: { [K in keyof T]: T[K]['typeScriptType'] }
  readonly default?: { [K in keyof T]?: T[K]['typeScriptType'] }

  constructor(
    types: [...T],
    defaultValue?: { [K in keyof T]?: T[K]['typeScriptType'] }
  ) {
    this.typeStr = `Tuple(${types.map(t => t.toString()).join(', ')})`
    this.typeScriptType = types.map(t => t as any) as any
    this.default = defaultValue
  }

  toString(): string {
    return this.typeStr
  }

  getDefaultSql(): string | undefined {
    if (!this.default) return undefined
    const vals = (this.default as any[]).map(v => (v === undefined ? 'NULL' : typeof v === 'string' ? `'${v}'` : v))
    return `(${vals.join(', ')})`
  }
}
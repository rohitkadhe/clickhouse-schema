import { type ChSchemaDefinition } from '@clickhouse-schema-core/clickhouse_schema'
import { type ChDataType } from '@clickhouse-schema-data-types/index'

/**
 * Options for the new ClickHouse JSON type
 */
export interface ChJSONOptions {
  max_dynamic_paths?: number
  max_dynamic_types?: number
  /**
   * Path type hints, e.g. { 'some.path': CHUInt32() }
   */
  pathTypeHints?: Record<string, ChDataType>
  /**
   * Paths to skip
   */
  skipPaths?: string[]
  /**
   * Regular expressions for paths to skip
   */
  skipRegexp?: string[]
  /**
   * Use legacy Object('JSON') type instead of new JSON type
   * Note: Object('JSON') supports DEFAULT values, but requires allow_experimental_object_type = 1
   */
  useLegacyJsonType?: boolean
}

/**
 * ChJSON is a class that represents the ClickHouse JSON data type (new or legacy)
 */
export class ChJSON<T extends ChSchemaDefinition> implements ChDataType {
  readonly typeStr: 'JSON' | "Object('JSON')"
  readonly innerType: T
  readonly typeScriptType!: { [K in keyof T]: T[K]['type']['typeScriptType'] }
  readonly default?: { [K in keyof T]: T[K]['type']['typeScriptType'] }
  readonly options?: ChJSONOptions

  constructor(
    innerType: T,
    defaultValue?: { [K in keyof T]: T[K]['type']['typeScriptType'] },
    options?: ChJSONOptions
  ) {
    this.innerType = innerType
    this.default = defaultValue
    this.options = options
    this.typeStr = options?.useLegacyJsonType ? "Object('JSON')" : 'JSON'
  }

  toString(): string {
    if (this.options?.useLegacyJsonType) {
      return "Object('JSON')"
    }
    // Build the parameter list for the JSON type
    const params: string[] = []
    if (this.options) {
      if (this.options.max_dynamic_paths !== undefined) {
        params.push(`max_dynamic_paths=${this.options.max_dynamic_paths}`)
      }
      if (this.options.max_dynamic_types !== undefined) {
        params.push(`max_dynamic_types=${this.options.max_dynamic_types}`)
      }
      if (this.options.pathTypeHints) {
        for (const [path, type] of Object.entries(this.options.pathTypeHints)) {
          params.push(`${path} ${type.toString()}`)
        }
      }
      if (this.options.skipPaths) {
        for (const path of this.options.skipPaths) {
          params.push(`SKIP ${path}`)
        }
      }
      if (this.options.skipRegexp) {
        for (const regexp of this.options.skipRegexp) {
          params.push(`SKIP REGEXP '${regexp}'`)
        }
      }
    }
    if (params.length === 0) {
      return 'JSON'
    }
    return `JSON(${params.join(', ')})`
  }

  getDefaultSql(): string | undefined {
    if (this.default === undefined) {
      return undefined
    }
    // Both JSON and Object('JSON') type defaults need to be wrapped in single quotes
    // ClickHouse requires JSON defaults to be string literals
    // Note: Object('JSON') supports DEFAULT but requires allow_experimental_object_type = 1
    return `'${JSON.stringify(this.default)}'`
  }
}

import { type ChSchemaDefinition, type ClickhouseSchema } from '@clickhouse-schema-core/clickhouse_schema'
import { type ChDataType } from '@clickhouse-schema-data-types/index'

/** Infer is a type that takes a ChDataType and returns the typescript that it represents */
type Infer<T extends ChDataType> = T['typeScriptType']

/**  InferSchemaClickhouseSchemaType is a type that takes a ClickhouseSchema and returns the typescript that it represents */
export type InferClickhouseSchemaType<T extends ClickhouseSchema<any>> = { [K in keyof T['schema']]: Infer<T['schema'][K]['type']> }

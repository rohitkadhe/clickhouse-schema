import { type ChSchemaDefinition } from '@clickhouse-schema-core/clickhouse_schema'
import { ChArray } from '@clickhouse-schema-data-types/ch_array'
import { ChBoolean } from '@clickhouse-schema-data-types/ch_boolean'
import { ChDate, ChDate32, ChDateTime, ChDateTime64 } from '@clickhouse-schema-data-types/ch_date'
import { ChDecimal } from '@clickhouse-schema-data-types/ch_decimal'
import { ChFloat32, ChFloat64 } from '@clickhouse-schema-data-types/ch_float'
import { ChIPv4, ChIPv6 } from '@clickhouse-schema-data-types/ch_ip_address'
import { ChJSON, ChJSONOptions } from '@clickhouse-schema-data-types/ch_json'
import { ChPoint } from '@clickhouse-schema-data-types/ch_point'
import { ChRing } from '@clickhouse-schema-data-types/ch_ring'

import { ChEnum, ChLowCardinality } from '@clickhouse-schema-data-types/ch_low_ cardinality'
import { ChNullable } from '@clickhouse-schema-data-types/ch_nullable'
import { ChFixedString, ChString } from '@clickhouse-schema-data-types/ch_string'
import { ChUUID } from '@clickhouse-schema-data-types/ch_uuid'
import { ChUInt8, ChUInt16, ChUInt32, ChUInt64, ChUInt128, ChUInt256, ChInt8, ChInt16, ChInt32, ChInt64, ChInt128, ChInt256 } from '@clickhouse-schema-data-types/ch_integer'
import { ChTuple } from '@clickhouse-schema-data-types/ch_tuple'

/**
 * ChDataType is an interface that represents a Clickhouse data type
 */
export interface ChDataType {
  typeStr: string
  default?: any
  innerType?: any
  typeScriptType: any
  toString: () => string
  /**
   * Returns the SQL representation of the default value, or undefined if no default.
   * If not implemented, falls back to JSON.stringify with quote replacement.
   */
  getDefaultSql?: () => string | undefined
}

/**
 *
 * @param df default value of the UInt8
 * @returns a new ChUInt8 object
 */
export const CHUInt8 = <T extends ChUInt8['typeScriptType']> (df?: T): ChUInt8 => new ChUInt8(df)
/**
 *
 * @param df default value of the UInt16
 * @returns a new ChUInt16 object
 */
export const CHUInt16 = <T extends ChUInt16['typeScriptType']> (df?: T): ChUInt16 => new ChUInt16(df)
/**
 *
 * @param df default value of the UInt32
 * @returns a new ChUInt32 object
 */
export const CHUInt32 = <T extends ChUInt32['typeScriptType']> (df?: T): ChUInt32 => new ChUInt32(df)
/**
 *
 * @param df default value of the UInt64
 * @returns a new ChUInt64 object
 */
export const CHUInt64 = <T extends ChUInt64['typeScriptType']> (df?: T): ChUInt64 => new ChUInt64(df)
/**
 *
 * @param df default value of the UInt128
 * @returns a new ChUInt128 object
 */
export const CHUInt128 = <T extends ChUInt128['typeScriptType']> (df?: T): ChUInt128 => new ChUInt128(df)
/**
 *
 * @param df default value of the UInt256
 * @returns a new ChUInt256 object
 */
export const CHUInt256 = <T extends ChUInt256['typeScriptType']> (df?: T): ChUInt256 => new ChUInt256(df)
/**
 *
 * @param df default value of the Int8
 * @returns a new ChInt8 object
 */
export const CHInt8 = <T extends ChInt8['typeScriptType']> (df?: T): ChInt8 => new ChInt8(df)
/**
 *
 * @param df default value of the Int16
 * @returns a new ChInt16 object
 */
export const CHInt16 = <T extends ChInt16['typeScriptType']> (df?: T): ChInt16 => new ChInt16(df)
/**
 *
 * @param df default value of the Int32
 * @returns a new ChInt32 object
 */
export const CHInt32 = <T extends ChInt32['typeScriptType']> (df?: T): ChInt32 => new ChInt32(df)
/**
 *
 * @param df default value of the Int64
 * @returns a new ChInt64 object
 */
export const CHInt64 = <T extends ChInt64['typeScriptType']> (df?: T): ChInt64 => new ChInt64(df)
/**
 *
 * @param df default value of the Int128
 * @returns a new ChInt128 object
 */
export const CHInt128 = <T extends ChInt128['typeScriptType']> (df?: T): ChInt128 => new ChInt128(df)
/**
 *
 * @param df default value of the Int256
 * @returns a new ChInt256 object
 */
export const CHInt256 = <T extends ChInt256['typeScriptType']> (df?: T): ChInt256 => new ChInt256(df)
/**
 *
 * @param df default value of the Float32
 * @returns a new ChFloat32 object
 */
export const CHFloat32 = <T extends ChFloat32['typeScriptType']> (df?: T): ChFloat32 => new ChFloat32(df)
/**
 *
 * @param df default value of the Float64
 * @returns a new ChFloat64 object
 */
export const CHFloat64 = <T extends ChFloat64['typeScriptType']> (df?: T): ChFloat64 => new ChFloat64(df)
/**
 *
 * @param df default value of the Boolean
 * @returns a new ChBoolean object
 */
export const CHBoolean = <T extends ChBoolean['typeScriptType']> (df?: T): ChBoolean => new ChBoolean(df)
/**
 *
 * @param df default value of the String
 * @returns a new ChString object
 */
export const CHString = <T extends ChString['typeScriptType']> (df?: T): ChString => new ChString(df)
/**
 *
 * @param df default value of the UUID
 * @returns a new ChUUID object
 */
export const CHUUID = <T extends ChUUID['typeScriptType']> (df?: T): ChUUID => new ChUUID(df)
/**
 *
 * @param df default value of the Date
 * @returns a new ChDate object
 */
export const CHDate = <T extends ChDate['typeScriptType']> (df?: T): ChDate => new ChDate(df)
/**
 *
 * @param df default value of the Date32
 * @returns a new ChDate32 object
 */
export const CHDate32 = <T extends ChDate32['typeScriptType']> (df?: T): ChDate32 => new ChDate32(df)
/**
 *
 * @param df default value of the IPv4
 * @returns a new ChIPv4 object
 */
export const CHIPv4 = <T extends ChIPv4['typeScriptType']> (df?: T): ChIPv4 => new ChIPv4(df)
/**
 *
 * @param df default value of the IPv6
 * @returns a new ChIPv6 object
 */
export const CHIPv6 = <T extends ChIPv6['typeScriptType']> (df?: T): ChIPv6 => new ChIPv6(df)

/**
 *
 * @param df default value of the Point as a tuple [x, y]
 * @returns a new ChPoint object
 */
export const CHPoint = <T extends ChPoint['typeScriptType']>(df?: T): ChPoint => new ChPoint(df)

/**
 *
 * @param df default value of the Ring as an array of float pairs [[x1, y1], [x2, y2], ...]
 * @returns a new ChRing object
 */
export const CHRing = <T extends ChRing['typeScriptType']>(df?: T): ChRing => new ChRing(df)

/**
 *
 * @param df default value of the tuple [x, y]
 * @returns a new ChTuple object
 */
export const CHTuple = <Types extends ChDataType[], T extends ChTuple<Types>['typeScriptType'] >(types: [...Types], df?: T): ChTuple<Types> => new ChTuple(types, df)
/**
 *
 * @param precision precision of the decimal
 * @param scale scale of the decimal
 * @returns a new ChDecimal object
 */
export const CHDecimal = <P extends number, S extends number, T extends ChDecimal<P, S>['typeScriptType']>(precision: P, scale: S, df?: T): ChDecimal<P, S> => new ChDecimal(precision, scale, df)

/**
 *
 * @param length length of the fixed string
 * @returns a new ChFixedString object
 */
export const CHFixedString = <L extends number, T extends ChFixedString<L>['typeScriptType']>(length: L, df?: T): ChFixedString<L> => new ChFixedString(length, df)

/**
 *
 * @param timezone timezone of the DateTime
 * @returns a new ChDateTime object
 */
export const CHDateTime = <TZ extends string, T extends ChDateTime<TZ>['typeScriptType']>(timezone: TZ, df?: T): ChDateTime<TZ> => new ChDateTime(timezone, df)
/**
 *
 * @param precision precision of the DateTime64
 * @param timezone timezone of the DateTime64
 * @returns a new ChDateTime64 object
 */
export const CHDateTime64 = <P extends number, TZ extends string, T extends ChDateTime64<P, TZ>['typeScriptType'] >(precision: P, timezone: TZ, df?: T): ChDateTime64<P, TZ> => new ChDateTime64(precision, timezone, df)
/**
 *
 * @param schema schema definition for the JSON
 * @param df default value for the JSON
 * @param options options for the new ClickHouse JSON type
 * @returns a new ChJSON object
 */
export const CHJSON = <SD extends ChSchemaDefinition, T extends ChJSON<SD>['typeScriptType']>(schema: SD, df?: T, options?: ChJSONOptions): ChJSON<SD> => new ChJSON(schema, df, options)
/**
 *
 * @param t type of the array
 * @returns a new ChArray object
 */
export const CHArray = <IN extends ChDataType, T extends ChArray<IN>['typeScriptType']>(t: IN, df?: T): ChArray<IN> => new ChArray(t, df)
/**
 *
 * @param enumObj enum object. The keys are the enum names and the values are the enum values
 * @returns a new ChEnum object
 */
export const CHEnum = <IN extends Record<string, number>, T extends ChEnum<IN>['typeScriptType']>(enumObj: IN, df?: T): ChEnum<IN> => new ChEnum(enumObj, df)
/**
 *
 * @param type type of the low cardinality. Must be a string or fixed string
 * @returns a new ChLowCardinality object
 */
export const CHLowCardinality = <IN extends ChString | ChFixedString<number>, T extends ChLowCardinality<IN>['typeScriptType']>(type: IN, df?: T): ChLowCardinality<IN> => new ChLowCardinality(type, df)
/**
 *
 * @param type type of the nullable. Must be a primitive type
 * @returns a new ChNullable object
 */
export const CHNullable = <IN extends ChPrimitiveType, T extends ChNullable<IN>['typeScriptType']>(type: IN, df?: T): ChNullable<IN> => new ChNullable(type, df)

export const ClickhouseTypes = {
  CHUInt8,
  CHUInt16,
  CHUInt32,
  CHUInt64,
  CHUInt128,
  CHUInt256,
  CHInt8,
  CHInt16,
  CHInt32,
  CHInt64,
  CHInt128,
  CHInt256,
  CHFloat32,
  CHFloat64,
  CHDecimal,
  CHBoolean,
  CHString,
  CHUUID,
  CHDate,
  CHDate32,
  CHDateTime,
  CHDateTime64,
  CHFixedString,
  CHIPv4,
  CHIPv6,
  CHPoint,
  CHRing,
  CHTuple,
  CHJSON,
  CHArray,
  CHEnum,
  CHLowCardinality,
  CHNullable
}

export type ChPrimitiveType =
  ChUInt8 | ChUInt16 | ChUInt32 | ChUInt64 | ChUInt128 | ChUInt256 |
  ChInt8 | ChInt16 | ChInt32 | ChInt64 | ChInt128 | ChInt256 |
  ChFloat32 | ChFloat64 | ChDecimal<number, number> | ChBoolean |
  ChDate | ChDate32 | ChDateTime<string> | ChDateTime64<number, string> |
  ChUUID | ChFixedString<number> | ChString | ChIPv4 | ChIPv6 | ChPoint | ChRing

export type ChCompositeType = ChArray<ChArray<ChDataType> | ChDataType> | ChEnum<Record<string, number>> | ChNullable<ChPrimitiveType> | ChJSON<ChSchemaDefinition> | ChLowCardinality<ChString | ChFixedString<number>>

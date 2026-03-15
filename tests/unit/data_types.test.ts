import { ClickhouseTypes } from '@clickhouse-schema-data-types/index'

describe('Data Types Tests', () => {
  it('should correctly create integer data types with the correct typeStr', () => {
    const uint8 = ClickhouseTypes.CHUInt8(2)
    const uint16 = ClickhouseTypes.CHUInt16(2)
    const uint32 = ClickhouseTypes.CHUInt32(2)
    const uint64 = ClickhouseTypes.CHUInt64(2)
    const uint128 = ClickhouseTypes.CHUInt128(2)
    const uint256 = ClickhouseTypes.CHUInt256(2)
    const int8 = ClickhouseTypes.CHInt8(2)
    const int16 = ClickhouseTypes.CHInt16(2)
    const int32 = ClickhouseTypes.CHInt32(2)
    const int64 = ClickhouseTypes.CHInt64(2)
    const int128 = ClickhouseTypes.CHInt128(2)
    const int256 = ClickhouseTypes.CHInt256(2)

    expect(uint8.toString()).toEqual('UInt8')
    expect(uint16.toString()).toEqual('UInt16')
    expect(uint32.toString()).toEqual('UInt32')
    expect(uint64.toString()).toEqual('UInt64')
    expect(uint128.toString()).toEqual('UInt128')
    expect(uint256.toString()).toEqual('UInt256')

    expect(uint64.toString()).toEqual('UInt64')
    expect(int8.toString()).toEqual('Int8')
    expect(int16.toString()).toEqual('Int16')
    expect(int32.toString()).toEqual('Int32')
    expect(int64.toString()).toEqual('Int64')
    expect(int128.toString()).toEqual('Int128')
    expect(int256.toString()).toEqual('Int256')
  })

  it('should correctly create float data types with the correct typeStr', () => {
    const float32 = ClickhouseTypes.CHFloat32(2)
    const float64 = ClickhouseTypes.CHFloat64(2)

    expect(float32.toString()).toEqual('Float32')
    expect(float64.toString()).toEqual('Float64')
  })

  it('should correctly create boolean data type with the correct typeStr', () => {
    const bool = ClickhouseTypes.CHBoolean(true)
    expect(bool.toString()).toEqual('Boolean')
  })

  it('should correctly create array data type with the correct typeStr', () => {
    const stringArray = ClickhouseTypes.CHArray(ClickhouseTypes.CHString('arr'))
    const nestedArray = ClickhouseTypes.CHArray(ClickhouseTypes.CHArray(ClickhouseTypes.CHString('arr')))

    expect(stringArray.toString()).toEqual('Array(String)')
    expect(nestedArray.toString()).toEqual('Array(Array(String))')
  })

  it('should correctly create date data types with the correct typeStr', () => {
    const date = ClickhouseTypes.CHDate(new Date())
    const date32 = ClickhouseTypes.CHDate32(new Date())
    const dateTime64 = ClickhouseTypes.CHDateTime64(3, 'UTC', new Date())
    const dateTime = ClickhouseTypes.CHDateTime('UTC', new Date())

    expect(dateTime.toString()).toEqual('DateTime(\'UTC\')')
    expect(dateTime64.toString()).toEqual('DateTime64(3, \'UTC\')')

    expect(date.toString()).toEqual('Date')
    expect(date32.toString()).toEqual('Date32')
  })

  it('should correctly create a string data types with the correct typeStr', () => {
    const string = ClickhouseTypes.CHString()
    const fixedString = ClickhouseTypes.CHFixedString(10)

    expect(string.toString()).toEqual('String')
    expect(fixedString.toString()).toEqual('FixedString(10)')
  })

  it('should correctly create a uuid data type with the correct typeStr', () => {
    const uuid = ClickhouseTypes.CHUUID('123')
    expect(uuid.toString()).toEqual('UUID')
  })

  it('should correctly create a json data type with the correct typeStr', () => {
    const json = ClickhouseTypes.CHJSON({
      dateTime64: { type: ClickhouseTypes.CHDateTime64(3, 'UTC') },
      dateTime: { type: ClickhouseTypes.CHDateTime('UTC') },
      enum: { type: ClickhouseTypes.CHEnum({ POST: 1, PUT: 2, DELETE: 3, GET: 4 }) },
      array: { type: ClickhouseTypes.CHArray(ClickhouseTypes.CHString()) },
      fixedString: { type: ClickhouseTypes.CHFixedString(10) },
      json: { type: ClickhouseTypes.CHJSON({ k: { type: ClickhouseTypes.CHString() } }) }
    }, { array: [''], dateTime: new Date(), dateTime64: new Date(), enum: 'DELETE', fixedString: '', json: { k: '' } })
    expect(json.toString()).toEqual('JSON')
    expect(json.typeStr).toBe('JSON')
  })

  it('should create a json data type with options and generate correct SQL', () => {
    const json = ClickhouseTypes.CHJSON(
      { foo: { type: ClickhouseTypes.CHString() } },
      undefined,
      {
        max_dynamic_paths: 2048,
        max_dynamic_types: 64,
        pathTypeHints: { 'foo.bar': ClickhouseTypes.CHUInt32(), 'baz': ClickhouseTypes.CHString() },
        skipPaths: ['secret', 'ignore.me'],
        skipRegexp: ['private.*', 'tmp.*']
      }
    )
    expect(json.toString()).toBe(
      "JSON(max_dynamic_paths=2048, max_dynamic_types=64, foo.bar UInt32, baz String, SKIP secret, SKIP ignore.me, SKIP REGEXP 'private.*', SKIP REGEXP 'tmp.*')"
    )
    expect(json.typeStr).toBe('JSON')
  })

  it('should create a legacy json data type when useLegacyJsonType is true', () => {
    const json = ClickhouseTypes.CHJSON(
      { foo: { type: ClickhouseTypes.CHString() } },
      undefined,
      { useLegacyJsonType: true }
    )
    expect(json.toString()).toBe("Object('JSON')")
    expect(json.typeStr).toBe("Object('JSON')")
  })

  it('should create a legacy json data type with options but always return Object(\'JSON\')', () => {
    const json = ClickhouseTypes.CHJSON(
      { foo: { type: ClickhouseTypes.CHString() } },
      undefined,
      {
        max_dynamic_paths: 2048,
        max_dynamic_types: 64,
        pathTypeHints: { 'foo.bar': ClickhouseTypes.CHUInt32(), 'baz': ClickhouseTypes.CHString() },
        skipPaths: ['secret', 'ignore.me'],
        skipRegexp: ['private.*', 'tmp.*'],
        useLegacyJsonType: true
      }
    )
    expect(json.toString()).toBe("Object('JSON')")
    expect(json.typeStr).toBe("Object('JSON')")
  })

  it('should correctly create a enum data type with the correct typeStr', () => {
    const enumType = ClickhouseTypes.CHEnum({ POST: 1, PUT: 2, DELETE: 3, GET: 4 }, 'POST')
    expect(enumType.toString()).toEqual('Enum(\'POST\' = 1,\'PUT\' = 2,\'DELETE\' = 3,\'GET\' = 4)')
  })

  it('should correctly create a nullable data type with the correct typeStr', () => {
    const nullableString = ClickhouseTypes.CHNullable(ClickhouseTypes.CHString('nullable'))
    expect(nullableString.toString()).toEqual('Nullable(String)')
  })

  it('should correctly create a decimal data type with the correct typeStr', () => {
    const decimal = ClickhouseTypes.CHDecimal(10, 2, 10.2)
    expect(decimal.toString()).toEqual('Decimal(10,2)')
  })

  it('should correctly create a low cardinality data type with the correct typeStr', () => {
    const lowCardinality = ClickhouseTypes.CHLowCardinality(ClickhouseTypes.CHString('lowCardinality'))
    expect(lowCardinality.toString()).toEqual('LowCardinality(String)')
  })

  it('should correctly create a ip address data types with the correct typeStr', () => {
    const ipv4 = ClickhouseTypes.CHIPv4('192.122.0.0')
    const ipv6 = ClickhouseTypes.CHIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')
    expect(ipv4.toString()).toEqual('IPv4')
    expect(ipv6.toString()).toEqual('IPv6')
  })

  it('should correctly create a point data type with the correct typeStr', () => {
    const point = ClickhouseTypes.CHPoint()
    expect(point.toString()).toEqual('Point')
  })

  it('should correctly create a point data type with default value', () => {
    const point = ClickhouseTypes.CHPoint([10.5, 20.3])
    expect(point.toString()).toEqual('Point')
    expect(point.default).toEqual([10.5, 20.3])
  })

  it('should correctly create a ring data type with the correct typeStr', () => {
    const ring = ClickhouseTypes.CHRing()
    expect(ring.toString()).toEqual('Ring')
  })

  it('should correctly create a ring data type with default value', () => {
    const ring = ClickhouseTypes.CHRing([
      [0.0, 0.0],
      [1.0, 0.0],
      [1.0, 1.0],
      [0.0, 1.0],
      [0.0, 0.0]
    ])
    expect(ring.toString()).toEqual('Ring')
    expect(ring.default).toEqual([
      [0.0, 0.0],
      [1.0, 0.0],
      [1.0, 1.0],
      [0.0, 1.0],
      [0.0, 0.0]
    ])
  })

  it('should correctly create a tuple data type with the correct typeStr', () => {
    const tuple = ClickhouseTypes.CHTuple(
      [ClickhouseTypes.CHFloat64(), ClickhouseTypes.CHFloat64()]
    )
  
    expect(tuple.toString()).toEqual('Tuple(Float64, Float64)')
  })
  
  it('should correctly create a tuple data type with default value', () => {
    const tuple = ClickhouseTypes.CHTuple([ClickhouseTypes.CHFloat64(), ClickhouseTypes.CHFloat64()], [10.5, 10.5])
  
    expect(tuple.toString()).toEqual('Tuple(Float64, Float64)')
    expect(tuple.getDefaultSql()).toEqual('(10.5, 10.5)')
  })

  it('should return undefined from getDefaultSql when Point has no default', () => {
    const point = ClickhouseTypes.CHPoint()
    expect(point.getDefaultSql()).toBeUndefined()
  })

  it('should return undefined from getDefaultSql when Ring has no default', () => {
    const ring = ClickhouseTypes.CHRing()
    expect(ring.getDefaultSql()).toBeUndefined()
  })

  it('should return undefined from getDefaultSql when JSON has no default', () => {
    const json = ClickhouseTypes.CHJSON({ foo: { type: ClickhouseTypes.CHString() } })
    expect(json.getDefaultSql()).toBeUndefined()
  })

  it('should return undefined from getDefaultSql when Tuple has no default', () => {
    const tuple = ClickhouseTypes.CHTuple([ClickhouseTypes.CHFloat64(), ClickhouseTypes.CHFloat64()])
    expect(tuple.getDefaultSql()).toBeUndefined()
  })

  it('should handle Tuple getDefaultSql with undefined values (NULL case)', () => {
    const tuple = ClickhouseTypes.CHTuple(
      [ClickhouseTypes.CHFloat64(), ClickhouseTypes.CHString()],
      [10.5, undefined] as any
    )
    expect(tuple.getDefaultSql()).toEqual("(10.5, NULL)")
  })

  it('should handle Tuple getDefaultSql with string values', () => {
    const tuple = ClickhouseTypes.CHTuple(
      [ClickhouseTypes.CHString(), ClickhouseTypes.CHString()],
      ['hello', 'world']
    )
    expect(tuple.getDefaultSql()).toEqual("('hello', 'world')")
  })

  it('should handle Tuple getDefaultSql with mixed types', () => {
    const tuple = ClickhouseTypes.CHTuple(
      [ClickhouseTypes.CHFloat64(), ClickhouseTypes.CHString(), ClickhouseTypes.CHBoolean()],
      [10.5, 'test', true]
    )
    expect(tuple.getDefaultSql()).toEqual("(10.5, 'test', true)")
  })
})

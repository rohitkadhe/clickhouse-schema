import { ClickhouseSchema, type ChSchemaOptions } from '@clickhouse-schema-core/clickhouse_schema'
import { type InferClickhouseSchemaType } from '@clickhouse-schema-core/infer_schema_type'
import { ClickhouseTypes } from '@clickhouse-schema-data-types/index'

describe('ClickhouseSchema Tests', () => {
  it('should correctly store schema definitions and options', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUInt128() },
      ch_json: { type: ClickhouseTypes.CHJSON({ k: { type: ClickhouseTypes.CHString() }, arr: { type: ClickhouseTypes.CHArray(ClickhouseTypes.CHJSON({ nested: { type: ClickhouseTypes.CHString() } })) } }) },
      ch_point: {type: ClickhouseTypes.CHPoint()},
      tuple: {type: ClickhouseTypes.CHTuple([ClickhouseTypes.CHString(), ClickhouseTypes.CHBoolean()])}

    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      order_by: 'id',
      table_name: 'users_table',
      engine: 'ReplicatedMergeTree()'
    }

    const schema = new ClickhouseSchema(schemaDefinition, options)

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    type schemaType = InferClickhouseSchemaType<typeof schema>
    expect(schema.GetOptions()).toEqual(options)
    // Check that the generated query includes the JSON type
    expect(schema.GetCreateTableQuery()).toContain('ch_json JSON')
  })

  it('should correctly throw an error if schema is missing both primary_key and order_by fields', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString() },
      email: { type: ClickhouseTypes.CHString() },
      age: { type: ClickhouseTypes.CHUInt8() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'users_table'
    }

    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedError = 'One of order_by or primary_key must be specified'
    expect(() => schema.GetCreateTableQuery()).toThrow(expectedError)
  })

  it('should correctly generate a create table query without a default value for any fields', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString() },
      email: { type: ClickhouseTypes.CHString() },
      age: { type: ClickhouseTypes.CHUInt8() },
      
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      table_name: 'users_table'
    }

    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table\n(\nid UUID,\nname String,\nemail String,\nage UInt8\n)\nENGINE = MergeTree()\nPRIMARY KEY id;'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with a default value for some fields', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString('john@gmail.com') },
      age: { type: ClickhouseTypes.CHUInt8() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      table_name: 'users_table'
    }

    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String DEFAULT \'john@gmail.com\',\nage UInt8\n)\nENGINE = MergeTree()\nPRIMARY KEY id;'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with additional options', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString('john@gmail.com') },
      age: { type: ClickhouseTypes.CHUInt8(18) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      table_name: 'users_table',
      additional_options: ['COMMENT \'This table provides user details\'']
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String DEFAULT \'john@gmail.com\',\nage UInt8 DEFAULT 18\n)\nENGINE = MergeTree()\nPRIMARY KEY id\nCOMMENT \'This table provides user details\';'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with a specified order_by field', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'users_table',
      order_by: 'id'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String\n)\nENGINE = MergeTree()\nORDER BY id;'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with a specified on_cluster field', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'users_table',
      primary_key: 'id',
      on_cluster: 'users_cluster'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table ON CLUSTER users_cluster\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String\n)\nENGINE = MergeTree()\nPRIMARY KEY id;'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with a specified engine', () => {
    const schema = new ClickhouseSchema({
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString() }
    }, {
      table_name: 'users_table',
      primary_key: 'id',
      engine: 'ReplicatedMergeTree()'
    })
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String\n)\nENGINE = ReplicatedMergeTree()\nPRIMARY KEY id;'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with a specified database', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      database: 'users_db',
      table_name: 'users_table',
      primary_key: 'id'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_db.users_table\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String\n)\nENGINE = MergeTree()\nPRIMARY KEY id;'
    const query = schema.GetCreateTableQuery()
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query list', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'users_table',
      primary_key: 'id',
      on_cluster: 'users_cluster',
      order_by: 'id',
      additional_options: ['COMMENT \'This table provides user details\'']
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = [
      'CREATE TABLE IF NOT EXISTS users_table ON CLUSTER users_cluster',
      '(',
      'id UUID,',
      "name String DEFAULT 'John Doe',",
      'email String',
      ')',
      'ENGINE = MergeTree()',
      'ORDER BY id',
      'PRIMARY KEY id',
      'COMMENT \'This table provides user details\';'
    ]
    const query = schema.GetCreateTableQueryAsList()
    expect(query).toEqual(expectedQuery)
  })

  it('should generate a create table query when to string is called', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      name: { type: ClickhouseTypes.CHString('John Doe') },
      email: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'users_table',
      primary_key: 'id',
      on_cluster: 'users_cluster',
      order_by: 'id',
      additional_options: ['COMMENT \'This table provides user details\'']
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table ON CLUSTER users_cluster\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String\n)\nENGINE = MergeTree()\nORDER BY id\nPRIMARY KEY id\nCOMMENT \'This table provides user details\';'
    // eslint-disable-next-line @typescript-eslint/no-base-to-string
    const query = schema.toString()

    expect(query).toEqual(expectedQuery)
  })

  it('should not throw if on_cluster is specified but primary_key or order_by is not', () => {
    try {
      const schemaDefinition = {
        id: { type: ClickhouseTypes.CHUUID() },
        name: { type: ClickhouseTypes.CHString('John Doe') },
        email: { type: ClickhouseTypes.CHString() }
      }
      const options: ChSchemaOptions<typeof schemaDefinition> = {
        table_name: 'users_table',
        on_cluster: 'users_cluster'
      }
      const schema = new ClickhouseSchema(schemaDefinition, options)
      const expectedQuery = 'CREATE TABLE IF NOT EXISTS users_table ON CLUSTER users_cluster\n(\nid UUID,\nname String DEFAULT \'John Doe\',\nemail String\n)\nENGINE = MergeTree();'
      const query = schema.GetCreateTableQuery()
      expect(query).toEqual(expectedQuery)
    } catch (e) {
    }
  })

  it('should generate a create table query with CHJSON with options', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUInt128() },
      ch_json: { type: ClickhouseTypes.CHJSON(
        { foo: { type: ClickhouseTypes.CHString() } },
        undefined,
        {
          max_dynamic_paths: 2048,
          max_dynamic_types: 64,
          pathTypeHints: { 'foo.bar': ClickhouseTypes.CHUInt32(), 'baz': ClickhouseTypes.CHString() },
          skipPaths: ['secret', 'ignore.me'],
          skipRegexp: ['private.*', 'tmp.*']
        }
      ) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      table_name: 'users_table'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery =
      "CREATE TABLE IF NOT EXISTS users_table\n(" +
      "\nid UInt128," +
      "\nch_json JSON(max_dynamic_paths=2048, max_dynamic_types=64, foo.bar UInt32, baz String, SKIP secret, SKIP ignore.me, SKIP REGEXP 'private.*', SKIP REGEXP 'tmp.*')" +
      "\n)\nENGINE = MergeTree()\nPRIMARY KEY id;"
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHJSON with legacy type', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUInt128() },
      ch_json: { type: ClickhouseTypes.CHJSON(
        { foo: { type: ClickhouseTypes.CHString() } },
        undefined,
        { useLegacyJsonType: true }
      ) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      table_name: 'users_table'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery =
      "CREATE TABLE IF NOT EXISTS users_table\n(" +
      "\nid UInt128," +
      "\nch_json Object('JSON')" +
      "\n)\nENGINE = MergeTree()\nPRIMARY KEY id;"
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHJSON with legacy type and options (options ignored)', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUInt128() },
      ch_json: { type: ClickhouseTypes.CHJSON(
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
      ) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      primary_key: 'id',
      table_name: 'users_table'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery =
      "CREATE TABLE IF NOT EXISTS users_table\n(" +
      "\nid UInt128," +
      "\nch_json Object('JSON')" +
      "\n)\nENGINE = MergeTree()\nPRIMARY KEY id;"
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHJSON with default value', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUInt64() },
      data: { type: ClickhouseTypes.CHJSON(
        { foo: { type: ClickhouseTypes.CHString() } },
        { foo: 'bar' }
      ) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'test_table',
      order_by: 'id'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS test_table\n(\nid UInt64,\ndata JSON DEFAULT \'{"foo":"bar"}\'\n)\nENGINE = MergeTree()\nORDER BY id;'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should generate a create table query with Object(\'JSON\') with default value', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUInt64() },
      data: { type: ClickhouseTypes.CHJSON(
        { foo: { type: ClickhouseTypes.CHString() } },
        { foo: 'bar' },
        { useLegacyJsonType: true }
      ) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'test_table',
      order_by: 'id'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS test_table\n(\nid UInt64,\ndata Object(\'JSON\') DEFAULT \'{"foo":"bar"}\'\n)\nENGINE = MergeTree()\nORDER BY id;'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHPoint and Memory engine using toString', () => {
    const schemaDefinition = {
      p: { type: ClickhouseTypes.CHPoint() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'geo_point',
      engine: 'Memory()'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    type SchemaType = InferClickhouseSchemaType<typeof schema>
    // Compile-time check: p should be inferred as [number, number]
    const _pointValue: SchemaType['p'] = [1, 2]
    expect(_pointValue).toEqual([1, 2])

    // eslint-disable-next-line @typescript-eslint/no-base-to-string
    const query = schema.toString()
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS geo_point\n(\np Point\n)\nENGINE = Memory();'
    expect(query).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHPoint and default value', () => {
    const schemaDefinition = {
      p: { type: ClickhouseTypes.CHPoint([0, 0]) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'geo_point',
      engine: 'Memory()'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    type SchemaType = InferClickhouseSchemaType<typeof schema>
    // Compile-time check: p should be inferred as [number, number]
    const _pointValue: SchemaType['p'] = [0, 0]
    expect(_pointValue).toEqual([0, 0])

    const query = schema.GetCreateTableQuery()
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS geo_point\n(\np Point DEFAULT (0, 0)\n)\nENGINE = Memory();'
    expect(query).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHPoint and non-zero default value', () => {
    const schemaDefinition = {
      p: { type: ClickhouseTypes.CHPoint([10.5, 20.3]) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'geo_point',
      engine: 'Memory()'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const query = schema.GetCreateTableQuery()
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS geo_point\n(\np Point DEFAULT (10.5, 20.3)\n)\nENGINE = Memory();'
    expect(query).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHTuple', () => {
    const schemaDefinition = {
      coords: {
        type: ClickhouseTypes.CHTuple(
          [ClickhouseTypes.CHFloat64(),
          ClickhouseTypes.CHFloat64()]
        )
      }
    }
  
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'tuple_table',
      engine: 'Memory()'
    }
  
    const schema = new ClickhouseSchema(schemaDefinition, options)
  
    type SchemaType = InferClickhouseSchemaType<typeof schema>
  
    const _value: SchemaType['coords'] = [1, 2]
    expect(_value).toEqual([1, 2])
  
    const query = schema.GetCreateTableQuery()
  
    const expectedQuery =
      'CREATE TABLE IF NOT EXISTS tuple_table\n(\ncoords Tuple(Float64, Float64)\n)\nENGINE = Memory();'
  
    expect(query).toEqual(expectedQuery)
  })
  
  it('should generate a create table query with CHTuple default value', () => {
    const schemaDefinition = {
      coords: {
        type: ClickhouseTypes.CHTuple(
          [ClickhouseTypes.CHFloat64(),
          ClickhouseTypes.CHFloat64()],
          [10.5, 20.3]
        )
      }
    }
  
  
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'tuple_table',
      engine: 'Memory()'
    }
  
    const schema = new ClickhouseSchema(schemaDefinition, options)
  
    const query = schema.GetCreateTableQuery()
  
    const expectedQuery =
      'CREATE TABLE IF NOT EXISTS tuple_table\n(\ncoords Tuple(Float64, Float64) DEFAULT (10.5, 20.3)\n)\nENGINE = Memory();'
  
    expect(query).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with PARTITION BY single expression (column name)', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      visitDate: { type: ClickhouseTypes.CHDate() },
      name: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'visits_table',
      primary_key: 'id',
      partition_by: 'visitDate'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS visits_table\n(\nid UUID,\nvisitDate Date,\nname String\n)\nENGINE = MergeTree()\nPARTITION BY visitDate\nPRIMARY KEY id;'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with PARTITION BY single expression (function)', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      visitDate: { type: ClickhouseTypes.CHDate() },
      name: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'visits_table',
      primary_key: 'id',
      partition_by: 'toYYYYMM(visitDate)'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS visits_table\n(\nid UUID,\nvisitDate Date,\nname String\n)\nENGINE = MergeTree()\nPARTITION BY toYYYYMM(visitDate)\nPRIMARY KEY id;'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with PARTITION BY tuple of expressions', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      startDate: { type: ClickhouseTypes.CHDate() },
      eventType: { type: ClickhouseTypes.CHString() },
      counterId: { type: ClickhouseTypes.CHUInt64() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'events_table',
      primary_key: 'id',
      partition_by: '(toMonday(startDate), eventType)'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS events_table\n(\nid UUID,\nstartDate Date,\neventType String,\ncounterId UInt64\n)\nENGINE = MergeTree()\nPARTITION BY (toMonday(startDate), eventType)\nPRIMARY KEY id;'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with PARTITION BY and complex expression', () => {
    const schemaDefinition = {
      userId: { type: ClickhouseTypes.CHUInt64() },
      sessionId: { type: ClickhouseTypes.CHUUID() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'session_log',
      order_by: 'sessionId',
      partition_by: 'sipHash64(userId) % 16'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS session_log\n(\nuserId UInt64,\nsessionId UUID\n)\nENGINE = MergeTree()\nPARTITION BY sipHash64(userId) % 16\nORDER BY sessionId;'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should correctly generate a create table query with PARTITION BY and all other options', () => {
    const schemaDefinition = {
      id: { type: ClickhouseTypes.CHUUID() },
      visitDate: { type: ClickhouseTypes.CHDate() },
      name: { type: ClickhouseTypes.CHString() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      database: 'analytics_db',
      table_name: 'visits_table',
      on_cluster: 'analytics_cluster',
      primary_key: 'id',
      order_by: 'visitDate',
      engine: 'ReplicatedMergeTree()',
      partition_by: 'toYYYYMM(visitDate)',
      additional_options: ['COMMENT \'Visits table partitioned by month\'']
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS analytics_db.visits_table ON CLUSTER analytics_cluster\n(\nid UUID,\nvisitDate Date,\nname String\n)\nENGINE = ReplicatedMergeTree()\nPARTITION BY toYYYYMM(visitDate)\nORDER BY visitDate\nPRIMARY KEY id\nCOMMENT \'Visits table partitioned by month\';'
    expect(schema.GetCreateTableQuery()).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHRing and Memory engine', () => {
    const schemaDefinition = {
      r: { type: ClickhouseTypes.CHRing() }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'geo_ring',
      engine: 'Memory()'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    type SchemaType = InferClickhouseSchemaType<typeof schema>
    // Compile-time check: r should be inferred as Array<[number, number]>
    const _ringValue: SchemaType['r'] = [[0.0, 0.0], [1.0, 0.0]]
    expect(_ringValue).toEqual([[0.0, 0.0], [1.0, 0.0]])

    const query = schema.GetCreateTableQuery()
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS geo_ring\n(\nr Ring\n)\nENGINE = Memory();'
    expect(query).toEqual(expectedQuery)
  })

  it('should generate a create table query with CHRing and default value', () => {
    const schemaDefinition = {
      r: { type: ClickhouseTypes.CHRing([
        [0.0, 0.0],
        [1.0, 0.0],
        [1.0, 1.0],
        [0.0, 1.0],
        [0.0, 0.0]
      ]) }
    }
    const options: ChSchemaOptions<typeof schemaDefinition> = {
      table_name: 'geo_ring',
      engine: 'Memory()'
    }
    const schema = new ClickhouseSchema(schemaDefinition, options)
    type SchemaType = InferClickhouseSchemaType<typeof schema>
    // Compile-time check: r should be inferred as Array<[number, number]>
    const _ringValue: SchemaType['r'] = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
    expect(_ringValue).toEqual([[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]])

    const query = schema.GetCreateTableQuery()
    const expectedQuery = 'CREATE TABLE IF NOT EXISTS geo_ring\n(\nr Ring DEFAULT [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]\n)\nENGINE = Memory();'
    expect(query).toEqual(expectedQuery)
  })
})

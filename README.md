# ClickHouse-Schema Guide

**Version 3.1.0**

In ClickHouse, defining and managing table schemas and their associated types can be done either manually or through the ClickHouse-Schema library. This guide compares these two approaches to illustrate the simplicity and efficiency ClickHouse-Schema brings to your projects.

## Traditional Manual Query Approach

Traditionally, creating a table in clickhouse requires manually writing the SQL query and the interface in your code. This method is straightforward but prone to errors and inconsistencies, especially when schema changes occur.

### Create Table Query

```sql
CREATE TABLE IF NOT EXISTS students
(
    id UInt32,
    name String,
    height float32,
    age UInt8,
    weight Float64,,
    isStudent Boolean
)
# Manually defined
interface StudentsTableTypeManuallyDefined {
  id: number,
  name: string,
  age: number,
  height: number,
  weight: number,
  isStudent: boolean
}
```

**Did you notice any errors with the code below?**  These would not get caught till runtime

## Using ClickHouse-Schema

ClickHouse-Schema automates schema creation and ensures type safety with minimal code, providing a more robust and maintainable solution.

### Defining Schema

``` typescript
const studentsTableSchema = new ClickhouseSchema({
  id: { type: CHUInt32() },
  name: { type: CHString() },
  age: { type: CHUInt8() },
  height: { type: CHFloat32() },
  weight: { type: CHFloat64() },
  isStudent: { type: CHBoolean() }
}, {
  table_name: 'students',
  primary_key: 'id'
})

//Automatic type inference. If schema changes type automatically changes too
type StudentsTableType = InferClickhouseSchemaType<typeof studentsTableSchema>
```

## Getting Started

To start using ClickHouse-Schema in your projects, follow these steps:

1. **Installation**
   To install ClickHouse-Schema, run the following command in your terminal:

   ```bash
   npm install clickhouse-schema
   ```

2. **Create a Schema**

    Define your table schema and provide options such as the table name and primary key. This will enable automatic type inference, making your code more robust and maintainable.

    ``` typescript

    import {
      ClickhouseSchema, CHUInt32, CHString, CHUInt8, CHFloat32, CHFloat64, CHBoolean, InferClickhouseSchemaType
    } from 'clickhouse-schema'

    // Use types directly or import ClickhouseTypes object to get all the types in one place
    const studentsTableSchema = new ClickhouseSchema({
      id: { type: CHUInt32() },
      name: { type: CHString() },
      age: { type: CHUInt8() },
      height: { type: CHFloat32() },
      weight: { type: CHFloat64() },
      isStudent: { type: CHBoolean() }
    }, {
      table_name: 'students',
      primary_key: 'id'
    })
    type MyTableType = InferClickhouseSchemaType<typeof studentsTableSchema>
    ```

3. **Utilize Schema Methods**
    ClickHouse-Schema provides several methods to streamline working with your database schema:

    - Use `<your_schema>.GetCreateTableQuery()` or `<your_schema>.toString()` to generate the SQL `CREATE TABLE` query.
    - Use `<your_schema>.GetOptions()` to access the options passed when creating the table schema.
    - Use `<your_schema>.GetCreateTableQueryAsList()` to get the `CREATE TABLE` query as a list of strings, which can be helpful for debugging or logging.

## Supported Types

- Integer (signed and unsigned integers): `UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256` types
- Floating-point numbers: `Float32` and `Float64` types
- Decimal - `Decimal` type
- Boolean: `Boolean` type
- Strings: `String` and `FixedString` types
- Dates: `Date`, `Date32`, `DateTime` and `DateTime64` types
- Geometric: `Point` type
- JSON: `JSON` type and legacy `Object('JSON')` type (use `useLegacyJsonType=true` in the options)
- UUID: `UUID` type
- Arrays: `Array` type
- Nullable: `Nullable` type
- LowCardinality: `LowCardinality` and `Enum` types
- IP Addresses - `IPv4` and `IPv6`

And support for more types is coming!

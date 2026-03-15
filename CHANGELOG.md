# clickhouse-schema

## 3.2.0

### Minor Changes

- Support for ring type

## 3.1.2

### Patch Changes

- Update repo link

## 3.1.1

### Patch Changes

- support project

## 3.1.0

### Minor Changes

- support for point type, tuple type and bug fixes
- Add support for partition_by option

## 3.0.0

### Major Changes

- # clickhouse-schema## 3.0.0### Major Changes- **Breaking:** The `CHJSON` type now uses the new ClickHouse `JSON` type by default (production ready in ClickHouse 25.3+). This may break existing code that expects the legacy `Object('JSON')` type or the old API for path hints.- **Legacy Support:** You can opt-in to the legacy `Object('JSON')` type by passing `{ useLegacyJsonType: true }` in the options to `CHJSON`. All other options are ignored in legacy mode.- **Advanced Options:** The new `CHJSON` type supports advanced options: - `max_dynamic_paths`, `max_dynamic_types` - `pathTypeHints` (now type-safe, expects a mapping to `ChDataType`) - `skipPaths`, `skipRegexp`- **ClickHouse Compatibility:** If you see an error about experimental JSON type, set `SET allow_experimental_json_type = 1` in your ClickHouse server/session, or upgrade to ClickHouse 25.3+ where this is not required.

## 2.0.1

### Patch Changes

- 843fc2f: Update readme to reflect release changes

## 2.0.0

### Major Changes

- 6dbce6f: # 2.0.0

  - Added support for default value checking at compile time. This also means all datatypes are now functions so they need to be called unlike before. So `ClickhouseTypes.CHBoolean` becomes `ClickhouseTypes.CHBoolean()` and this is the true for all data types.
  - Refactored type inference to reduce LOC and make it more modular
  - Improved validation for `order_by` and `primary_key` options by checking for only compatible keys at compile time

## 1.0.4

### Patch Changes

- Fix bug causing error when on_cluser option is specified and primary_key or order_by is not

## 1.0.3

### Patch Changes

- Add repository to readme

## 1.0.2

### Patch Changes

- Update package json to add keywords

## 1.0.1

### Patch Changes

- b4eb756: Update readme instructions import so that users can copy paste

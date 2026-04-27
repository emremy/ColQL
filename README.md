# ColQL

`ColQL` is a TypeScript-first in-memory query and compact columnar storage engine for JavaScript and TypeScript.

It is designed for cases where you want to keep data in RAM, query it with a small fluent API, serialize it compactly, and avoid the memory overhead of storing every row as a JavaScript object. Depending on data shape, compact columnar storage can use significantly less memory than object arrays, potentially up to 5x-30x or more for narrow schemas.

## Why Columnar Storage?

A normal JavaScript object array stores repeated object shapes, property metadata, string values, and booleans as full JavaScript values:

```ts
[
  { id: 1, age: 25, status: "active", is_active: true },
  { id: 2, age: 42, status: "passive", is_active: false },
];
```

`ColQL` stores values by column instead:

```ts
{
  columns: {
    id: Uint32Array,
    age: Uint8Array,
    status: Uint8Array,
    is_active: BitSet,
  },
  rowCount: 2,
}
```

Numeric values use typed arrays, dictionary columns store compact numeric codes instead of repeated strings, and booleans are packed into bits.

## Install

```sh
npm install colql
```

## Example

```ts
import { table, column } from "colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
});

for (let i = 0; i < 10_000; i++) {
  users.insert({
    id: i,
    age: i % 100,
    status: i % 2 === 0 ? "active" : "passive",
    is_active: i % 2 === 0,
  });
}

const result = users
  .where("age", ">=", 18)
  .where("status", "=", "active")
  .select(["id", "age", "status"])
  .limit(10)
  .toArray();

const averageAdultAge = users.where("age", ">=", 18).avg("age");
const oldestUsers = users.top(10, "age");

console.log(result, averageAdultAge, oldestUsers);
```

## Column Types

`ColQL` uses PostgreSQL-inspired names where they fit JavaScript typed-array storage:

```ts
column.int16();
column.int32();
column.uint8();
column.uint16();
column.uint32();
column.float32();
column.float64();
column.boolean();
column.dictionary(["active", "passive"] as const);
```

Optional aliases are included:

```ts
column.smallint(); // int16
column.integer(); // int32
column.real(); // float32
column.doublePrecision(); // float64
```

## Query API

Queries are lazy by default. These methods build a query pipeline and do not scan or materialize result rows immediately:

```ts
where(columnName, operator, value)
whereIn(columnName, values)
whereNotIn(columnName, values)
select(columnNames)
limit(n)
offset(n)
```

Execution happens only when you call:

```ts
toArray()
first()
count()
size()
isEmpty()
forEach(callback)
sum(columnName)
avg(columnName)
min(columnName)
max(columnName)
top(n, columnName)
bottom(n, columnName)
```

Supported operators:

```ts
"="
"!="
">"
">="
"<"
"<="
"in"
"not in"
```

`toArray()` materializes rows and therefore uses memory proportional to the result size. `where`, `select`, `limit`, `offset`, `count`, `first`, and numeric aggregations are designed to avoid unnecessary intermediate allocations. `count()` and aggregations scan matching row indexes without materializing row objects.

## Streaming

Tables and queries are iterable. Iteration is lazy and respects filters, selected columns, offsets, and limits:

```ts
for (const row of users.where("status", "=", "active").select(["id", "age"]).limit(100)) {
  console.log(row.id, row.age);
}
```

This does not allocate a full result array. Each row object is materialized only as it is yielded.

## Serialization

Tables can be serialized into one compact binary `ArrayBuffer` and restored later:

```ts
const buffer = users.serialize();
const restored = table.deserialize(buffer);

console.log(restored.count());
```

The binary format is explicit and dependency-free:

- 8-byte magic header: `COLQL003`.
- 4-byte little-endian JSON header length.
- UTF-8 JSON header with version, row count, capacity, schema metadata, dictionary values, and column payload offsets.
- Raw typed-array bytes for numeric and dictionary columns.
- Raw `Uint8Array` BitSet bytes for boolean columns.

Serialization reads compact column storage directly and does not materialize row objects. Deserialization rebuilds typed-array and BitSet storage from the payload views where possible.

## Aggregations

Numeric aggregations operate directly on column storage and work with filtered queries:

```ts
users.count();
users.sum("age");
users.avg("age");
users.min("age");
users.max("age");

const activeAverageAge = users.where("status", "=", "active").avg("age");
```

Aggregations do not create intermediate arrays and do not materialize rows. They scan the selected row range once, applying all filters in the same pass.

## Top And Bottom

`top(n, columnName)` and `bottom(n, columnName)` return the highest or lowest `n` rows by a numeric column:

```ts
const oldest = users.top(10, "age");

const activeYoungest = users
  .where("status", "=", "active")
  .select(["id", "age", "status"])
  .bottom(10, "age");
```

These methods are intentionally not `orderBy`. They use a bounded binary heap and keep only `n` candidate row indexes in memory while scanning, so the selection cost is `O(rows * log n)` instead of sorting the full dataset with `O(rows * log rows)` memory pressure. Only the final `n` rows are materialized.

## TypeScript Inference

Schemas infer insert, where, select, aggregation, and top/bottom types:

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
});

users.insert({
  id: 1,
  age: 25,
  status: "active",
  is_active: true,
});

const selected = users.select(["id", "status"]).toArray();
// Array<{ id: number; status: "active" | "passive" }>

users.where("age", ">", 18);
users.whereIn("status", ["active"]);
users.sum("age");
users.top(5, "age");
```

With const dictionaries, invalid values are rejected at compile time where TypeScript can see them, and at runtime during insert/query construction.

## RAM-Friendly Design

`ColQL` avoids storing rows internally. Tables own one storage object per column:

- Numeric columns use typed arrays such as `Uint8Array`, `Uint32Array`, and `Float64Array`.
- Dictionary columns encode strings as numeric codes and choose `Uint8Array`, `Uint16Array`, or `Uint32Array` based on dictionary size.
- Boolean columns use a small `BitSet` backed by `Uint8Array`.
- Tables grow dynamically by doubling capacity and resizing each column storage.
- Query filters scan row indexes and read only the columns needed for filtering.
- Chained `where` filters are evaluated in one execution pass.
- Selected rows are materialized only when output is requested.
- Serialization copies compact column bytes, not row objects.

## Benchmarks

Benchmarks are dependency-free scripts in `benchmarks/` and run against the built package:

```sh
npm run build
npm run benchmark:memory
npm run benchmark:query
npm run benchmark:serialization
```

Default benchmark runs use 100,000 rows averaged over 3 runs. To include the 1,000,000 row scenario:

```sh
COLQL_BENCH_LARGE=1 npm run benchmark:memory
COLQL_BENCH_LARGE=1 npm run benchmark:query
COLQL_BENCH_LARGE=1 npm run benchmark:serialization
```

Recent local results on this workspace:

```txt
100,000 rows, average over 3 runs:
Object Array tracked total: 6.22 MB
ColQL tracked total:        0.81 MB
tracked total reduction:    ~7.70x

1,000,000 rows, average over 3 runs:
Object Array tracked total: 63.36 MB
ColQL tracked total:        6.15 MB
tracked total reduction:    ~10.30x
```

`heapUsed` is useful for comparing JavaScript object pressure. The benchmark also reports `arrayBuffers`, because typed arrays are backed by ArrayBuffers and may not appear in `heapUsed` alone. The most honest memory comparison is the reported tracked total: `heapUsed + arrayBuffers`.

Query results vary by runtime and hardware. On the same local run with 1,000,000 rows averaged over 3 runs:

```txt
array.filter where: 10.843ms
ColQL where count: 22.231ms
array select + limit: 8.896ms
ColQL select + limit: 0.077ms
```

Serialization benchmark on the same local run:

```txt
1,000,000 rows, average over 3 runs:
serialize:   0.565ms
deserialize: 0.757ms
size:        6.13 MB
```

The query benchmark highlights the current tradeoff: raw array filtering can be very fast in V8, while `ColQL` is designed to avoid object-array storage, serialize compactly, and stop early for RAM-safe operations such as `select + limit`.

## DX Helpers

```ts
users.size();
users.isEmpty();
users.get(0);
users.getSchema();
users.whereIn("status", ["active"]);
users.whereNotIn("status", ["blocked"]);
```

`users.schema` remains available as the schema definition property for backward compatibility. `getSchema()` is the method-form helper.

## Intentionally Not Included in v0.0.3

`orderBy`, `groupBy`, `join`, and `distinct` are not included in v0.0.3 because they usually require materialization or additional memory structures. The first releases focus on RAM-safe operations and a small, predictable API.

Indexing and SQL parser support are also intentionally out of scope for v0.0.3.

## Current Limitations

- Data is in-memory only; serialization produces a compact binary buffer but does not manage files directly.
- Columns are required; nullable values are not implemented yet.
- Numeric columns rely on JavaScript typed-array coercion rules.
- There are no secondary indexes yet, so filters scan row indexes.
- Query operations are intentionally small: no sorting, grouping, joining, or distinct selection.

## v0.0.3 Roadmap

- Compact numeric, dictionary, and boolean storage.
- PostgreSQL-inspired column factory names.
- Lazy `where`, `select`, `limit`, and `offset` pipeline.
- `toArray`, `first`, `count`, `forEach`, and iterator execution.
- Memory-safe `sum`, `avg`, `min`, and `max`.
- Heap-based `top` and `bottom` without full dataset sorting.
- Binary serialization and deserialization.
- Predicate execution in one row scan for chained filters.
- Dependency-free memory, query, and serialization benchmarks.
- Type-safe schema inference for inserts, filters, selected rows, and numeric-only APIs.
- Detailed tests for storage correctness, resizing, query behavior, laziness, serialization, streaming, and type inference.

## Development

```sh
npm test
npm run build
npm run benchmark:memory
npm run benchmark:query
npm run benchmark:serialization
```

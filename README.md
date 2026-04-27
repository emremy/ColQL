# memql

`memql` is a TypeScript-first in-memory query and compact columnar storage engine for JavaScript and TypeScript.

It is designed for cases where you want to keep data in RAM, query it with a small fluent API, and avoid the memory overhead of storing every row as a JavaScript object. Depending on data shape, compact columnar storage can use significantly less memory than object arrays, potentially up to 5x-30x.

## Why Columnar Storage?

A normal JavaScript object array stores repeated object shapes, property metadata, string values, and booleans as full JavaScript values:

```ts
[
  { id: 1, age: 25, status: "active", is_active: true },
  { id: 2, age: 42, status: "passive", is_active: false },
];
```

`memql` stores values by column instead:

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
npm install memql
```

## Example

```ts
import { table, column } from "memql";

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

console.log(result);

const averageAdultAge = users.where("age", ">=", 18).avg("age");
const oldestUsers = users.top(10, "age");
```

## Column Types

`memql` uses PostgreSQL-inspired names where they fit JavaScript typed-array storage:

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
select(columnNames)
limit(n)
offset(n)
```

Execution happens only when you call:

```ts
toArray()
first()
count()
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

Schemas infer insert, where, and select types:

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
users.where("status", "=", "active");
```

With const dictionaries, invalid values are rejected at compile time where TypeScript can see them, and at runtime during insert/query construction.

## RAM-Friendly Design

`memql` avoids storing rows internally. Tables own one storage object per column:

- Numeric columns use typed arrays such as `Uint8Array`, `Uint32Array`, and `Float64Array`.
- Dictionary columns encode strings as numeric codes and choose `Uint8Array`, `Uint16Array`, or `Uint32Array` based on dictionary size.
- Boolean columns use a small `BitSet` backed by `Uint8Array`.
- Tables grow dynamically by doubling capacity and resizing each column storage.
- Query filters scan row indexes and read only the columns needed for filtering.
- Chained `where` filters are evaluated in one execution pass.
- Selected rows are materialized only when output is requested.

## Benchmarks

Benchmarks are dependency-free scripts in `benchmarks/` and run against the built package:

```sh
npm run build
npm run benchmark:memory -- 100000
npm run benchmark:query -- 100000
```

Recent local results on this workspace:

```txt
100,000 rows memory:
Object Array heapUsed:      6.22 MB
memql heapUsed:             0.07 MB
Object Array tracked total: 6.22 MB
memql tracked total:        0.84 MB
tracked total reduction:    ~7.41x

1,000,000 rows memory:
Object Array heapUsed:      63.37 MB
memql heapUsed:             0.07 MB
Object Array tracked total: 63.37 MB
memql tracked total:        6.20 MB
tracked total reduction:    ~10.22x
```

`heapUsed` is included because it is useful for comparing JavaScript object pressure. The benchmark also reports `arrayBuffers`, because typed arrays are backed by ArrayBuffers and may not appear in `heapUsed` alone.

Query results vary by runtime and hardware. On the same local run with 1,000,000 rows:

```txt
array.filter where: 9.477ms
memql where count: 26.003ms
array filter count: 9.465ms
memql count: 19.686ms
array select + limit: 8.527ms
memql select + limit: 0.33ms
```

The query benchmark highlights the current tradeoff: raw array filtering can be very fast in V8, while `memql` is designed to avoid object-array storage and to stop early for RAM-safe operations such as `select + limit`.

## Intentionally Not Included in v0.0.2

`orderBy`, `groupBy`, `join`, and `distinct` are not included in v0.0.2 because they usually require materialization or additional memory structures. The first releases focus on RAM-safe operations and a small, predictable API.

Indexing and SQL parser support are also intentionally out of scope for v0.0.2.

## Current Limitations

- Data is in-memory only and is not persisted to disk.
- Columns are required; nullable values are not implemented yet.
- Numeric columns rely on JavaScript typed-array coercion rules.
- There are no secondary indexes yet, so filters scan row indexes.
- Query operations are intentionally small: no sorting, grouping, joining, or distinct selection.

## v0.0.2 Roadmap

- Compact numeric, dictionary, and boolean storage.
- PostgreSQL-inspired column factory names.
- Lazy `where`, `select`, `limit`, and `offset` pipeline.
- `toArray`, `first`, `count`, `forEach`, and iterator execution.
- Memory-safe `sum`, `avg`, `min`, and `max`.
- Heap-based `top` and `bottom` without full dataset sorting.
- Predicate execution in one row scan for chained filters.
- Dependency-free memory and query benchmarks.
- Type-safe schema inference for inserts, filters, and selected rows.
- Detailed tests for storage correctness, resizing, query behavior, laziness, and type inference.

## Development

```sh
npm test
npm run build
npm run benchmark:memory -- 100000
npm run benchmark:query -- 100000
```

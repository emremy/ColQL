
# ColQL

[![CI](https://github.com/emremy/ColQL/actions/workflows/ci.yml/badge.svg)](https://github.com/emremy/ColQL/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@colql/colql.svg)](https://www.npmjs.com/package/@colql/colql)
[![npm downloads](https://img.shields.io/npm/dm/@colql/colql.svg)](https://www.npmjs.com/package/@colql/colql)
[![license](https://img.shields.io/npm/l/@colql/colql.svg)](LICENSE)

A memory-efficient in-memory columnar query engine for TypeScript.

> Up to ~7x less memory than JavaScript object arrays (based on tracked total memory).

---

## 🚀 Quick Example

```ts
import { table, column } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean()
});

users.insert({
  id: 1,
  age: 25,
  status: "active",
  is_active: true
});

const activeAdults = users
  .where("age", ">=", 18)
  .where("status", "=", "active")
  .select(["id"])
  .limit(5)
  .toArray();
```

---

## 📦 Install

```sh
npm install @colql/colql
```

---

## 🧠 Why ColQL

ColQL is built for workloads where keeping data in memory is useful, but JavaScript object arrays are too bulky.

Instead of storing rows like this:

```ts
[
  { id: 1, age: 25, status: "active", is_active: true }
]
```

ColQL stores compact columns:

```ts
{
  id: Uint32Array,
  age: Uint8Array,
  status: Uint8Array,
  is_active: BitSet
}
```

That means:

- Numeric data lives in `TypedArray` storage
- Repeated strings are dictionary-encoded into small integer codes
- Booleans are packed into bits
- Queries execute lazily without building intermediate arrays
- Rows are materialized only when needed

ColQL is not a SQL parser or ORM. It is a small, predictable, TypeScript-first columnar engine.

---

## ⚡ Key Design Principle

ColQL is **lazy by default**:

- Queries do not execute immediately
- No intermediate arrays are created
- Data is processed row-by-row
- Execution stops early when possible (`limit`, `first`)

This enables efficient handling of large in-memory datasets.

---

## 🎯 When to use ColQL

ColQL is useful when:

- You work with large in-memory datasets (100K+ rows)
- JavaScript object arrays consume too much memory
- You need filtering, aggregation, or streaming without allocations
- You want predictable memory usage

ColQL is NOT intended for:

- relational joins
- complex SQL workloads
- transactional systems

---

## 📊 Benchmarks

Run locally:

```sh
npm run build
npm run benchmark:memory
npm run benchmark:query
```

Example (100,000 rows, averaged):

| Storage | heapUsed | arrayBuffers | tracked total |
|--------|--------:|------------:|--------------:|
| Object Array | 6.22 MB | 0.00 MB | 6.22 MB |
| ColQL | 0.04 MB | 0.77 MB | 0.81 MB |

**Tracked total = heapUsed + arrayBuffers**

TypedArray memory is often reported under `arrayBuffers`, so tracked total is the fair comparison.

---

### Query Benchmark

| Query | Time |
|------|-----:|
| array.filter where | 2.01ms |
| ColQL where count | 4.74ms |
| array select + limit | 1.84ms |
| ColQL select + limit | 0.18ms |

ColQL trades some raw filtering speed for significantly lower memory usage and predictable allocation behavior.

---

## ✨ Features

- Columnar in-memory storage
- Lazy query execution (no intermediate arrays)
- TypedArray-backed numeric storage
- BitSet-backed boolean storage
- Dictionary encoding for string-like values
- Streaming iteration (`for...of`)
- Aggregations: `count`, `sum`, `avg`, `min`, `max`
- Heap-based `top` / `bottom` (no full sort)
- Compact binary serialization
- Zero runtime dependencies

---

## 🛡 Runtime Validation

ColQL validates inserted data and query inputs at runtime to prevent silent `TypedArray` coercion and data corruption.

TypeScript catches many mistakes at compile time, but runtime validation protects JavaScript users and data coming from APIs, files, queues, and other untyped sources.

Examples:

- `uint8` rejects values outside `0..255`
- integer columns reject decimals
- float columns reject `NaN`, `Infinity`, and `-Infinity`
- dictionary columns reject unknown values
- boolean columns reject non-boolean values like `1`, `0`, or `"true"`
- invalid query columns, operators, limits, offsets, and row indexes throw descriptive `ColQLError`s

```ts
users.insert({
  id: 1,
  age: 300,
  status: "active",
  is_active: true
});
// ColQLError: Invalid value for column "age": expected uint8 integer between 0 and 255, received 300.
```

Every `ColQLError` includes a stable `code` field, such as `COLQL_OUT_OF_RANGE`, `COLQL_TYPE_MISMATCH`, or `COLQL_INVALID_COLUMN`.

---

## 🔍 API Overview

### Table

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8()
});
```

### Column Types

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

### Query

```ts
users.where("age", ">", 18);
users.whereIn("status", ["active"]);
users.select(["id", "age"]);
users.limit(10);
users.offset(10);
```

### Execution

```ts
users.count();
users.first();
users.toArray();
users.forEach(console.log);
```

### Aggregation

```ts
users.sum("age");
users.avg("age");
users.min("age");
users.max("age");
```

### Top / Bottom

```ts
users.top(10, "age");
users.bottom(10, "age");
```

### Streaming

```ts
for (const row of users.where("status", "=", "active")) {
  console.log(row);
}
```

---

## 💾 Serialization

```ts
const buffer = users.serialize();
const restored = table.deserialize(buffer);
```

- Stores schema + raw column buffers
- No row materialization
- Fast load/save

---

## ⚠️ Intentional Limitations

ColQL intentionally does not include:

- `orderBy`, `groupBy`, `join`, `distinct`
- indexing
- SQL parser
- runtime dependencies

These are deferred to keep the core engine small and memory-efficient.

---

## 📌 Status

ColQL is in early development (`v0.0.x`).  
API may change before `1.0.0`.

---

## 🛠 Development

```sh
npm install
npm test
npm run build
npm run benchmark:memory
npm run benchmark:query
```

# ColQL

[![CI](https://github.com/emremy/ColQL/actions/workflows/ci.yml/badge.svg)](https://github.com/emremy/ColQL/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@colql/colql.svg)](https://www.npmjs.com/package/@colql/colql)
[![npm downloads](https://img.shields.io/npm/dm/@colql/colql.svg)](https://www.npmjs.com/package/@colql/colql)
[![license](https://img.shields.io/npm/l/@colql/colql.svg)](LICENSE)

ColQL is a zero-dependency, in-memory columnar query engine for TypeScript apps that need compact process-local storage, typed schemas, explicit indexes, and safe mutations.

It is not a SQL database or persistence layer. ColQL is for data you already want to keep inside a Node.js process.

## Why ColQL?

- Compact columnar storage backed by typed arrays, dictionaries, and bit-packed booleans
- Lazy queries with filtering, projection, aggregation, streaming, limit, and offset
- Object predicates plus tuple-style `where(column, operator, value)`
- Explicit equality indexes and sorted numeric indexes for hot predicates
- Mutable tables with `updateMany` and `deleteMany`
- Runtime validation with structured `ColQLError` codes
- Binary serialization for table data
- TypeScript inference for rows, predicates, projections, and mutation payloads
- Zero runtime dependencies

## Install

```sh
npm install @colql/colql
```

## Quick Example

```ts
import { column, table } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  score: column.float64(),
  verified: column.boolean(),
});

users.insertMany([
  { id: 1, age: 29, status: "active", score: 91.5, verified: true },
  { id: 2, age: 17, status: "passive", score: 72.0, verified: false },
  { id: 3, age: 44, status: "active", score: 88.2, verified: true },
]);

users.createIndex("status");
users.createSortedIndex("age");

const activeAdults = users
  .where({
    status: "active",
    age: { gte: 18 },
  })
  .select(["id", "age", "score"])
  .toArray();

const result = users.updateMany(
  { status: "passive", age: { lt: 18 } },
  { status: "archived" },
);

console.log(activeAdults);
console.log(result.affectedRows);
```

## Performance Snapshot

ColQL includes a Fastify example that can boot with 1M deterministic rows and exercise indexed, range, scan, callback-filter, mutation, stress, and memory paths.

These are local reference numbers from `examples/fastify-api` after the example's mutation validation path, not guarantees. Actual numbers vary by Node.js version, CPU, memory pressure, data distribution, query selectivity, projection size, and mutation frequency.

| 1M-row workload | Local avg | Expected shape |
|---|---:|---|
| Selective equality query with `createIndex()` | 2.06ms | Fastest path when candidate sets are small |
| Numeric range query with `createSortedIndex()` | 27.69ms | Helps selective ranges; broad ranges may resemble scans |
| Broad structured predicate | 25.31ms | May intentionally scan when an index is not selective |
| `filter(fn)` callback predicate | 218.93ms | Full-scan escape hatch; not index-aware |

Run the example locally:

```sh
cd examples/fastify-api
npm install
npm run test:large
```

For benchmark scripts and interpretation notes, see [Performance and Benchmarks](./docs/doc/13-performance-and-benchmarks.md).

## When To Use ColQL

Use ColQL when:

- you need to keep thousands to millions of records in memory
- JavaScript object arrays use too much memory
- filters and aggregations should avoid intermediate arrays
- a TypeScript schema can describe your columns
- explicit indexes are acceptable for hot equality or range predicates
- runtime validation matters because data may come from untyped sources

Avoid ColQL when:

- you need durable storage, transactions, joins, or SQL
- row indexes must be stable external identifiers
- every query requires arbitrary sorting or grouping
- you need concurrent writers or multi-process coordination
- you want automatic indexes, compound indexes, or query planning across tables

Row indexes are physical positions and can change after deletes. Use an explicit `id` column for stable identity.

## Examples

- [Basic usage](./examples/basic.ts)
- [Aggregation](./examples/aggregation.ts)
- [Streaming](./examples/streaming.ts)
- [Serialization](./examples/serialization.ts)
- [Fastify API with 1M-row validation](./examples/fastify-api)

The Fastify example demonstrates HTTP query params mapped to object predicates, range queries, `filter(fn)`, `updateMany`, `deleteMany`, query diagnostics, index stats, and memory counters.

## Documentation

Detailed documentation is available under [`docs/doc`](./docs/doc).

Recommended reading:

- [Overview](./docs/doc/00-overview.md)
- [Installation](./docs/doc/01-installation.md)
- [Schema and Columns](./docs/doc/02-schema-and-columns.md)
- [Querying](./docs/doc/04-querying.md)
- [Equality Indexes](./docs/doc/06-indexing.md)
- [Sorted Indexes](./docs/doc/07-sorted-indexes.md)
- [Mutations](./docs/doc/08-mutations.md)
- [Serialization](./docs/doc/11-serialization.md)
- [Memory Model](./docs/doc/12-memory-model.md)
- [Limitations and Design Decisions](./docs/doc/15-limitations-and-design-decisions.md)
- [API Reference](./docs/doc/16-api-reference.md)

## Common APIs

```ts
users.insert(row);
users.insertMany(rows);

users.where({ status: "active", age: { gte: 18 } }).toArray();
users.where("age", ">=", 18).select(["id"]).toArray();
users.whereIn("status", ["active", "passive"]);
users.filter((row) => row.score > 90);

users.count();
users.avg("age");
users.top(10, "score");

users.update(0, { status: "active" });
users.updateMany({ status: "passive" }, { status: "active" });
users.deleteMany({ status: "archived" });

users.createIndex("id");
users.createSortedIndex("age");

const buffer = users.serialize();
const restored = table.deserialize(buffer);
```

`filter(fn)` is intentionally a full-scan escape hatch. Prefer structured predicates when you want index planning.

## Error Handling

ColQL validates schemas, inserted rows, query predicates, mutation payloads, indexes, and serialized input at runtime. Failures throw `ColQLError` with a stable `code`, a message, and optional details.

```ts
import { ColQLError } from "@colql/colql";

try {
  users.insert({ id: 4, age: 300, status: "active", score: 1, verified: true });
} catch (error) {
  if (error instanceof ColQLError) {
    console.log(error.code); // COLQL_OUT_OF_RANGE
  }
}
```

## Development

```sh
npm install
npm test
npm run build
npm run benchmark:memory
npm run benchmark:query
npm run benchmark:indexed
npm run benchmark:range
npm run benchmark:optimizer
npm run benchmark:serialization
npm run benchmark:delete
```

## Status

ColQL v0.2.x aims to keep the public API reasonably stable, but breaking changes may still happen before 1.0.0.

## Limitations

ColQL intentionally does not include SQL parsing, joins, transactions, concurrency control, automatic indexes, compound indexes, or durable storage. Indexes are derived performance structures; query results must be the same whether ColQL uses an index or a full scan.

# ColQL

[![CI](https://github.com/emremy/ColQL/actions/workflows/ci.yml/badge.svg)](https://github.com/emremy/ColQL/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@colql/colql.svg)](https://www.npmjs.com/package/@colql/colql)
[![npm downloads](https://img.shields.io/npm/dm/@colql/colql.svg)](https://www.npmjs.com/package/@colql/colql)
[![license](https://img.shields.io/npm/l/@colql/colql.svg)](LICENSE)

ColQL is a memory-conscious in-memory columnar query engine for TypeScript. It stores data in compact columns, runs lazy queries, validates inputs at runtime, and exposes explicit indexes and mutation APIs without adding runtime dependencies.

## Quick Example

```ts
import { table, column } from "@colql/colql";

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

users.updateWhere("id", "=", 1, { age: 26 });

const activeAdults = users
  .where("age", ">=", 18)
  .where("status", "=", "active")
  .select(["id", "age"])
  .limit(5)
  .toArray();
```

## Install

```sh
npm install @colql/colql
```

## Highlights

- Chunked columnar storage backed by `TypedArray`, dictionary, and bit-packed boolean columns
- Lazy filtering, projection, aggregation, and streaming iteration
- Physical deletes with no tombstones or compact step
- Row updates plus predicate-based update/delete
- Optional equality indexes and sorted indexes
- Cost-aware query planning with lazy index rebuilds after mutations
- Runtime validation with structured `ColQLError` errors
- Binary serialization of schema and column data
- TypeScript inference for rows, predicates, projections, and mutation payloads
- Zero runtime dependencies

## Documentation

Detailed documentation is available under [`docs/doc`](./docs/doc).

Recommended reading:

- [Overview](./docs/doc/00-overview.md)
- [Schema and Columns](./docs/doc/02-schema-and-columns.md)
- [Querying](./docs/doc/04-querying.md)
- [Indexing](./docs/doc/06-indexing.md)
- [Mutations](./docs/doc/08-mutations.md)
- [Error Handling](./docs/doc/10-error-handling.md)
- [Memory Model](./docs/doc/12-memory-model.md)

The full documentation set also covers installation, inserts, aggregations, sorted indexes, physical deletes, serialization, benchmarks, TypeScript type safety, limitations, and a compact API reference.

## Common APIs

```ts
users.insert(row);
users.insertMany(rows);

users.where("age", ">=", 18).select(["id"]).toArray();
users.whereIn("status", ["active"]);
users.whereNotIn("status", ["archived"]);

users.count();
users.avg("age");
users.top(10, "score");

users.update(0, { status: "active" });
users.updateWhere("status", "=", "passive", { status: "active" });
users.delete(0);
users.deleteWhere("age", "<", 18);

users.createIndex("id");
users.createSortedIndex("age");

const buffer = users.serialize();
const restored = table.deserialize(buffer);
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

ColQL is still pre-1.0. APIs may change before a stable 1.0 release.

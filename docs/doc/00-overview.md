# Overview

ColQL is an in-memory columnar query engine for TypeScript. It is designed for applications that already have data in process and want compact storage, lazy filtering, projections, simple aggregations, explicit indexes, safe mutations, and binary serialization without bringing in a database server or runtime dependencies.

ColQL is not a SQL database, ORM, persistence layer, or transactional system. It does not parse SQL, join tables, enforce uniqueness, or provide concurrency control. Its core tradeoff is narrower scope in exchange for predictable in-memory behavior.

## Core Idea

JavaScript arrays usually store data as row objects:

```ts
const users = [
  { id: 1, age: 25, status: "active", is_active: true },
  { id: 2, age: 17, status: "passive", is_active: false },
];
```

That shape is convenient, but each row object has object overhead and repeated strings. ColQL stores the same logical table by column:

```txt
id        -> Uint32 chunks
age       -> Uint8 chunks
status    -> compact dictionary codes
is_active -> bit-packed boolean chunks
```

Rows are materialized only when an API needs row objects, such as `toArray()`, `first()`, iteration, or projection output.

## Where ColQL Fits

Use ColQL when:

- you need to keep thousands to millions of records in memory
- JavaScript object arrays use too much memory
- filters and aggregations should avoid intermediate arrays
- a TypeScript schema can describe your columns
- explicit indexes are acceptable for hot equality or range predicates
- runtime validation matters because data may come from untyped sources

Avoid ColQL when:

- you need durable storage, transactions, joins, or SQL
- row indexes must be stable identifiers
- every query requires arbitrary sorting or grouping
- you need concurrent writers or multi-process coordination

## Major Capabilities

- Schema-based tables with TypeScript inference
- Numeric, boolean, and dictionary columns
- Chunked columnar storage
- Lazy queries with `where`, `select`, `limit`, and `offset`
- Streaming iteration with `for...of` and `forEach`
- Aggregations such as `count`, `avg`, `top`, and `bottom`
- Equality indexes for numeric and dictionary columns
- Sorted indexes for numeric range queries
- Physical deletes and row updates
- Predicate-based update/delete with snapshot semantics
- Runtime validation and structured `ColQLError` failures
- Binary serialization and deserialization

## Quick Example

```ts
import { table, column, ColQLError } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  score: column.float64(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  is_active: column.boolean(),
});

users.insertMany([
  { id: 1, age: 25, score: 91.5, status: "active", is_active: true },
  { id: 2, age: 17, score: 72.0, status: "passive", is_active: false },
  { id: 3, age: 44, score: 88.2, status: "archived", is_active: false },
]);

users.createIndex("status");
users.createSortedIndex("age");

const activeAdults = users
  .where("status", "=", "active")
  .where("age", ">=", 18)
  .select(["id", "score"])
  .toArray();

const result = users.updateWhere("id", "=", 1, { score: 94.2 });
console.log(result.affectedRows); // 1

try {
  users.insert({ id: 4, age: 300, score: 1, status: "active", is_active: true });
} catch (error) {
  if (error instanceof ColQLError) {
    console.log(error.code); // COLQL_OUT_OF_RANGE
  }
}
```

## Read Next

- [Installation](./01-installation.md)
- [Schema and Columns](./02-schema-and-columns.md)
- [Querying](./04-querying.md)
- [Memory Model](./12-memory-model.md)

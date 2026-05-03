# Limitations and Design Decisions

ColQL intentionally keeps a narrow, explicit feature set.

ColQL aims to keep the public API reasonably stable, but breaking changes may still happen before 1.0.0.

## Not Included

- SQL parser
- joins
- `groupBy`
- `distinct`
- `orderBy`
- compound indexes
- automatic indexes
- transactions
- concurrency control
- durable storage
- serialized indexes
- compound unique indexes

## Why These Limits Exist

ColQL optimizes for:

- memory predictability
- understandable in-memory behavior
- explicit index costs
- small runtime footprint
- TypeScript-first APIs
- no runtime dependencies

Adding SQL, joins, transactional semantics, or automatic indexing would make the engine broader and less predictable.

## Row Indexes Are Not Stable IDs

Row indexes are internal positions, not stable external identifiers. Inserts, updates, and deletes may change row positions; physical deletes shift row indexes after the deleted row. Use an explicit ID column for stable identity:

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8(),
});
```

## Indexes Are Derived

Equality and sorted indexes are optional derived structures and are not serialized. They affect performance only, not correctness. A query must return the same result through an index or a full scan.

Unique indexes are also derived and not serialized, but they are integrity constraints as well as lookup structures. Recreate them after deserialization when uniqueness enforcement or by-key helpers are needed.

Dirty indexes are rebuilt before use or explicitly by the user. This avoids complex incremental row-position maintenance, especially around physical deletes.

## Mutation Semantics Are Safety-Oriented

Predicate mutations snapshot row indexes before writing. This costs temporary memory proportional to matched rows, but it prevents shifting row indexes or predicate changes from altering the mutation target set mid-operation.

## Query Semantics Are Scan-Order Oriented

Queries preserve logical row order. Sorted indexes accelerate candidate discovery for range predicates; they do not imply sorted output.

See [Querying](./04-querying.md), [Physical Deletes](./09-physical-deletes.md), and [Memory Model](./12-memory-model.md).

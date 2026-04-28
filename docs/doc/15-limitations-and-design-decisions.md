# Limitations and Design Decisions

ColQL intentionally keeps a narrow, explicit feature set.

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

Physical deletes shift row indexes after the deleted row. Do not store row indexes as permanent identifiers. Use an explicit ID column:

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8(),
});
```

## Indexes Are Derived

Equality and sorted indexes are optional and not serialized. They are rebuilt lazily after mutations or explicitly by the user.

This avoids complex incremental row-ID maintenance, especially around physical deletes.

## Mutation Semantics Are Safety-Oriented

Predicate mutations snapshot row indexes before writing. This costs temporary memory proportional to matched rows, but it prevents shifting row indexes or predicate changes from altering the mutation target set mid-operation.

## Query Semantics Are Scan-Order Oriented

Queries preserve logical row order. Sorted indexes accelerate candidate discovery for range predicates; they do not imply sorted output.

See [Querying](./04-querying.md), [Physical Deletes](./09-physical-deletes.md), and [Memory Model](./12-memory-model.md).

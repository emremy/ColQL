# Limitations and Design Decisions

ColQL intentionally keeps a narrow, explicit feature set.

ColQL aims to keep the public API reasonably stable, and v0.6.0 continues hardening public diagnostics, serialization validation, type/API gates, and internal background-indexing architecture. Breaking changes may still happen before 1.0.0; the API is not fully frozen.

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
- public background-indexing configuration
- compound unique indexes
- distributed or multi-process coordination

## Why These Limits Exist

ColQL optimizes for:

- memory predictability
- understandable in-memory behavior
- explicit index costs
- small runtime footprint
- TypeScript-first APIs
- no runtime dependencies

Adding SQL, joins, transactional semantics, or automatic index creation would make the engine broader and less predictable.

## When Not To Use ColQL

ColQL is intentionally process-local and in-memory. Avoid it when:

- multiple pods, workers, processes, or machines need shared mutable state
- data must survive process restarts without an external persistence layer
- transactional semantics, rollback, isolation, or write-ahead durability are required
- writes dominate the workload and frequently dirty large indexes
- a small JavaScript array is already simple, clear, and fast enough
- analytical SQL over files or large columnar datasets is the main requirement, where DuckDB may be a better fit

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

Dirty indexes are rebuilt before actual query execution or explicitly by the user. v0.6.0 also includes internal background rebuild infrastructure for equality and sorted indexes, but public query APIs remain synchronous and automatic worker scheduling is not exposed yet. `query.explain()` reports dirty, queued, rebuilding, or failed index state without rebuilding or scheduling work, so diagnostics do not hide rebuild cost.

Queued, rebuilding, and failed indexes are not used for query results. ColQL falls back to a scan or another fresh index. Background rebuild results are accepted only after generation and column-epoch validation; stale results are discarded.

Unique indexes remain synchronous and main-thread-only because they enforce integrity, not just query performance.

## Mutation Semantics Are Safety-Oriented

Predicate mutations snapshot row indexes before writing. This costs temporary memory proportional to matched rows, but it prevents shifting row indexes or predicate changes from altering the mutation target set mid-operation.

## Query Semantics Are Scan-Order Oriented

Queries preserve logical row order. Sorted indexes accelerate candidate discovery for range predicates; they do not imply sorted output.

See [Querying](./04-querying.md), [Physical Deletes](./09-physical-deletes.md), and [Memory Model](./12-memory-model.md).

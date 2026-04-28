# Physical Delete Strategy

This is a research note for the chunked storage work that led to production physical deletes in ColQL.

## Problem

Single-buffer column storage makes physical delete expensive: deleting row `i` requires shifting every later row in every column. That is simple, but it creates delete cost proportional to the remaining table size.

## Adopted Strategy

ColQL now uses chunked columnar storage internally. Each column stores logical rows across fixed-size chunks with a default chunk size of 65,536 rows. Physical delete shifts values only inside the affected chunk, removes empty chunks, preserves logical row order, and does not use tombstones.

## Delete Semantics

`table.delete(rowIndex)` physically removes the row. Row indexes after the deleted row may change, so callers should use an explicit `id` column for stable identity.

## Index Behavior

Equality and sorted indexes are derived data. Deletes mark indexes dirty, and ColQL rebuilds them lazily when a future query needs them. This keeps delete simple and avoids complex incremental row-id rewrites.

## Delete Benchmark Memory Attribution

The production delete benchmark reports memory snapshots at separate phases: start, after table build, after index creation, after delete operations, before/after indexed queries, after materialized query results are released, and after indexes are dropped.

This separation is important because memory growth after deletes can come from several places:

- base chunked column storage
- optional equality/sorted indexes
- lazy index rebuild after deletes
- benchmark-local random delete arrays
- `toArray()` materialized result rows

Use `npm run benchmark:delete` to collect local numbers for the current runtime.

## Recommendation

YES, with the current constraints. Chunked storage gives ColQL real physical deletes without tombstones while preserving the public query model. The main tradeoff is that row lookup now maps logical indexes through chunk metadata, but the 65,536-row default keeps chunk counts low for typical in-memory workloads.

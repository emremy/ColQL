# Equality Indexes

Equality indexes are optional derived performance structures for selective equality and membership queries. A query must return the same result whether ColQL uses an index or a full scan. Unique indexes are separate integrity indexes; see [Unique Indexes](./17-unique-indexes.md).

```ts
users.createIndex("id");
users.createIndex("status");

const user = users.where("id", "=", 123).first();
```

## API

```ts
users.createIndex("id");
users.dropIndex("id");
users.hasIndex("id");
users.indexes();
users.indexStats();
users.rebuildIndex("id");
users.rebuildIndexes();
```

`createIndex`, `dropIndex`, and rebuild methods return the table instance. `indexes()` returns indexed column names. `indexStats()` returns approximate memory and cardinality metadata.

## Supported Columns and Operators

Equality indexes support numeric and dictionary columns. Boolean columns are not supported by equality indexes because scanning low-cardinality boolean values is often as efficient as indexing them.

Indexed operators:

- `=`
- `in`
- `whereIn`

Not indexed:

- `!=`
- `not in`
- boolean columns
- multi-column compound indexes

ColQL does not build a combined index for compound predicates. Multiple predicates are still combined at query time. Queries can still use unsupported predicates; they scan instead. Fallback to scan affects performance only, not correctness.

## Planner Behavior

ColQL uses a cost-aware planner. If an index exists but would return too many candidate rows, the planner can fall back to a scan. This avoids allocating or iterating a broad index candidate set when a scan is likely cheaper. Planner decisions affect performance only, not query results.

Indexes are most useful for selective predicates:

```ts
users.createIndex("id");
users.where("id", "=", 42).first();
```

Broad predicates may scan:

```ts
users.createIndex("status");
users.where("status", "in", ["active", "passive"]).count();
```

## Lifecycle and Rebuilds

Inserts, deletes, and updates can change internal row positions or indexed values. Row positions are not stable IDs and should not be used as external identifiers. If stable identity is required, define and index an ID column.

Updates dirty only indexes whose indexed columns changed. Updating an unrelated column does not dirty equality or sorted indexes for other columns. Deletes still dirty equality and sorted indexes broadly because physical row positions shift. Inserts update clean equality indexes incrementally and mark sorted indexes dirty.

Internally, indexes can be in these lifecycle states:

- `fresh`: the planner may use the index.
- `dirty`: the index is not trusted until rebuilt.
- `queued`: a background rebuild has been accepted but is not visible.
- `rebuilding`: a background rebuild is in progress and not visible.
- `failed`: the previous background rebuild failed; queries must not use the index.

Queued, rebuilding, failed, and dirty indexes are not used to return query results. ColQL either falls back to a scan or uses another fresh index. The public query APIs remain synchronous.

Current normal execution preserves the synchronous fallback path: when an indexed query requires a dirty equality index, ColQL can rebuild it synchronously before use, so the first indexed query after a relevant indexed-column mutation may be slower than later queries. The v0.6.0 background-indexing architecture adds internal worker-backed rebuild paths for large indexes, but automatic public scheduling is not exposed yet.

Background rebuild results are accepted only after generation and column-epoch validation. Stale or malformed results are discarded and never become visible to the planner.

You can rebuild explicitly:

```ts
users.updateWhere("status", "=", "passive", { status: "active" });
users.rebuildIndex("status");
```

`rebuildIndexes()` rebuilds all equality and sorted indexes.

Manual rebuild methods remain available, but they are not intended to be the primary lifecycle-management model. Users define indexes; ColQL manages derived index state internally.

## Serialization

Indexes are not serialized. They are derived performance data and can be recreated:

```ts
const restored = table.deserialize(buffer);
restored.createIndex("id");
```

See [Serialization](./11-serialization.md) and [Memory Model](./12-memory-model.md).

# Equality Indexes

Equality indexes are optional derived structures for selective equality and membership queries.

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

Equality indexes support numeric and dictionary columns. Boolean columns are not supported by equality indexes.

Indexed operators:

- `=`
- `in`
- `whereIn`

Not indexed:

- `!=`
- `not in`
- boolean columns
- compound predicates as a combined compound index

Queries can still use unsupported predicates; they scan instead.

## Planner Behavior

ColQL uses a cost-aware planner. If an index exists but would return too many candidate rows, the planner can fall back to a scan. This avoids allocating or iterating a broad index candidate set when a scan is likely cheaper.

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

## Dirty and Lazy Rebuilds

Deletes and updates can change row indexes or indexed values. ColQL marks existing indexes dirty after nonzero mutations and rebuilds them lazily when an indexed query needs them. The first indexed query after a mutation may be slower than later queries.

You can rebuild explicitly:

```ts
users.updateWhere("status", "=", "passive", { status: "active" });
users.rebuildIndex("status");
```

`rebuildIndexes()` rebuilds all equality and sorted indexes.

## Serialization

Indexes are not serialized. They are derived data and can be recreated:

```ts
const restored = table.deserialize(buffer);
restored.createIndex("id");
```

See [Serialization](./11-serialization.md) and [Memory Model](./12-memory-model.md).

# Serialization

ColQL can serialize a process-local table snapshot to an `ArrayBuffer`:

```ts
const buffer = users.serialize();
const restored = table.deserialize(buffer);
```

## What Is Serialized

Snapshot serialization stores:

- schema metadata
- row count
- capacity
- numeric column buffers
- dictionary column codes and dictionary values
- boolean bit storage

Serialization does not materialize row objects. It is not durable storage, a database file format, or a cross-process coordination mechanism.

## What Is Not Serialized

Indexes are not serialized:

- equality indexes
- sorted indexes
- unique indexes

They are derived data and can be rebuilt after deserialization. Recreating equality and sorted indexes affects performance only, not query correctness. Recreating unique indexes also restores uniqueness enforcement.

```ts
const restored = table.deserialize(buffer);
restored.createIndex("id");
restored.createSortedIndex("age");
restored.createUniqueIndex("id");
```

`restored.indexes()`, `restored.sortedIndexes()`, and `restored.uniqueIndexes()` are empty until indexes are recreated.

## Index Lifecycle After Restore

Restored tables are correct before indexes are recreated, but indexed performance and unique-index helpers require explicit index creation:

```ts
const restored = table.deserialize(buffer);

console.log(restored.where("status", "=", "active").explain());
// scanType: "full"
// reasonCode: "NO_INDEX_FOR_COLUMN"

restored.createIndex("status");
console.log(restored.where("status", "=", "active").explain());
// scanType: "index"
```

Dirty indexes are different from missing indexes. After updates or deletes, existing indexes may be marked dirty. Actual query execution rebuilds a dirty index lazily before using it, so stale index results are not returned. `query.explain()` reports that state without rebuilding:

```ts
users.updateMany({ status: "active" }, { status: "expired" });

console.log(users.where("status", "=", "expired").explain());
// indexState: "dirty"
// reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION"
```

## After Mutations and Deletes

Serialization writes the current physical state:

```ts
users.updateWhere("id", "=", 1, { status: "active" });
users.deleteWhere("status", "=", "archived");

const restored = table.deserialize(users.serialize());
console.log(restored.toArray());
```

## Validation

Deserialization validates the input buffer shape, magic header, wire-format version, metadata, column names, row-count/capacity relationship, payload offsets, alignment, payload lengths, dictionary values, and dictionary codes. Invalid input throws `ColQLError` with `COLQL_INVALID_SERIALIZED_DATA`.

## Wire Format Policy

The serialized wire-format version is independent from the npm package version. Patch and minor releases should preserve the current wire format when possible, but ColQL is still pre-1.0 and unsupported snapshot versions fail loudly with `COLQL_INVALID_SERIALIZED_DATA`. Snapshots produced by v0.4.x are not guaranteed to be compatible with v0.5.0.

Indexes are never trusted from serialized input. If future metadata contains serialized index state, current ColQL versions reject it rather than loading stale derived row positions.

See [Error Handling](./10-error-handling.md) and [Indexing](./06-indexing.md).

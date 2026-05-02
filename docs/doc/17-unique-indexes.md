# Unique Indexes

Unique indexes are derived lookup structures that also enforce data integrity. This is the key difference from equality and sorted indexes: equality and sorted indexes affect performance only, while unique indexes reject duplicate keys.

```ts
users.createUniqueIndex("id");

const user = users.findBy("id", 123);
users.updateBy("id", 123, { status: "active" });
users.deleteBy("id", 123);
```

## API

```ts
users.createUniqueIndex("id");
users.dropUniqueIndex("id");
users.hasUniqueIndex("id");
users.uniqueIndexes();
users.uniqueIndexStats();
users.rebuildUniqueIndex("id");
users.rebuildUniqueIndexes();

users.findBy("id", 123);
users.updateBy("id", 123, { status: "active" });
users.deleteBy("id", 123);
```

Unique indexes support numeric and dictionary columns. Boolean columns are not supported and throw `COLQL_UNIQUE_INDEX_UNSUPPORTED`.

## Guarantees

Once a unique index exists, duplicate keys for that column are rejected:

- `createUniqueIndex()` scans existing rows and throws if duplicates already exist.
- `insert()` rejects duplicate keys.
- `insertMany()` rejects duplicates against existing rows and within the input batch.
- `update()` and predicate updates reject duplicate-producing changes.
- failed bulk insert/update operations are all-or-nothing.
- deletes free keys for reuse.
- rebuilds detect duplicates and fail atomically.

Duplicate violations throw `COLQL_DUPLICATE_KEY` with details such as `columnName`, `encodedValue`, and row/input positions when available.

## By-Key Helpers

`findBy`, `updateBy`, and `deleteBy` require a unique index and do not scan when one is missing. Missing unique indexes throw `COLQL_UNIQUE_INDEX_NOT_FOUND`.

Missing keys are not errors:

```ts
users.findBy("id", 999);                 // undefined
users.updateBy("id", 999, { age: 30 });  // { affectedRows: 0 }
users.deleteBy("id", 999);               // { affectedRows: 0 }
```

Row indexes remain unstable physical positions. Use an explicit ID column plus a unique index for stable identity.

## Dirty Rebuilds and Serialization

Unique indexes store row positions internally, so deletes and updates can make them dirty. Dirty unique indexes are rebuilt before by-key lookup or stats so stale row positions are not returned.

Unique indexes are not serialized:

```ts
const restored = table.deserialize(users.serialize());
restored.createUniqueIndex("id");
```

See [Mutations](./08-mutations.md), [Serialization](./11-serialization.md), and [Memory Model](./12-memory-model.md).

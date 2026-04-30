# Serialization

ColQL can serialize a table to an `ArrayBuffer`:

```ts
const buffer = users.serialize();
const restored = table.deserialize(buffer);
```

## What Is Serialized

Serialization stores:

- schema metadata
- row count
- capacity
- numeric column buffers
- dictionary column codes and dictionary values
- boolean bit storage

Serialization does not materialize row objects.

## What Is Not Serialized

Indexes are not serialized:

- equality indexes
- sorted indexes

They are derived performance data and can be rebuilt after deserialization. Recreating indexes after deserialization affects performance only, not query correctness.

```ts
const restored = table.deserialize(buffer);
restored.createIndex("id");
restored.createSortedIndex("age");
```

`restored.indexes()` and `restored.sortedIndexes()` are empty until indexes are recreated.

## After Mutations and Deletes

Serialization writes the current physical state:

```ts
users.updateWhere("id", "=", 1, { status: "active" });
users.deleteWhere("status", "=", "archived");

const restored = table.deserialize(users.serialize());
console.log(restored.toArray());
```

## Validation

Deserialization validates the input buffer shape, magic header, version, metadata, and column payload sizes. Invalid input throws `ColQLError` with `COLQL_INVALID_SERIALIZED_DATA`.

See [Error Handling](./10-error-handling.md) and [Indexing](./06-indexing.md).

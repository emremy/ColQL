# Physical Deletes

ColQL physically removes rows from storage. It does not use tombstones and does not require a separate compaction step.

```ts
users.delete(3);
```

`delete(rowIndex)` returns the table instance for backward compatibility.

## Row Index Semantics

Logical row order is preserved, but row indexes after the deleted row may shift. Row indexes are internal positions, not stable IDs.

```ts
const before = users.get(4);
users.delete(2);
const after = users.get(3); // may be the same logical row as `before`
```

Do not store row indexes as permanent identifiers.

Use an explicit ID column:

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8(),
});
```

## Chunked Storage

ColQL stores each column in chunks. A physical delete removes the value at the target row index from every column and shifts values inside affected chunk storage. Empty chunks can be removed. This keeps the public table model simple: there are no deleted markers for queries to skip.

## Predicate Deletes

Predicate deletes snapshot row indexes first and then delete from highest index to lowest:

```ts
const result = users.deleteWhere("status", "=", "archived");
console.log(result.affectedRows);
```

Descending deletion avoids accidentally skipping rows as lower row indexes shift.

## Indexes After Delete

Deletes mark existing equality and sorted indexes dirty. If an indexed query needs a dirty index, ColQL rebuilds it before use. Dirty indexes are not used to return stale results. You can also rebuild explicitly:

```ts
users.deleteWhere("age", "<", 18);
users.rebuildIndexes();
```

## Serialization After Delete

Serialization writes the current physical table state:

```ts
users.delete(0);
const restored = table.deserialize(users.serialize());
console.log(restored.rowCount); // same current row count
```

Indexes are not serialized; recreate them after deserialization if needed.

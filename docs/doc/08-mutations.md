# Mutations

ColQL supports single-row updates, predicate updates, physical row deletes, and predicate deletes.

## API

```ts
users.update(rowIndex, partialRow);
users.where(...).update(partialRow);
users.where(...).delete();

users.updateMany(predicate, partialRow);
users.deleteMany(predicate);

users.updateWhere(column, operator, value, partialRow);
users.deleteWhere(column, operator, value);
```

New mutation APIs return:

```ts
type MutationResult = {
  affectedRows: number;
};
```

Existing physical row delete keeps its older behavior:

```ts
users.delete(rowIndex); // returns the table instance
```

`users.update(rowIndex, partialRow)` returns `{ affectedRows: 1 }` when successful. Predicate update/delete return `{ affectedRows: number }`; no-match predicate mutations return `{ affectedRows: 0 }`.

`updateMany` and `deleteMany` are preferred table-level convenience wrappers for common predicate mutations. Existing query mutation APIs remain available, and `updateWhere`/`deleteWhere` remain legacy convenience aliases. No mutation APIs are removed.

## Single-Row Update

```ts
const result = users.update(0, { age: 26, status: "active" });
console.log(result.affectedRows); // 1
```

The row index must be valid at the time of the call. The partial row must contain at least one known column and each value must pass the column validator.

## Predicate Update

```ts
const result = users.updateWhere("status", "=", "passive", {
  status: "active",
});
```

Object predicate form:

```ts
const result = users.updateMany(
  { status: "passive", age: { gte: 18 } },
  { status: "active" },
);
```

Query form:

```ts
const result = users
  .where("age", ">=", 18)
  .offset(10)
  .limit(25)
  .update({ is_active: true });
```

Query mutations respect `where`, `offset`, and `limit`. `select()` affects query output shape but does not restrict update payloads:

```ts
users
  .where("status", "=", "passive")
  .select(["id"])
  .limit(10)
  .update({ status: "active" });
```

## Predicate Delete

```ts
const result = users.deleteWhere("age", "<", 18);
```

Object predicate form:

```ts
const result = users.deleteMany({ status: "archived" });
```

Query form:

```ts
const result = users
  .where("status", "=", "archived")
  .offset(5)
  .limit(10)
  .delete();
```

Predicate deletes physically remove rows. Row indexes after deleted rows may shift.

## Safety Rules

ColQL applies mutation safety rules internally:

- matching row indexes are snapshotted before predicate mutation
- update input is validated before writing to storage
- predicate updates are all-or-nothing for validation
- predicate deletes delete matched row indexes from highest to lowest
- no-match predicate update/delete returns `{ affectedRows: 0 }`
- nonzero update/delete mutations mark existing indexes dirty
- incremental index maintenance is not attempted

Snapshotting matters when an update changes the predicate column:

```ts
users.updateWhere("status", "=", "passive", { status: "active" });
```

Only rows that matched before mutation are updated.

## Errors

Invalid values throw `ColQLError` before mutation:

```ts
users.updateWhere("status", "=", "active", { age: 999 });
// COLQL_OUT_OF_RANGE; no rows are changed
```

See [Error Handling](./10-error-handling.md), [Physical Deletes](./09-physical-deletes.md), and [Indexing](./06-indexing.md).

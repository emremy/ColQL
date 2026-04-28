# Inserts and Bulk Loading

Use `insert(row)` to append one row:

```ts
users.insert({
  id: 1,
  age: 25,
  status: "active",
  is_active: true,
});
```

`insert` validates the full row before writing it. It returns the table instance so inserts can be chained.

## Bulk Inserts

Use `insertMany(rows)` when loading multiple rows:

```ts
users.insertMany([
  { id: 1, age: 25, status: "active", is_active: true },
  { id: 2, age: 17, status: "passive", is_active: false },
]);
```

`insertMany` validates every row before inserting any row. If one row is invalid, the table is not partially mutated.

```ts
try {
  users.insertMany([
    { id: 1, age: 25, status: "active", is_active: true },
    { id: 2, age: 999, status: "passive", is_active: false },
  ]);
} catch {
  console.log(users.rowCount); // unchanged
}
```

## Row Counts

`rowCount` is the physical number of rows in the table:

```ts
console.log(users.rowCount);
console.log(users.count()); // count() on the table counts all rows
```

`count()` can also be used on a query:

```ts
const activeCount = users.where("status", "=", "active").count();
```

## Capacity and Growth

ColQL starts each table with an initial capacity and grows storage as needed. Internally, storage is chunked, so appends grow column capacity without relying on one large contiguous object array. Numeric data lives in typed-array chunks, dictionary values are encoded into numeric chunks, and booleans are bit-packed.

You normally do not need to manage capacity yourself. If you know you will load many rows, `insertMany` is clearer and validates all input before mutating.

## Common Insert Errors

```ts
users.insert({ id: 1, age: 25, status: "active" });
// Missing is_active -> COLQL_MISSING_VALUE

users.insert({ id: 1, age: 25, status: "active", is_active: true, email: "x" });
// Unknown email -> COLQL_INVALID_COLUMN

users.insert({ id: -1, age: 25, status: "active", is_active: true });
// uint32 range failure -> COLQL_OUT_OF_RANGE
```

See [Error Handling](./10-error-handling.md) for `ColQLError` handling.

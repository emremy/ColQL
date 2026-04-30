# Querying

ColQL queries are lazy. Chaining `where`, `select`, `limit`, and `offset` builds a query object; rows are scanned or read through indexes only when an execution method runs.

## Filters

```ts
users.where("age", ">=", 18);
users.where("status", "=", "active");
users.where("is_active", "=", true);
```

Object predicates are also supported:

```ts
people.where({
  age: { gt: 25 },
  active: true,
});

people.where({
  country: { in: ["TR", "US"] },
});
```

Object `where` is syntactic sugar over the same structured predicates as tuple `where(column, operator, value)`. It can still use equality and sorted indexes when the translated predicates are indexable.

Supported operators:

```txt
=
!=
>
>=
<
<=
in
not in
```

Range operators (`>`, `>=`, `<`, `<=`) are supported for numeric columns. Equality and membership are supported for numeric, dictionary, and boolean columns, subject to validation.

Object predicates use these operator names:

```ts
users.where({ age: { eq: 25 } });
users.where({ age: { gt: 25, lte: 65 } });
users.where({ country: { in: ["TR", "US"] } });
```

Numeric columns support `eq`, `gt`, `gte`, `lt`, `lte`, and `in`. Boolean and dictionary columns support equality/default values and `in`.

## Callback Filters

Use `filter(fn)` as a full-scan escape hatch when a predicate is easier to express in TypeScript:

```ts
users.filter((row) => row.age > 25);
```

Callback filters run after structured predicates:

```ts
const rows = users
  .where({ status: "active" })
  .filter((row) => row.age > 25)
  .toArray();
```

`filter(fn)` is not index-aware. Structured predicates run first; callback filters then run as a full-scan callback pass over rows that remain eligible.

## Membership Helpers

```ts
users.whereIn("status", ["active", "passive"]);
users.whereNotIn("status", ["archived"]);
```

`in` and `not in` require a non-empty array.

## Projection

`select()` controls output shape:

```ts
const rows = users
  .where("status", "=", "active")
  .select(["id", "age"])
  .toArray();
```

Projection reduces materialized row objects, but it does not change which rows match. For query mutations, `select()` does not restrict which columns can be updated. See [Mutations](./08-mutations.md).

## Limit and Offset

```ts
const page = users
  .where("age", ">=", 18)
  .offset(20)
  .limit(10)
  .toArray();
```

The query window is applied after filtering. `limit(0)` is valid and returns no rows. `offset` and `limit` must be non-negative integers.

## Executing Queries

```ts
users.first();       // first matching row or undefined
users.toArray();     // materializes all matching rows
users.count();       // counts matching rows
users.forEach(row => console.log(row));
```

Tables expose the same convenience methods for all rows:

```ts
users.count();
users.first();
users.toArray();
```

Streaming:

```ts
for (const row of users.where("status", "=", "active")) {
  console.log(row.id);
}
```

`toArray()` materializes JavaScript row objects proportional to the result size. Prefer `count`, `first`, `forEach`, or `for...of` when you do not need all rows in memory.

## Scans and Indexes

Without a usable index, queries scan row indexes from `0` to `rowCount - 1`. If an equality or sorted index exists, ColQL may use it automatically when the planner estimates the candidate set is selective enough. Broad indexed queries may still fall back to scan to avoid index overhead.

Indexes and planner choices affect performance only. A query must return the same result whether ColQL uses an index or a full scan.

See [Equality Indexes](./06-indexing.md) and [Sorted Indexes](./07-sorted-indexes.md).

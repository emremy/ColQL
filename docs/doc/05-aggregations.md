# Aggregations

Aggregations execute lazily over matching row indexes and do not materialize row objects unless the operation returns rows, such as `top` and `bottom`.

## Counting

```ts
users.count();
users.size(); // alias for count()
users.isEmpty();

const active = users.where("status", "=", "active").count();
```

For queries, `count()` respects `where`, `offset`, and `limit`.

## Numeric Aggregations

```ts
const totalScore = users.sum("score");
const averageAge = users.avg("age");
const youngest = users.min("age");
const oldest = users.max("age");
```

`sum`, `avg`, `min`, and `max` require numeric columns. Calling them on dictionary or boolean columns throws `ColQLError` with `COLQL_INVALID_COLUMN_TYPE`.

Empty match behavior:

- `sum()` returns `0`
- `avg()` returns `undefined`
- `min()` returns `undefined`
- `max()` returns `undefined`

## Top and Bottom

```ts
const topScores = users.top(10, "score");
const youngestUsers = users.bottom(5, "age");

const activeTopScores = users
  .where("status", "=", "active")
  .select(["id", "score"])
  .top(10, "score");
```

`top` and `bottom` require a positive integer count and a numeric column. Internally, ColQL uses a heap instead of sorting the full result set, so memory usage is tied to the requested `n` plus the selected output rows.

## Practical Guidance

- Use `count`, `sum`, `avg`, `min`, and `max` when you do not need row objects.
- Use `top` and `bottom` for bounded ranked results.
- Use `select()` before `top`/`bottom` when only a subset of columns is needed in the output.

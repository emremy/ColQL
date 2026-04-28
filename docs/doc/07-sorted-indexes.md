# Sorted Indexes

Sorted indexes accelerate selective numeric range queries.

```ts
users.createSortedIndex("age");

const adults = users
  .where("age", ">=", 18)
  .select(["id", "age"])
  .toArray();
```

## API

```ts
users.createSortedIndex("age");
users.dropSortedIndex("age");
users.hasSortedIndex("age");
users.sortedIndexes();
users.sortedIndexStats();
users.rebuildSortedIndex("age");
users.rebuildIndexes();
```

Sorted indexes are separate from equality indexes because they store row IDs ordered by numeric column value instead of buckets by exact value.

## Supported Columns and Operators

Sorted indexes are numeric-only.

Supported range operators:

- `>`
- `>=`
- `<`
- `<=`

Equality on a numeric column can use an equality index, not a sorted index.

## Planner Behavior

The planner estimates the number of matching rows from sorted-index bounds. If the range is selective enough, ColQL scans the candidate row IDs. If the range is broad, ColQL may fall back to a table scan.

```ts
users.createSortedIndex("score");

const highScores = users.where("score", ">", 900).toArray();
const manyRows = users.where("score", ">", 10).count(); // may scan
```

Candidate row IDs are returned in scan order so query output preserves logical row order.

## Dirty and Lazy Rebuilds

Sorted indexes are marked dirty after inserts, deletes, and updates. They are rebuilt lazily when a query needs them, or eagerly with:

```ts
users.rebuildSortedIndex("age");
users.rebuildIndexes();
```

The stats include a `dirty` flag:

```ts
console.log(users.sortedIndexStats());
```

## Serialization

Sorted indexes are not serialized. Recreate them after deserialization:

```ts
const restored = table.deserialize(buffer);
restored.createSortedIndex("age");
```

See [Equality Indexes](./06-indexing.md) and [Performance and Benchmarks](./13-performance-and-benchmarks.md).

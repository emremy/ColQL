# Memory Model

ColQL memory usage is best understood as:

```txt
ColQL memory = storage + optional indexes + temporary materialization
```

## Base Storage

Base storage is columnar and chunked:

- Numeric columns use typed-array chunks.
- Dictionary columns encode string values into numeric codes.
- Boolean columns are bit-packed.

This avoids per-row object overhead while data remains inside ColQL.

## Dictionary Encoding

Dictionary columns store one of a fixed list of strings:

```ts
status: column.dictionary(["active", "passive", "archived"] as const)
```

Internally, each value is stored as a compact code. This is useful when many rows repeat the same small set of strings.

## Boolean Bit Storage

Boolean columns do not store one JavaScript boolean per row. They use bit storage, so many boolean values share the same byte-level backing storage.

## Chunked Storage

Chunking lets columns grow and physically delete rows without depending on one giant contiguous row-object array. Deletes operate on each column's chunked storage and remove empty chunks when possible.

## Index Memory

Indexes are separate derived performance structures:

- equality indexes store row-position buckets by value
- sorted indexes store row positions sorted by numeric value
- unique indexes store one row position per unique key and also enforce uniqueness

Indexes improve selected query shapes but increase memory. Equality and sorted indexes do not change query correctness; the same query must return the same result through an index or a full scan. Unique indexes are different: they are still derived structures, but they also reject duplicate keys and support by-key helpers.

```ts
users.dropIndex("status");
users.dropSortedIndex("age");
users.dropUniqueIndex("id");
```

## Materialization

`toArray()` creates JavaScript row objects:

```ts
const rows = users.where("status", "=", "active").toArray();
```

This can allocate memory proportional to the result size. Prefer these when possible:

```ts
users.where("status", "=", "active").count();
users.where("status", "=", "active").first();
users.where("status", "=", "active").forEach(row => console.log(row.id));

for (const row of users.where("status", "=", "active")) {
  console.log(row.id);
}
```

Use `select()` to reduce the size of materialized rows:

```ts
users.where("status", "=", "active").select(["id"]).toArray();
```

## Mutation Snapshots

Predicate update/delete snapshot matching row indexes before mutating. This keeps behavior safe and predictable, but broad predicate mutations allocate memory proportional to the number of matched row indexes.

```ts
users.where("age", ">=", 18).update({ is_active: true });
```

For very broad changes, expect temporary row-index snapshot memory.

## Practical Guidance

- Use indexes for selective hot queries.
- Avoid indexes for columns with low selectivity unless queries prove useful.
- Drop indexes to recover derived-memory overhead.
- Expect the first indexed query after mutation to include lazy rebuild cost if the needed index is dirty.
- Avoid `toArray()` for huge result sets when counting or streaming is enough.
- Remember that `heapUsed` alone can under-report typed-array storage; inspect `arrayBuffers` too.

See [Performance and Benchmarks](./13-performance-and-benchmarks.md).

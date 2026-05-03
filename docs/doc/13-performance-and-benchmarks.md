# Performance and Benchmarks

Benchmarks are local tools for understanding tradeoffs on your machine. They are not universal promises; results vary with Node version, CPU, memory pressure, data distribution, and query selectivity.

Benchmark numbers are not CI requirements. Use them as local diagnostics and rerun them on the hardware, Node version, data shape, and workload that matter to your application.

Build first:

```sh
npm run build
```

Then run:

```sh
npm run benchmark:memory
npm run benchmark:query
npm run benchmark:indexed
npm run benchmark:range
npm run benchmark:optimizer
npm run benchmark:serialization
npm run benchmark:delete
npm run benchmark:array-comparison
```

Most benchmark scripts accept larger scenarios with:

```sh
COLQL_BENCH_LARGE=1 npm run benchmark:indexed
```

## What Each Benchmark Measures

- `benchmark:memory`: object-array memory versus ColQL storage memory.
- `benchmark:query`: basic filter/projection/query execution behavior.
- `benchmark:indexed`: equality index creation and indexed query behavior.
- `benchmark:range`: sorted index range query behavior.
- `benchmark:optimizer`: planner behavior for selective and broad predicates.
- `benchmark:serialization`: serialize/deserialize timing and output size.
- `benchmark:delete`: physical delete, update, dirty index rebuild, and memory phases.
- `benchmark:physical-delete`: focused physical-delete behavior.
- `benchmark:array-comparison`: JS object arrays versus ColQL scan, equality, sorted, and unique-index paths across common workloads.

## JS Array Comparison Benchmark

`benchmark:array-comparison` compares plain JavaScript object arrays with ColQL scan, equality index, sorted index, and unique-index paths. It covers 1,000, 100,000, and 1,000,000 row datasets by default.

Workloads include:

- memory usage
- `fromRows` / `insertMany`
- structured filter/count
- projection plus limit
- find-by-id and unique `findBy`
- `exists` and `countWhere`
- range and broad scans
- callback `filter(fn)`
- predicate update/delete
- `updateBy` and `deleteBy`

Interpret this benchmark as a tradeoff map:

- JS arrays can be faster for small data, simple broad scans, callback predicates, and raw mutation transforms.
- ColQL is more useful when compact storage, schema validation, structured predicates, projection with limits, or explicit indexed lookups matter.
- `filter(fn)` is intentionally a full-scan escape hatch; prefer structured predicates when index planning or optimized scans matter.
- `deleteBy` on a unique index maintains physical row-position correctness, so it can cost more than `updateBy` on large tables.

Run only this comparison with:

```sh
npm run benchmark:array-comparison
```

For JSON output:

```sh
npm run benchmark:array-comparison -- --json
```

## Memory Metrics

Node reports typed-array backing memory under `arrayBuffers`, not only `heapUsed`. ColQL benchmarks often report:

```txt
tracked total = heapUsed + arrayBuffers
```

Use tracked total when comparing ColQL storage with object arrays.

In a local stabilization run on 2026-04-29, `benchmark:memory` reported for 100,000 rows:

| Storage | heapUsed | arrayBuffers | tracked total |
|---|---:|---:|---:|
| Object Array | 6.22 MB | 0.00 MB | 6.22 MB |
| ColQL | 0.08 MB | 0.77 MB | 0.84 MB |

Treat these as local reference numbers, not guarantees.

## Interpreting Index Benchmarks

Indexes help most when predicates are selective:

```ts
users.createIndex("id");
users.where("id", "=", 123).first();
```

Broad predicates may fall back to scan by planner choice. This is expected, not a failed index. Planner choices affect performance only, not query results.

In the same local run, `benchmark:indexed` showed selective `id = 99990` queries benefiting from the equality index, while `status in all` was close to scan time because the planner avoids broad index work. This is the expected tradeoff: indexes help selective lookups and cost memory.

`benchmark:range` showed sorted indexes helping selective ranges such as `age > 90`, while broad `age > 10` was similar to scan. It also showed that combining a selective equality index with an additional range filter can be much faster than scanning the broad range first.

`benchmark:optimizer` measures the planner choosing the smallest useful indexed candidate source and then applying remaining filters. Multiple predicates are combined at query time; ColQL does not build multi-column compound indexes.

## Interpreting Delete and Mutation Benchmarks

The delete benchmark separates phases:

- table build
- index creation
- single-row deletes and updates
- predicate update
- random deletes
- first indexed query after dirty indexes
- later indexed query after rebuild
- materialized query output
- index drop

The first indexed query after mutation may include lazy index rebuild cost. Dirty indexes are rebuilt before use and are not used to return stale results.

In the local delete/mutation run, the first indexed query after dirtying indexes was much slower than the second indexed query because it paid lazy rebuild cost. The benchmark also shows `toArray()` as a separate memory phase because it materializes row objects.

Broad mutations can still be slower than raw JavaScript array transforms. ColQL validates mutation payloads, snapshots matching row positions for all-or-nothing behavior, updates columnar storage, and marks or maintains derived indexes. This safety work is intentional; compare against arrays for the exact mutation shape you care about.

## Practical Advice

Measure the exact workload you care about:

- number of rows
- dictionary cardinality
- predicate selectivity
- projected columns
- amount of materialization
- mutation frequency
- index lifecycle

For small/simple data, a JavaScript array can be the better tool. ColQL becomes more useful when memory layout, structured predicates, or explicit indexed lookups matter.

See [Memory Model](./12-memory-model.md) for memory tradeoffs.

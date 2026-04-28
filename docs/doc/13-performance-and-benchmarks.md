# Performance and Benchmarks

Benchmarks are local tools for understanding tradeoffs on your machine. They are not universal promises; results vary with Node version, CPU, memory pressure, data distribution, and query selectivity.

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

## Memory Metrics

Node reports typed-array backing memory under `arrayBuffers`, not only `heapUsed`. ColQL benchmarks often report:

```txt
tracked total = heapUsed + arrayBuffers
```

Use tracked total when comparing ColQL storage with object arrays.

## Interpreting Index Benchmarks

Indexes help most when predicates are selective:

```ts
users.createIndex("id");
users.where("id", "=", 123).first();
```

Broad predicates may fall back to scan by planner choice. This is expected, not a failed index.

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

The first indexed query after mutation may include lazy index rebuild cost.

## Practical Advice

Measure the exact workload you care about:

- number of rows
- dictionary cardinality
- predicate selectivity
- projected columns
- amount of materialization
- mutation frequency
- index lifecycle

See [Memory Model](./12-memory-model.md) for memory tradeoffs.

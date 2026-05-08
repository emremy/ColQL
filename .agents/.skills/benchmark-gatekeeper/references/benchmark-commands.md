# Suggested Benchmark Commands

Build first when measuring package output:

```bash
npm run build
npm run benchmark:memory
npm run benchmark:query
npm run benchmark:indexed
npm run benchmark:range
npm run benchmark:optimizer
npm run benchmark:serialization
npm run benchmark:delete
npm run benchmark:physical-delete
npm run benchmark:array-comparison
npm run benchmark:session-analytics
```

For stronger memory signal:

```bash
node --expose-gc benchmarks/memory.mjs
```

For larger benchmark scenarios where supported:

```bash
COLQL_BENCH_LARGE=1 npm run benchmark:indexed
COLQL_BENCH_LARGE=1 npm run benchmark:range
ROWS=100000 npm run benchmark:session-analytics
```

# Release Checklist

This checklist is release evidence, not a benchmark promise. Run it from a clean working tree when preparing a tag.

## Required Gates

```sh
npm test
npm run test:types
npm run build
npm run test:worker-runtime
npm run bench:codspeed
npm run benchmark:background-indexing -- --json
npm run benchmark:worker-runtime -- --json
npm --cache /private/tmp/colql-npm-cache pack --dry-run
```

## v0.6.0 Background Indexing Checks

Confirm:

- Public query APIs remain synchronous.
- Normal query and mutation paths do not automatically schedule workers yet.
- `query.explain()` remains non-executing and does not schedule rebuilds.
- Queued, rebuilding, and failed indexes are not used for query results.
- Equality and sorted background rebuilds use typed-buffer output and stale-result discard.
- Unique indexes remain synchronous and main-thread-only.
- Serialization excludes indexes, lifecycle state, and worker jobs.
- Restored tables have data only; indexes must be recreated.
- Package dry-run includes built worker artifacts under `dist/indexing/background`.

## Benchmark Notes

Background-indexing benchmarks should report rows, chunks, worker count, rebuild mode, rebuild duration, fallback duration, output byte estimates, and memory counters including `heapUsed`, `rss`, `external`, and `arrayBuffers`.

Small worker benchmarks may be slower than synchronous rebuilds. That is expected when startup, message passing, and merge overhead dominate. Use 1M rows, and optionally 10M rows outside CI, to evaluate large dirty-index rebuild behavior.

## Known Build Warning

The CJS build can emit a `tsup` warning for `import.meta` in the internal worker executor. Treat it as non-blocking only when:

- `npm run test:worker-runtime` passes for built ESM and CJS worker artifacts.
- `npm --cache /private/tmp/colql-npm-cache pack --dry-run` includes the worker artifacts.
- No source-only worker path is required at runtime.

# ColQL Agent Guidelines

ColQL is a production-oriented in-memory columnar query engine for TypeScript.

This repository prioritizes correctness, predictable performance,
deterministic behavior, stable APIs, and memory efficiency over feature
velocity or abstraction complexity.

## Core Principles

### Correctness First

Correctness is more important than benchmark numbers, micro-optimizations,
feature count, or abstraction purity.

Never trade correctness for synthetic benchmark wins.

### Predictable Performance

ColQL prefers stable latency, bounded memory behavior, and explicit execution
cost over occasional peak throughput.

Avoid hidden allocations, accidental materialization, unstable planner behavior,
and benchmark tricks.

### Minimal Runtime Complexity

Avoid unnecessary runtime dependencies, abstractions, dynamic behavior, and
hidden magic.

New dependencies require strong justification.

## Project Map

Use these files as starting points for future tasks:

- `src/index.ts`: public package exports.
- `src/types.ts`: public TypeScript types and query/mutation shapes.
- `src/table.ts`: table API, storage coordination, mutation safety,
  serialization, indexes, and by-key helpers.
- `src/query.ts`: lazy query execution, filtering, projection, aggregation,
  mutation queries, `query.explain()`, and `__debugPlan()`.
- `src/storage/*`: numeric chunks, dictionary encoding, bit-packed booleans,
  and bitset internals.
- `src/indexing/*`: equality, sorted, unique, and planner/index lifecycle
  logic.
- `src/validation.ts` and `src/errors.ts`: runtime validation and structured
  `ColQLError` behavior.
- `tests/scenarios/*`: higher-level oracle/parity flows for realistic
  behavior.
- `docs/doc/*`: public documentation that must stay aligned with behavior.

## Critical Invariants

### rowIndex Is Not Stable

`rowIndex` must never be treated as a durable identifier.

Physical deletes may shift row positions. Do not expose row-index semantics as
stable IDs.

### Physical Deletes

Deletes are physical. ColQL does not use tombstones.

Deletes must preserve logical ordering, maintain column alignment, reduce
`rowCount`, and handle shifting row positions correctly. Predicate deletes must
snapshot target row indexes before mutation and delete from highest row index to
lowest when applying row-by-row deletion.

### Mutation Atomicity

Mutations must be all-or-nothing. Partial mutations are forbidden.

Predicate mutations must snapshot target rows before applying writes. Invalid
payloads and unique-index conflicts must be detected before writing.

### Index Lifecycle

Equality, sorted, and unique indexes are derived runtime state.

Indexes must be marked dirty after mutation, rebuild lazily before use, and
never return stale rows. Indexed query results must always match scan ground
truth.

Unique indexes also enforce duplicate-key protection while present and power
`findBy`, `updateBy`, and `deleteBy`.

### Serialization

Serialized state must not assume indexes are valid.

Equality, sorted, and unique indexes are rebuildable derived state and are not
serialized as trusted runtime state.

## Performance Guidelines

### Avoid Hot Path Allocations

Avoid these in hot paths:

- `Array.map` / `Array.filter` / `Array.reduce` chains
- spread syntax on large collections
- repeated object creation
- closure allocation inside loops
- unnecessary cloning

### Avoid Full Materialization

Do not materialize full rows unless explicitly required.

Prefer projection pushdown, columnar operations, and aggregation without row
expansion.

### Benchmark Discipline

Performance claims require before/after comparison, realistic dataset sizes,
and reproducible commands.

Prefer 100k and 1M rows for meaningful benchmark validation when the affected
path is performance-sensitive.

For memory-sensitive work, include `arrayBuffers`; typed-array and bitset
backing memory may not appear in `heapUsed`.

## Testing Expectations

All correctness-sensitive changes should include tests, especially planner
changes, index behavior, mutation logic, physical deletes, serialization, and
aggregation semantics.

Prefer deterministic tests, scan-vs-index validation, and JS array ground-truth
comparison.

For targeted validation, start with the nearest focused test file, then broaden
when the change crosses subsystem boundaries:

- storage changes: `tests/*-column.test.ts`, `tests/bitset.test.ts`,
  `tests/chunked-storage-production.test.ts`
- index changes: `tests/index*.test.ts`, `tests/sorted-index*.test.ts`,
  `tests/unique-index*.test.ts`, `tests/query-correctness-parity.test.ts`
- mutation changes: `tests/mutation.test.ts`, `tests/delete*.test.ts`,
  `tests/by-key-helpers.test.ts`, scenario consistency tests
- serialization changes: `tests/serialization*.test.ts`, serialization
  scenarios
- public types/API changes: `tests/type-inference.test-d.ts`,
  `docs/doc/16-api-reference.md`, README examples

## API Stability

Before v1.0.0, prefer stabilization over expansion. Avoid unnecessary API churn
and do not rename public methods casually.

Public behavior includes runtime semantics, TypeScript inference, error shapes,
serialization shape, query diagnostics from `query.explain()`, mutation return
shapes, and ordering behavior.

`query.__debugPlan()` exists for tests and low-level diagnostics, but
application code should not depend on it as a stable planning contract.

## Forbidden Patterns

Do not introduce:

- `rowIndex` as a stable ID
- unnecessary eager index rebuilds after mutation
- hidden row materialization
- full table cloning during mutation
- unnecessary runtime dependencies
- benchmark-only optimizations
- duplicated row-oriented storage
- serialized trusted index state

## Memory Philosophy

Memory usage matters. ColQL exists partly to avoid object-heavy memory
amplification.

Changes affecting typed-array chunks, bit-packed booleans, dictionary encoding,
materialization, indexes, or mutation snapshots should include memory analysis
when relevant.

## Repository Commands

Run tests:

```bash
npm test
```

Build:

```bash
npm run build
```

`dist/` is ignored and generated by the build.

Benchmarks:

```bash
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

Large benchmark modes where supported:

```bash
COLQL_BENCH_LARGE=1 npm run benchmark:indexed
COLQL_BENCH_LARGE=1 npm run benchmark:range
ROWS=100000 npm run benchmark:session-analytics
```

Memory-sensitive benchmarks should preferably use:

```bash
node --expose-gc
```

## Repo-Scoped Skills

Repo-specific Codex skills live under `.agents/.skills/*`.

When reviewing a change, use the relevant skill when it applies:

- `api-stability-guard`
- `benchmark-gatekeeper`
- `index-correctness-auditor`
- `memory-layout-analyzer`
- `mutation-safety-tester`

## Long-Term Direction

Current project focus:

- API stability
- correctness
- benchmark reliability
- predictable memory behavior
- production readiness before v1.0.0

Avoid major ecosystem expansion before core stabilization.

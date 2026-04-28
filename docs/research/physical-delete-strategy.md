# Physical Delete Strategy Research

> Historical research note: this document records the investigation that led to ColQL production chunked storage and physical deletes. Some benchmark sections describe prototypes and should be read as research context, not current API documentation. Current user-facing behavior is documented in `README.md`.

## 1. Problem Statement

At the time of this investigation, ColQL stored each production column in a compact single typed-array-like buffer. That is excellent for memory density and scan locality, but physical delete is expensive because deleting row `i` requires shifting every row after `i` left in every column.

For a table with many rows and several columns, the cost is roughly:

```txt
O((rowCount - i) * columnCount)
```

The worst case is deleting near the beginning of a large table. Indexes also use row ids, so physical row movement implies either complex incremental index updates or a rebuild.

## 2. Why Single-Buffer Physical Delete Is Expensive

A single `TypedArray` column is contiguous. Removing an element from the middle means there is no gap representation available; the bytes after the removed row must be shifted. Numeric and dictionary columns can use `copyWithin`, but the amount of copied data is still proportional to the number of remaining rows. BitSet-backed boolean columns are worse because individual bits must be shifted unless a specialized word-level delete is implemented.

This is predictable but can create high CPU latency for delete-heavy workloads.

## 3. Tombstone vs Physical Delete Tradeoff

Tombstones avoid shifting data by marking rows deleted. They are fast and stable at delete time, but they add long-term costs:

- scans must skip deleted rows
- storage is not immediately reclaimed
- cardinality and index statistics become more complex
- compaction eventually becomes necessary

Physical delete keeps storage logically clean and preserves `rowCount` semantics, but the data movement has to be bounded.

## 4. Chunked Storage Design

The prototype stores each column as fixed-capacity chunks:

```txt
age:
  chunk0: Uint8Array(65_536)
  chunk1: Uint8Array(65_536)
  chunk2: Uint8Array(65_536)
```

Each chunked column tracks:

```txt
chunkSize
chunks
lengths
rowCount
```

A physical delete shifts values only inside the located chunk, decrements that chunk length, and removes an empty chunk when safe.

The prototype includes:

- `uint32`
- `uint8`
- `float64`
- dictionary codes using `Uint8Array`, `Uint16Array`, or `Uint32Array`
- boolean chunks using packed bits
- a minimal experimental table with `insert`, `insertMany`, `get`, `delete`, `toArray`, `count`, and scan-based `where`

## 5. Strategy A: Variable Chunk Lengths

Implemented in the prototype.

After delete, a chunk can become shorter than neighboring chunks. No rows are pulled forward from later chunks.

Pros:

- delete moves at most one chunk of data per column
- no cascading cross-chunk movement
- memory spikes are avoided
- implementation is small and understandable

Cons:

- locating a logical row requires walking chunk lengths in this prototype
- random lookup can become slower as chunk count grows
- result order is preserved, but row location is no longer direct `rowIndex / chunkSize`
- production integration would likely need prefix sums or a Fenwick tree for faster locate

## 6. Strategy B: Pull-Forward / Rebalance

Not implemented in this prototype.

After delete, the first row from the next chunk could be pulled into the current chunk, then repeated across later chunks to keep all chunks dense.

Pros:

- direct row location remains possible
- chunks stay full except the tail chunk
- scans and random access remain simpler

Cons:

- delete can cascade across many chunks
- worst-case behavior approaches full-table shifting
- implementation is more complex
- more index row-id churn is likely

For ColQL's memory-conscious goals, Strategy A is the better first design to evaluate.

## 7. Complexity Comparison

| Operation | Single Buffer | Strategy A Chunked |
| --- | ---: | ---: |
| Append | Amortized O(1) | Amortized O(1) |
| Get / locate | O(1) | O(chunks) prototype, improvable |
| Delete first row | O(rowCount * columns) | O(chunkSize * columns) |
| Delete middle row | O(rowCount / 2 * columns) | O(chunkSize * columns) |
| Delete last row | O(1) | O(1) plus locate |
| Scan | O(rowCount) | O(rowCount + chunks) |
| Index maintenance | update/rebuild required | dirty + lazy rebuild recommended |

## 8. Benchmark Results

Environment-local benchmark command:

```sh
npm run benchmark:physical-delete
```

Dataset:

```txt
250,000 rows
schema: id uint32, age uint8, score float64, status dictionary code, is_active bit-packed boolean
```

Single-buffer simulation:

```txt
delete first row:      1.677ms
delete middle row:     0.367ms
delete last row:       0.012ms
delete 1k random rows: 4031.681ms
tracked total memory:  7.07 MB
```

Chunked Strategy A:

| Chunk size | First delete | Middle delete | Last delete | 1k random deletes | Tracked total memory | Random speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16,384 | 0.817ms | 0.058ms | 0.023ms | 209.813ms | 13.08 MB | 19.22x |
| 65,536 | 3.569ms | 0.245ms | 0.008ms | 879.902ms | 7.33 MB | 4.58x |
| 262,144 | 9.559ms | 4.316ms | 0.003ms | 3941.413ms | 7.33 MB | 1.02x |

A separate locate/get benchmark for one `uint32` column showed:

```txt
chunkSize 16,384: 10k random get 0.647ms
chunkSize 65,536: 10k random get 0.157ms
chunkSize 262,144: 10k random get 0.118ms
```

Interpretation:

- Small chunks are much better for repeated physical deletes.
- Large chunks reduce metadata and improve simple locate behavior, but delete cost approaches single-buffer behavior.
- `65,536` remains a plausible default candidate, but `16,384` gave much stronger delete behavior in this local run.
- Boolean bit shifting is a visible hidden cost; production boolean delete would need optimization.
- Memory measurements include runtime allocation behavior and should be treated as directional, not exact.

## 9. Index Handling

The prototype does not integrate with production equality or sorted indexes. It records the intended production rule: physical delete should mark indexes dirty, then indexed query paths should rebuild lazily before use.

This is preferable to incremental row-id maintenance during deletes because Strategy A changes logical row positions after every delete. Rebuilding derived indexes from the current chunks is simpler, safer, and consistent with ColQL's existing sorted-index dirty/rebuild approach.

Future design should evaluate:

- whether all indexes are invalidated after any delete
- whether only affected indexes are dirty
- whether bulk delete should delay rebuild until the first indexed query
- how to expose rebuild cost in benchmark output

## 10. Recommendation

Chunked physical delete looks viable as an experimental storage strategy, especially for workloads with repeated deletes away from the tail. The result is not yet ready to become the default production storage engine.

Recommended next step:

- keep the single-buffer engine as default
- continue chunked storage as an internal experiment
- optimize locate with prefix sums or a Fenwick tree
- optimize boolean chunk delete
- add scan benchmarks against production storage
- prototype lazy index rebuild over chunked rows
- consider an opt-in table/storage mode only after more evidence

Current recommendation: do not adopt chunked storage as the default yet. Consider it a promising opt-in candidate for delete-heavy workloads after further measurement.

## 11. Read/Query Tradeoff Analysis

This phase measured whether chunked storage harms read-heavy behavior. The benchmark scripts are intentionally standalone and storage-focused:

```sh
node benchmarks/chunked-read.mjs
node benchmarks/chunked-query.mjs
node benchmarks/chunked-insert.mjs
```

Important caveat: the production baseline uses the real public ColQL table implementation from `dist`, including its runtime validation and row materialization behavior. The chunked benchmark model is a benchmark-local storage model aligned with the experimental chunked design. These numbers are useful for architectural direction, but they should not be treated as final product-level performance claims.

Dataset:

```txt
250,000 rows
schema: id uint32, age uint8, score float64, status dictionary, is_active boolean
```

### Scan Performance

Full scan benchmark sums `age` over all rows.

| Storage | Scan time | Rows/sec | Relative to baseline |
| --- | ---: | ---: | ---: |
| Production baseline | 1.885ms | 132.6M | baseline |
| Chunk 16,384 via row lookup | 3.569ms | 70.0M | 89% slower |
| Chunk 65,536 via row lookup | 1.247ms | 200.4M | 34% faster |
| Chunk 262,144 via row lookup | 1.126ms | 222.1M | 40% faster |
| Chunk 16,384 direct chunk scan | 1.082ms | 231.0M | 43% faster |
| Chunk 65,536 direct chunk scan | 0.139ms | 1.8B | much faster microbenchmark |
| Chunk 262,144 direct chunk scan | 0.186ms | 1.34B | much faster microbenchmark |

Interpretation:

- Naive row-by-row lookup hurts small chunks because every lookup walks chunk lengths.
- Chunk-aware scans are very fast because they iterate directly over typed-array chunks.
- A production chunked engine should avoid per-row prefix lookup during scans and instead scan chunk-by-chunk.

### get(rowIndex) Performance

Random access benchmark performs 100,000 random `get(rowIndex)` calls.

| Storage | Time | Ops/sec | Relative to baseline |
| --- | ---: | ---: | ---: |
| Production baseline | 21.229ms | 4.71M | baseline |
| Chunk 16,384 | 23.277ms | 4.30M | 10% slower |
| Chunk 65,536 | 8.972ms | 11.15M | 58% faster |
| Chunk 262,144 | 5.862ms | 17.06M | 72% faster |

Interpretation:

- With only a few chunks, naive prefix lookup is acceptable.
- Smaller chunks can make random get slower unless locate is optimized.
- If chunked storage moves toward production, row location should use cached prefix offsets, binary search, or a Fenwick tree.

### Insert Performance

Insert benchmark compares one-by-one insert and `insertMany`.

| Storage | insert | insertMany |
| --- | ---: | ---: |
| Production baseline | 125.885ms | 161.756ms |
| Chunk 16,384 | 15.081ms | 12.818ms |
| Chunk 65,536 | 13.264ms | 10.019ms |
| Chunk 262,144 | 11.527ms | 9.125ms |

Interpretation:

- The chunked benchmark path is much faster, but this is partly because it is storage-focused and avoids some public-table validation/planner machinery.
- Larger chunks are slightly better for append-heavy workloads because they allocate fewer chunks.
- This does not prove production inserts would be 10x faster after migration; it only shows chunked append itself is not a bottleneck.

### Query Performance

Scan-based query benchmark measured:

```ts
where("age", ">", 18)
where("status", "=", "active")
where("id", "=", target)
```

| Storage | age > 18 | status = active | id = target |
| --- | ---: | ---: | ---: |
| Production baseline | 9.579ms | 8.611ms | 7.911ms |
| Chunk 16,384 | 4.147ms | 4.327ms | 4.438ms |
| Chunk 65,536 | 1.855ms | 2.127ms | 1.659ms |
| Chunk 262,144 | 1.507ms | 2.029ms | 1.609ms |

Interpretation:

- The benchmark-local chunked query loop does not show read/query harm.
- Larger chunks are best for scan-style predicates because there are fewer chunk-length checks.
- A production implementation would need a real apples-to-apples integration benchmark before claiming query speedups.

### Combined Workload

Combined workload:

```txt
insert existing dataset -> delete 10,000 random rows -> scan age -> query status
```

| Chunk size | Combined workload time |
| ---: | ---: |
| 16,384 | 237.207ms |
| 65,536 | 853.743ms |
| 262,144 | 3386.066ms |

Interpretation:

- The combined workload is dominated by delete cost.
- `16,384` chunks are clearly best for delete-heavy mixed workloads.
- `65,536` remains a compromise if read behavior and metadata overhead matter more than maximum delete speed.

### Memory Behavior

Representative read benchmark tracked totals:

| Storage | Tracked total |
| --- | ---: |
| Production baseline | 13.33 MB |
| Chunk 16,384 | 12.93 MB |
| Chunk 65,536 | 17.75 MB |
| Chunk 262,144 | 20.93 MB |

Representative query benchmark tracked totals:

| Storage | Tracked total |
| --- | ---: |
| Production baseline | 11.94 MB |
| Chunk 16,384 | 12.29 MB |
| Chunk 65,536 | 15.45 MB |
| Chunk 262,144 | 19.39 MB |

Interpretation:

- Raw storage bytes are similar because columns store the same primitive values.
- Larger chunks can over-allocate more tail capacity, increasing tracked `arrayBuffers`.
- Smaller chunks increase metadata but reduce wasted tail capacity.
- Memory results are runtime-sensitive and should be compared directionally.

## 12. Historical Conclusion: Should ColQL Adopt Chunked Storage?

ADOPTED.

After this research, ColQL promoted chunked columnar storage to the production default. The remaining notes in this section are historical tradeoff analysis and follow-up ideas.

Best current interpretation:

- For delete-heavy workloads, chunked storage is viable and worth continuing.
- For read-heavy workloads, chunked storage does not appear inherently harmful if scans are chunk-aware.
- For random `get(rowIndex)`, naive prefix lookup is acceptable with larger chunks but needs optimization for smaller chunks.
- For memory, chunk size matters: smaller chunks reduce tail waste but add metadata; larger chunks improve read locality but lose delete benefits.

Best chunk size from this phase:

- `16,384` for delete-heavy workloads.
- `65,536` as the best balanced candidate.
- `262,144` is not attractive for physical delete because it behaves too much like single-buffer storage.

Historical recommended path before production adoption:

- keep monitoring production chunked storage behavior
- keep prototype/research benchmarks clearly separated from package contents
- implement prefix lookup optimization
- add production-integrated chunk-aware scan paths
- benchmark index rebuild after deletes
- continue evaluating whether any advanced chunk-size configuration is worth exposing

## Delete Benchmark Memory Attribution

The production delete benchmark now records memory at explicit phase checkpoints with forced GC when available:

```sh
npm run benchmark:delete
```

Representative run for `250,000` rows:

```txt
start:                         total  3.73 MB
after single deletes:          total  3.97 MB
after build:                   total  8.39 MB
after indexes:                 total 31.49 MB
after random index generation: total 31.50 MB
after 1k random deletes:       total 31.55 MB
after delete GC:               total 31.54 MB
before query:                  total 31.53 MB
after first indexed count:     total 31.54 MB
after second indexed count:    total 31.54 MB
after query toArray:           total 39.24 MB
after query result released:   total 31.56 MB
after indexes dropped:         total  8.55 MB
```

Observed attribution:

- Storage build accounts for the expected typed-array/bitset buffers: about `8.39 MB` tracked total in this run.
- Equality indexes account for the large persistent heap increase: total rises from `8.39 MB` to `31.49 MB` after `createIndex("id")` and `createIndex("status")`.
- Random delete index generation is negligible at this scale.
- Physical deletes do not materially increase retained memory: `31.50 MB` after random index generation vs `31.55 MB` after 1,000 deletes.
- The first indexed query after deletes is slower because it includes lazy index rebuild (`54.420ms` vs `8.325ms` for the second indexed count), but retained memory remains stable.
- `toArray()` materializes result rows and temporarily increases memory (`39.24 MB`), then returns to the indexed baseline after the result reference is released.
- Dropping indexes returns total memory close to post-build storage memory (`8.55 MB`), which confirms the high post-query total was index memory, not chunked storage leakage.

Conclusion: this benchmark did not reveal a production storage leak. The earlier high `tracked total memory` number was mostly equality index heap plus temporary `toArray()` materialization when result rows were retained long enough to be measured.

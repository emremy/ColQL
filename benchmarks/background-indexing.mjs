import os from "node:os";
import { column, table } from "../dist/index.mjs";
import { NodeBackgroundWorkerExecutor } from "../dist/indexing/background/node-worker-executor.mjs";
import { loadBackgroundBenchmarkInternals } from "./background-benchmark-internals.mjs";

const {
  Table,
  NumericColumnStorage,
  DictionaryColumnStorage,
  createEqualityBackgroundJob,
  equalityBackgroundRebuildEligibility,
  executeEqualityChunkRebuild,
  createSortedBackgroundJob,
  executeSortedChunkRebuild,
  sortedBackgroundRebuildEligibility,
  BackgroundWorkerPool,
} = await loadBackgroundBenchmarkInternals();

const DEFAULT_ROWS = 100_000;
const LARGE_ROWS = 1_000_000;
const OPTIONAL_10M_ROWS = 10_000_000;
const CHUNK_SIZE = 65_536;
const STATUS_VALUES = ["active", "passive", "trial"];
const jsonOutput = process.argv.includes("--json");
const workerCount = process.env.WORKER_COUNT
  ? Number.parseInt(process.env.WORKER_COUNT, 10)
  : Math.max(1, Math.min(4, (os.availableParallelism?.() ?? os.cpus().length) - 1));
const rowCounts = process.env.ROWS
  ? [Number.parseInt(process.env.ROWS, 10)]
  : process.env.COLQL_BENCH_10M === "1"
    ? [OPTIONAL_10M_ROWS]
    : process.env.COLQL_BENCH_LARGE === "1"
      ? [DEFAULT_ROWS, LARGE_ROWS]
      : [DEFAULT_ROWS];

for (const rows of rowCounts) {
  if (!Number.isInteger(rows) || rows < 1) {
    throw new Error(`Invalid row count: ${String(rows)}.`);
  }
}
if (!Number.isInteger(workerCount) || workerCount < 1) {
  throw new Error(`Invalid WORKER_COUNT: ${String(process.env.WORKER_COUNT)}.`);
}

const schema = {
  id: column.uint32(),
  score: column.uint32(),
  status: column.dictionary(STATUS_VALUES),
};

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

async function timeAsync(fn) {
  const start = performance.now();
  const result = await fn();
  return { duration: performance.now() - start, result };
}

function memorySnapshot() {
  const memory = process.memoryUsage();
  return {
    heapUsed: memory.heapUsed,
    rss: memory.rss,
    external: memory.external,
    arrayBuffers: memory.arrayBuffers,
  };
}

function memoryDelta(before, after) {
  return {
    heapUsed: after.heapUsed - before.heapUsed,
    rss: after.rss - before.rss,
    external: after.external - before.external,
    arrayBuffers: after.arrayBuffers - before.arrayBuffers,
  };
}

function createIndexedTable(rowCount) {
  const rows = new Table(schema, rowCount, {
    storages: {
      id: NumericColumnStorage.withSharedBuffer("uint32", rowCount, undefined, 0, CHUNK_SIZE),
      score: NumericColumnStorage.withSharedBuffer("uint32", rowCount, undefined, 0, CHUNK_SIZE),
      status: DictionaryColumnStorage.withSharedBuffer(STATUS_VALUES, rowCount, undefined, 0, CHUNK_SIZE),
    },
  });

  for (let id = 0; id < rowCount; id += 1) {
    rows.insert({
      id,
      score: (id * 1_103) % 1_000_000,
      status: id % 17 === 0 ? "trial" : id % 3 === 0 ? "passive" : "active",
    });
  }

  rows.createIndex("status").createSortedIndex("score");
  return rows;
}

function tableInternals(rows) {
  return {
    manager: rows.indexManager,
    storages: rows.storages,
  };
}

function equalityMetadata(manager, columnName, jobId) {
  const snapshot = manager.lifecycleSnapshot("equality", columnName);
  return {
    jobId,
    indexId: `equality:${columnName}`,
    indexKind: "equality",
    columnName,
    generation: snapshot.generation,
    columnEpoch: snapshot.columnEpoch,
  };
}

function sortedMetadata(manager, columnName, rowCount, jobId) {
  const snapshot = manager.lifecycleSnapshot("sorted", columnName);
  return {
    jobId,
    indexId: `sorted:${columnName}`,
    indexKind: "sorted",
    columnName,
    generation: snapshot.generation,
    columnEpoch: snapshot.columnEpoch,
    rowCount,
  };
}

async function runPool(job, executor, count = workerCount) {
  const results = [];
  let resolveDone;
  let rejectDone;
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  const pool = new BackgroundWorkerPool({
    workerCount: count,
    executor,
    onTaskComplete: (taskResult) => results.push(taskResult.result),
    onJobCompleted: () => resolveDone(),
    onJobFailed: (_snapshot, error) => rejectDone(error),
  });

  pool.submitJob(job);
  await done;
  return results.sort((left, right) => left.chunkIndex - right.chunkIndex);
}

async function runWorkerJob(job, count = workerCount) {
  const executor = new NodeBackgroundWorkerExecutor({ workerCount: count });
  try {
    return await runPool(job, executor.execute, count);
  } finally {
    await executor.dispose();
  }
}

function assertCount(label, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${label} sanity check failed: expected ${expected}, received ${actual}.`);
  }
}

function push(results, rowCount, mode, operation, extra) {
  results.push({
    rows: rowCount,
    chunks: extra.chunks,
    workerCount: extra.workerCount ?? 0,
    mode,
    operation,
    ms: extra.ms,
    resultCount: extra.resultCount,
    rebuildMs: extra.rebuildMs,
    applyMs: extra.applyMs,
    fallbackMs: extra.fallbackMs,
    estimatedOutputBytes: extra.estimatedOutputBytes,
    transferBytes: extra.transferBytes,
    memoryDelta: extra.memoryDelta,
    staleResultsDiscarded: extra.staleResultsDiscarded ?? 0,
  });
}

function transferBytes(results) {
  return results.reduce((total, result) => total + result.byteLength, 0);
}

async function benchmarkEquality(rowCount, mode) {
  const rows = createIndexedTable(rowCount);
  rows.update(1, { status: "trial" });
  const expected = rows.where("status", "=", "trial").explain().indexState === "dirty"
    ? trialCountAfterRowOneUpdate(rowCount)
    : rows.where("status", "=", "trial").count();

  if (mode === "sync") {
    const run = time(() => rows.where("status", "=", "trial").count());
    assertCount("sync equality rebuild query", run.result, expected);
    return { chunks: 0, ms: run.duration, resultCount: run.result };
  }

  const { manager, storages } = tableInternals(rows);
  const metadata = equalityMetadata(manager, "status", `${mode}:eq:${rowCount}`);
  const descriptor = storages.status.describeChunks();
  const eligibility = equalityBackgroundRebuildEligibility(descriptor);
  const job = createEqualityBackgroundJob(metadata, descriptor);
  manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column", {
    chunksTotal: job.tasks.length,
    workerCount,
  });
  manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column");

  const fallback = time(() => rows.where("status", "=", "trial").count());
  assertCount(`${mode} equality fallback query`, fallback.result, expected);
  const before = memorySnapshot();
  const rebuild = await timeAsync(() =>
    mode === "mock-background"
      ? runPool(job, executeEqualityChunkRebuild)
      : runWorkerJob(job)
  );
  const after = memorySnapshot();
  const apply = time(() => manager.completeEqualityBackgroundRebuild(metadata, rebuild.result));
  if (apply.result !== "applied") {
    throw new Error(`${mode} equality apply failed: ${String(apply.result)}.`);
  }
  const indexed = rows.where("status", "=", "trial").count();
  assertCount(`${mode} equality indexed query`, indexed, expected);

  return {
    chunks: job.tasks.length,
    workerCount,
    ms: fallback.duration + rebuild.duration + apply.duration,
    fallbackMs: fallback.duration,
    rebuildMs: rebuild.duration,
    applyMs: apply.duration,
    resultCount: indexed,
    estimatedOutputBytes: eligibility.estimatedOutputBytes,
    transferBytes: transferBytes(rebuild.result),
    memoryDelta: memoryDelta(before, after),
  };
}

async function benchmarkSorted(rowCount, mode) {
  const rows = createIndexedTable(rowCount);
  rows.update(0, { score: 42 });
  const expected = rows.where("score", "<", 50_000).explain().indexState === "dirty"
    ? countScoresBelow(rowCount, 50_000, 0, 42)
    : rows.where("score", "<", 50_000).count();

  if (mode === "sync") {
    const run = time(() => rows.where("score", "<", 50_000).count());
    assertCount("sync sorted rebuild query", run.result, expected);
    return { chunks: 0, ms: run.duration, resultCount: run.result };
  }

  const { manager, storages } = tableInternals(rows);
  const metadata = sortedMetadata(manager, "score", rows.rowCount, `${mode}:sorted:${rowCount}`);
  const descriptor = storages.score.describeChunks();
  const eligibility = sortedBackgroundRebuildEligibility(descriptor);
  const job = createSortedBackgroundJob(metadata, descriptor);
  manager.startSortedBackgroundRebuild(metadata, "update:indexed-column", {
    chunksTotal: job.tasks.length,
    workerCount,
  });
  manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column");

  const fallback = time(() => rows.where("score", "<", 50_000).count());
  assertCount(`${mode} sorted fallback query`, fallback.result, expected);
  const before = memorySnapshot();
  const rebuild = await timeAsync(() =>
    mode === "mock-background"
      ? runPool(job, executeSortedChunkRebuild)
      : runWorkerJob(job)
  );
  const after = memorySnapshot();
  const apply = time(() => manager.completeSortedBackgroundRebuild(metadata, rebuild.result));
  if (apply.result !== "applied") {
    throw new Error(`${mode} sorted apply failed: ${String(apply.result)}.`);
  }
  const indexed = rows.where("score", "<", 50_000).count();
  assertCount(`${mode} sorted indexed query`, indexed, expected);

  return {
    chunks: job.tasks.length,
    workerCount,
    ms: fallback.duration + rebuild.duration + apply.duration,
    fallbackMs: fallback.duration,
    rebuildMs: rebuild.duration,
    applyMs: apply.duration,
    resultCount: indexed,
    estimatedOutputBytes: eligibility.estimatedOutputBytes,
    transferBytes: transferBytes(rebuild.result),
    memoryDelta: memoryDelta(before, after),
  };
}

async function benchmarkStaleDiscard(rowCount, kind) {
  const rows = createIndexedTable(rowCount);
  const { manager, storages } = tableInternals(rows);

  if (kind === "equality") {
    rows.update(1, { status: "trial" });
    const metadata = equalityMetadata(manager, "status", `stale:eq:${rowCount}`);
    const job = createEqualityBackgroundJob(metadata, storages.status.describeChunks());
    manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column");
    manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column");
    const outputs = await runPool(job, executeEqualityChunkRebuild);
    rows.update(2, { status: "trial" });
    const discard = time(() => manager.completeEqualityBackgroundRebuild(metadata, outputs));
    if (discard.result !== "stale") throw new Error("Expected stale equality result.");
    return {
      chunks: job.tasks.length,
      ms: discard.duration,
      resultCount: rows.where("status", "=", "trial").count(),
      staleResultsDiscarded: manager.diagnostics().find((entry) => entry.kind === "equality" && entry.column === "status")?.staleResultsDiscarded ?? 0,
    };
  }

  rows.update(0, { score: 42 });
  const metadata = sortedMetadata(manager, "score", rows.rowCount, `stale:sorted:${rowCount}`);
  const job = createSortedBackgroundJob(metadata, storages.score.describeChunks());
  manager.startSortedBackgroundRebuild(metadata, "update:indexed-column");
  manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column");
  const outputs = await runPool(job, executeSortedChunkRebuild);
  rows.delete(1);
  const discard = time(() => manager.completeSortedBackgroundRebuild(metadata, outputs));
  if (discard.result !== "stale") throw new Error("Expected stale sorted result.");
  return {
    chunks: job.tasks.length,
    ms: discard.duration,
    resultCount: rows.where("score", "<", 50_000).count(),
    staleResultsDiscarded: manager.diagnostics().find((entry) => entry.kind === "sorted" && entry.column === "score")?.staleResultsDiscarded ?? 0,
  };
}

function benchmarkRestoreReindex(rowCount) {
  const rows = createIndexedTable(rowCount);
  const serialized = rows.serialize();
  const restore = time(() => table.deserialize(serialized));
  const reindex = time(() => {
    restore.result.createIndex("status").createSortedIndex("score");
    return restore.result.indexes().length + restore.result.sortedIndexes().length;
  });
  assertCount("restore/reindex count", reindex.result, 2);
  return {
    chunks: 0,
    ms: restore.duration + reindex.duration,
    resultCount: restore.result.rowCount,
    rebuildMs: reindex.duration,
  };
}

function countScoresBelow(rowCount, limit, changedRow, changedValue) {
  let count = 0;
  for (let id = 0; id < rowCount; id += 1) {
    const score = id === changedRow ? changedValue : (id * 1_103) % 1_000_000;
    if (score < limit) count += 1;
  }
  return count;
}

function trialCountAfterRowOneUpdate(rowCount) {
  let count = 0;
  for (let id = 0; id < rowCount; id += 1) {
    const status = id === 1
      ? "trial"
      : id % 17 === 0 ? "trial" : id % 3 === 0 ? "passive" : "active";
    if (status === "trial") count += 1;
  }
  return count;
}

function formatBytes(value) {
  if (value === undefined) return "";
  return `${(value / 1024 / 1024).toFixed(2)} MiB`;
}

function printHuman(results) {
  console.log("ColQL background indexing benchmark");
  console.log(`Node ${process.version} on ${process.platform} ${process.arch}`);
  console.log(`CPU: ${os.cpus()[0]?.model ?? "unknown"} (${os.cpus().length} logical cores)`);
  console.log(`Worker count: ${workerCount}`);
  console.log("Caveats: local machine only; real workers include startup and message overhead; background scheduling is still internal/manual in this phase.");
  console.log("Tip: use COLQL_BENCH_LARGE=1 for 1M rows, COLQL_BENCH_10M=1 for an optional manual 10M run, or -- --json for machine-readable output.\n");
  console.log("Rows       Mode             Operation                 Chunks Workers Result       Total ms  Fallback  Rebuild   Apply     Transfer   Est output Stale");
  console.log("------------------------------------------------------------------------------------------------------------------------------------------------");
  for (const result of results) {
    console.log(
      `${String(result.rows).padStart(9)}  ${result.mode.padEnd(16)} ${result.operation.padEnd(25)} ${String(result.chunks ?? 0).padStart(6)} ${String(result.workerCount ?? 0).padStart(7)} ${String(result.resultCount ?? "").padStart(8)} ${formatMs(result.ms).padStart(10)} ${formatMs(result.fallbackMs).padStart(9)} ${formatMs(result.rebuildMs).padStart(8)} ${formatMs(result.applyMs).padStart(8)} ${formatBytes(result.transferBytes).padStart(10)} ${formatBytes(result.estimatedOutputBytes).padStart(10)} ${String(result.staleResultsDiscarded ?? 0).padStart(5)}`,
    );
  }
}

function formatMs(value) {
  return value === undefined ? "" : value.toFixed(3);
}

async function main() {
  const results = [];
  for (const rows of rowCounts) {
    push(results, rows, "sync", "equality rebuild query", await benchmarkEquality(rows, "sync"));
    push(results, rows, "mock-background", "equality rebuild", await benchmarkEquality(rows, "mock-background"));
    push(results, rows, "real-worker", "equality rebuild", await benchmarkEquality(rows, "real-worker"));
    push(results, rows, "sync", "sorted rebuild query", await benchmarkSorted(rows, "sync"));
    push(results, rows, "mock-background", "sorted rebuild", await benchmarkSorted(rows, "mock-background"));
    push(results, rows, "real-worker", "sorted rebuild", await benchmarkSorted(rows, "real-worker"));
    push(results, rows, "mock-background", "equality stale discard", await benchmarkStaleDiscard(rows, "equality"));
    push(results, rows, "mock-background", "sorted stale discard", await benchmarkStaleDiscard(rows, "sorted"));
    push(results, rows, "sync", "restore and reindex", benchmarkRestoreReindex(rows));
  }

  if (jsonOutput) {
    console.log(JSON.stringify({ workerCount, results }, null, 2));
  } else {
    printHuman(results);
  }
}

await main();

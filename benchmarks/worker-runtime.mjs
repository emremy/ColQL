import os from "node:os";
import { NodeBackgroundWorkerExecutor } from "../dist/indexing/background/node-worker-executor.mjs";
import { loadBackgroundBenchmarkInternals } from "./background-benchmark-internals.mjs";

const {
  NumericColumnStorage,
  DictionaryColumnStorage,
  createEqualityBackgroundJob,
  executeEqualityChunkRebuild,
  createSortedBackgroundJob,
  executeSortedChunkRebuild,
  BackgroundWorkerPool,
} = await loadBackgroundBenchmarkInternals();

const DEFAULT_ROWS = 100_000;
const CHUNK_SIZE = 65_536;
const rows = process.env.ROWS ? Number.parseInt(process.env.ROWS, 10) : DEFAULT_ROWS;
const workerCount = process.env.WORKER_COUNT
  ? Number.parseInt(process.env.WORKER_COUNT, 10)
  : Math.max(1, Math.min(4, (os.availableParallelism?.() ?? os.cpus().length) - 1));
const jsonOutput = process.argv.includes("--json");

if (!Number.isInteger(rows) || rows < 1) throw new Error(`Invalid ROWS: ${String(process.env.ROWS)}.`);
if (!Number.isInteger(workerCount) || workerCount < 1) throw new Error(`Invalid WORKER_COUNT: ${String(process.env.WORKER_COUNT)}.`);

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

function createNumericStorage() {
  const data = new Uint32Array(rows);
  for (let index = 0; index < rows; index += 1) {
    data[index] = (index * 1_103) % 1_000_000;
  }
  return NumericColumnStorage.withSharedBuffer("uint32", rows, data, rows, CHUNK_SIZE);
}

function createDictionaryStorage() {
  const data = new Uint8Array(rows);
  for (let index = 0; index < rows; index += 1) {
    data[index] = index % 3;
  }
  return DictionaryColumnStorage.withSharedBuffer(["active", "passive", "trial"], rows, data, rows, CHUNK_SIZE);
}

function equalityMetadata(columnName, jobId) {
  return {
    jobId,
    indexId: `equality:${columnName}`,
    indexKind: "equality",
    columnName,
    generation: 1,
    columnEpoch: 1,
  };
}

function sortedMetadata(columnName, jobId) {
  return {
    jobId,
    indexId: `sorted:${columnName}`,
    indexKind: "sorted",
    columnName,
    generation: 1,
    columnEpoch: 1,
    rowCount: rows,
  };
}

async function runPool(job, executor) {
  const results = [];
  let resolveDone;
  let rejectDone;
  const done = new Promise((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  const pool = new BackgroundWorkerPool({
    workerCount,
    executor,
    onTaskComplete: (result) => results.push(result.result),
    onJobCompleted: () => resolveDone(),
    onJobFailed: (_snapshot, error) => rejectDone(error),
  });
  pool.submitJob(job);
  await done;
  return results;
}

async function runWorkerPool(job) {
  const executor = new NodeBackgroundWorkerExecutor({ workerCount });
  try {
    return await runPool(job, executor.execute);
  } finally {
    await executor.dispose();
  }
}

async function benchmarkStartup() {
  const cold = await timeAsync(async () => {
    const executor = new NodeBackgroundWorkerExecutor({ workerCount: 1 });
    try {
      await executor.ping();
    } finally {
      await executor.dispose();
    }
  });

  const executor = new NodeBackgroundWorkerExecutor({ workerCount });
  try {
    const warm = await timeAsync(async () => {
      for (let index = 0; index < 25; index += 1) {
        await executor.ping();
      }
    });
    return {
      coldWorkerStartupMs: cold.duration,
      warmPingsMs: warm.duration,
      warmPingAverageMs: warm.duration / 25,
    };
  } finally {
    await executor.dispose();
  }
}

async function main() {
  const numeric = createNumericStorage();
  const dictionary = createDictionaryStorage();
  const equalityNumericJob = createEqualityBackgroundJob(equalityMetadata("score", "worker:eq:numeric"), numeric.describeChunks());
  const equalityDictionaryJob = createEqualityBackgroundJob(equalityMetadata("status", "worker:eq:dictionary"), dictionary.describeChunks());
  const sortedJob = createSortedBackgroundJob(sortedMetadata("score", "worker:sorted"), numeric.describeChunks());
  const startup = await benchmarkStartup();

  const fakeEquality = await timeAsync(() => runPool(equalityNumericJob, executeEqualityChunkRebuild));
  const workerEquality = await timeAsync(() => runWorkerPool(equalityNumericJob));
  const workerDictionary = await timeAsync(() => runWorkerPool(equalityDictionaryJob));
  const fakeSorted = await timeAsync(() => runPool(sortedJob, executeSortedChunkRebuild));
  const workerSorted = await timeAsync(() => runWorkerPool(sortedJob));

  const results = [
    {
      operation: "cold worker startup + ping",
      rows: 0,
      chunks: 0,
      workerCount: 1,
      ms: startup.coldWorkerStartupMs,
    },
    {
      operation: "warm pooled ping x25",
      rows: 0,
      chunks: 0,
      workerCount,
      ms: startup.warmPingsMs,
      averageMs: startup.warmPingAverageMs,
    },
    {
      operation: "fake equality numeric tasks",
      rows,
      chunks: equalityNumericJob.tasks.length,
      workerCount,
      ms: fakeEquality.duration,
    },
    {
      operation: "real-worker equality numeric tasks",
      rows,
      chunks: equalityNumericJob.tasks.length,
      workerCount,
      ms: workerEquality.duration,
      transferBytes: sumBytes(workerEquality.result),
    },
    {
      operation: "real-worker equality dictionary tasks",
      rows,
      chunks: equalityDictionaryJob.tasks.length,
      workerCount,
      ms: workerDictionary.duration,
      transferBytes: sumBytes(workerDictionary.result),
    },
    {
      operation: "fake sorted numeric tasks",
      rows,
      chunks: sortedJob.tasks.length,
      workerCount,
      ms: fakeSorted.duration,
    },
    {
      operation: "real-worker sorted numeric tasks",
      rows,
      chunks: sortedJob.tasks.length,
      workerCount,
      ms: workerSorted.duration,
      transferBytes: sumBytes(workerSorted.result),
    },
  ];

  if (jsonOutput) {
    console.log(JSON.stringify({ workerCount, rows, results }, null, 2));
  } else {
    printHuman(results);
  }
}

function sumBytes(results) {
  return results.reduce((total, result) => total + result.byteLength, 0);
}

function printHuman(results) {
  console.log("ColQL worker runtime benchmark");
  console.log(`Node ${process.version} on ${process.platform} ${process.arch}`);
  console.log(`CPU: ${os.cpus()[0]?.model ?? "unknown"} (${os.cpus().length} logical cores)`);
  console.log(`Rows: ${rows}, worker count: ${workerCount}`);
  console.log("Caveats: local machine only; cold worker startup is expected to be noisy, warm task throughput is the more useful regression signal.\n");
  console.log("Operation                              Rows    Chunks Workers  Time (ms)  Avg (ms)  Transfer");
  console.log("---------------------------------------------------------------------------------------------");
  for (const result of results) {
    console.log(
      `${result.operation.padEnd(38)} ${String(result.rows).padStart(7)} ${String(result.chunks).padStart(7)} ${String(result.workerCount).padStart(7)} ${result.ms.toFixed(3).padStart(10)} ${formatOptional(result.averageMs).padStart(9)} ${formatBytes(result.transferBytes).padStart(9)}`,
    );
  }
}

function formatOptional(value) {
  return value === undefined ? "" : value.toFixed(3);
}

function formatBytes(value) {
  return value === undefined ? "" : `${(value / 1024 / 1024).toFixed(2)} MiB`;
}

await main();

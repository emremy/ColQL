import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import {
  createEqualityBackgroundJob,
  executeEqualityChunkRebuild,
  type EqualityEncodedChunkResult,
} from "../src/indexing/background/equality-rebuild";
import {
  createSortedBackgroundJob,
  executeSortedChunkRebuild,
  type SortedEncodedChunkResult,
} from "../src/indexing/background/sorted-rebuild";
import { BackgroundWorkerPool } from "../src/indexing/background/worker-pool";
import type { BackgroundChunkJob, BackgroundChunkTask } from "../src/indexing/background/types";
import type { BackgroundWorkerTaskPayload, BackgroundWorkerTaskResult } from "../src/indexing/background/worker-protocol";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";

type NodeWorkerExecutorModule = {
  readonly NodeBackgroundWorkerExecutor: new (options: {
    readonly workerCount?: number;
    readonly workerUrl?: URL;
  }) => {
    readonly execute: (
      task: BackgroundChunkTask<BackgroundWorkerTaskPayload>,
    ) => Promise<BackgroundWorkerTaskResult>;
    ping(): Promise<"pong">;
    dispose(): Promise<void>;
  };
};

const workerEntryUrl = pathToFileURL(resolve("dist/indexing/background/worker-entry.mjs"));
const workerExecutorPath = resolve("dist/indexing/background/node-worker-executor.mjs");
const cjsWorkerExecutorPath = resolve("dist/indexing/background/node-worker-executor.js");
const hasBuiltWorkerArtifacts = existsSync(workerEntryUrl) &&
  existsSync(workerExecutorPath) &&
  existsSync(cjsWorkerExecutorPath);
const describeBuiltWorker = hasBuiltWorkerArtifacts ? describe : describe.skip;
const executors: Array<{ dispose(): Promise<void> }> = [];

afterAll(async () => {
  await Promise.all(executors.map((executor) => executor.dispose()));
});

describeBuiltWorker("real worker_threads background runtime smoke", () => {
  it("starts the built worker artifact and responds to ping", async () => {
    const executor = await createExecutor(1);
    await expect(executor.ping()).resolves.toBe("pong");
  });

  it("resolves the built ESM worker artifact and responds to ping", async () => {
    const module = await import("../dist/indexing/background/node-worker-executor.mjs") as NodeWorkerExecutorModule;
    const executor = new module.NodeBackgroundWorkerExecutor({ workerCount: 1 });
    executors.push(executor);

    await expect(executor.ping()).resolves.toBe("pong");
  });

  it("resolves the built CJS worker artifact and responds to ping", async () => {
    const require = createRequire(import.meta.url);
    const module = require("../dist/indexing/background/node-worker-executor.js") as NodeWorkerExecutorModule;
    const executor = new module.NodeBackgroundWorkerExecutor({ workerCount: 1 });
    executors.push(executor);

    await expect(executor.ping()).resolves.toBe("pong");
  });

  it("matches fake equality executor output for SAB-backed numeric chunks", async () => {
    const storage = NumericColumnStorage.withSharedBuffer(
      "uint32",
      4,
      new Uint32Array([2, 1, 2, 3]),
      4,
      2,
    );
    const job = createEqualityBackgroundJob({
      jobId: "eq:numeric",
      indexId: "equality:score",
      indexKind: "equality",
      columnName: "score",
      generation: 1,
      columnEpoch: 1,
    }, storage.describeChunks());

    const fake = job.tasks.map((task) => executeEqualityChunkRebuild(fullTask(job, task)));
    const real = await executeJob(job);

    expect(real.map(normalizeEquality)).toEqual(fake.map(normalizeEquality));
    expect(real.every((result) => result.keyBuffer instanceof ArrayBuffer)).toBe(true);
  });

  it("matches fake equality executor output for SAB-backed dictionary code chunks", async () => {
    const storage = DictionaryColumnStorage.withSharedBuffer(
      ["active", "passive", "trial"] as const,
      4,
      new Uint8Array([0, 1, 0, 2]),
      4,
      2,
    );
    const job = createEqualityBackgroundJob({
      jobId: "eq:dictionary",
      indexId: "equality:status",
      indexKind: "equality",
      columnName: "status",
      generation: 1,
      columnEpoch: 1,
    }, storage.describeChunks());

    const fake = job.tasks.map((task) => executeEqualityChunkRebuild(fullTask(job, task)));
    const real = await executeJob(job);

    expect(real.map(normalizeEquality)).toEqual(fake.map(normalizeEquality));
  });

  it("matches fake sorted executor output for SAB-backed numeric chunks", async () => {
    const storage = NumericColumnStorage.withSharedBuffer(
      "uint32",
      5,
      new Uint32Array([30, 10, 20, 10, 40]),
      5,
      2,
    );
    const job = createSortedBackgroundJob({
      jobId: "sorted:score",
      indexId: "sorted:score",
      indexKind: "sorted",
      columnName: "score",
      generation: 1,
      columnEpoch: 1,
      rowCount: 5,
    }, storage.describeChunks());

    const fake = job.tasks.map((task) => executeSortedChunkRebuild(fullTask(job, task)));
    const real = await executeJob(job);

    expect(real.map(normalizeSorted)).toEqual(fake.map(normalizeSorted));
    expect(real.every((result) => result.rowIdsBuffer instanceof ArrayBuffer)).toBe(true);
  });

  it("rejects non-SAB chunk input before posting to a worker", async () => {
    const executor = await createExecutor(1);
    const storage = new NumericColumnStorage(
      "uint32",
      2,
      new Uint32Array([1, 2]),
      2,
      2,
    );
    const job = createEqualityBackgroundJob({
      jobId: "eq:array-buffer",
      indexId: "equality:score",
      indexKind: "equality",
      columnName: "score",
      generation: 1,
      columnEpoch: 1,
    }, storage.describeChunks());

    expect(() => executor.execute(fullTask(job, job.tasks[0]))).toThrow(/SharedArrayBuffer-backed/);
  });

  it("reports worker task failures and rejects work after dispose", async () => {
    const executor = await createExecutor(1);
    const storage = NumericColumnStorage.withSharedBuffer(
      "uint32",
      1,
      new Uint32Array([1]),
      1,
      1,
    );
    const job = createEqualityBackgroundJob({
      jobId: "bad-kind",
      indexId: "unique:score",
      indexKind: "equality",
      columnName: "score",
      generation: 1,
      columnEpoch: 1,
    }, storage.describeChunks());
    const task = {
      ...fullTask(job, job.tasks[0]),
      indexKind: "unique",
    } as unknown as BackgroundChunkTask<BackgroundWorkerTaskPayload>;

    await expect(executor.execute(task)).rejects.toMatchObject({
      code: "COLQL_UNSUPPORTED_OPERATION",
    });

    await executor.dispose();
    expect(() => executor.ping()).toThrow(/disposed/);
  });
});

async function createExecutor(workerCount: number) {
  const module = await import("../dist/indexing/background/node-worker-executor.mjs") as NodeWorkerExecutorModule;
  const executor = new module.NodeBackgroundWorkerExecutor({
    workerCount,
    workerUrl: workerEntryUrl,
  });
  executors.push(executor);
  return executor;
}

function fullTask<TPayload>(
  job: BackgroundChunkJob<TPayload>,
  task: BackgroundChunkJob<TPayload>["tasks"][number],
): BackgroundChunkTask<TPayload> {
  return {
    ...task,
    jobId: job.jobId,
    indexId: job.indexId,
    indexKind: job.indexKind,
    columnName: job.columnName,
    generation: job.generation,
    columnEpoch: job.columnEpoch,
  };
}

async function executeJob<TPayload extends BackgroundWorkerTaskPayload, TResult extends BackgroundWorkerTaskResult>(
  job: BackgroundChunkJob<TPayload>,
): Promise<TResult[]> {
  const executor = await createExecutor(2);
  const results: TResult[] = [];
  let resolveDone!: () => void;
  const done = new Promise<void>((resolveDonePromise) => {
    resolveDone = resolveDonePromise;
  });
  const pool = new BackgroundWorkerPool<TPayload, TResult>({
    workerCount: 2,
    executor: executor.execute as (task: BackgroundChunkTask<TPayload>, workerId: number) => Promise<TResult>,
    onTaskComplete: (result) => results.push(result.result),
    onJobCompleted: () => resolveDone(),
  });

  pool.submitJob(job);
  await done;
  return results.sort((left, right) => left.chunkIndex - right.chunkIndex);
}

function normalizeEquality(result: EqualityEncodedChunkResult) {
  return {
    columnName: result.columnName,
    chunkIndex: result.chunkIndex,
    rowStart: result.rowStart,
    keyArrayName: result.keyArrayName,
    keyCount: result.keyCount,
    rowIdCount: result.rowIdCount,
    keys: Array.from(new (arrayConstructor(result.keyArrayName))(result.keyBuffer)),
    offsets: Array.from(new Uint32Array(result.offsetsBuffer)),
    rowIds: Array.from(new Uint32Array(result.rowIdsBuffer)),
  };
}

function normalizeSorted(result: SortedEncodedChunkResult) {
  return {
    columnName: result.columnName,
    chunkIndex: result.chunkIndex,
    rowStart: result.rowStart,
    rowCount: result.rowCount,
    valueArrayName: result.valueArrayName,
    values: Array.from(new (arrayConstructor(result.valueArrayName))(result.valuesBuffer)),
    rowIds: Array.from(new Uint32Array(result.rowIdsBuffer)),
    minValue: result.minValue,
    maxValue: result.maxValue,
  };
}

function arrayConstructor(name: string) {
  switch (name) {
    case "Int16Array":
      return Int16Array;
    case "Int32Array":
      return Int32Array;
    case "Uint8Array":
      return Uint8Array;
    case "Uint16Array":
      return Uint16Array;
    case "Uint32Array":
      return Uint32Array;
    case "Float32Array":
      return Float32Array;
    case "Float64Array":
      return Float64Array;
    default:
      throw new Error(`Unknown typed array: ${name}`);
  }
}

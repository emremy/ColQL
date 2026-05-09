import { describe, expect, it } from "vitest";
import { column, table } from "../src";
import {
  createSortedBackgroundJob,
  executeSortedChunkRebuild,
  sortedBackgroundRebuildEligibility,
  type SortedBackgroundRebuildJobMetadata,
  type SortedBackgroundRebuildTaskPayload,
  type SortedEncodedChunkResult,
} from "../src/indexing/background/sorted-rebuild";
import { BackgroundWorkerPool } from "../src/indexing/background/worker-pool";
import type { BackgroundChunkJob } from "../src/indexing/background/types";
import { IndexManager } from "../src/indexing/index-manager";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";
import { Table } from "../src/table";

const schema = {
  id: column.uint32(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive"] as const),
};

function indexManager(instance: unknown): IndexManager {
  return (instance as { indexManager: IndexManager }).indexManager;
}

function storages(instance: unknown) {
  return (instance as {
    storages: {
      id: NumericColumnStorage;
      score: NumericColumnStorage;
      status: DictionaryColumnStorage<readonly ["active", "passive"]>;
    };
  }).storages;
}

function sortedRows(instance: unknown, columnName: string): number[] {
  const manager = indexManager(instance) as unknown as {
    sortedIndexesByColumn: Map<string, { rowIdsSortedByValue: Uint32Array }>;
  };
  return [...(manager.sortedIndexesByColumn.get(columnName)?.rowIdsSortedByValue ?? [])];
}

function metadataFor(
  manager: IndexManager,
  columnName: string,
  rowCount: number,
  jobId = `job:${columnName}`,
): SortedBackgroundRebuildJobMetadata {
  const snapshot = manager.lifecycleSnapshot("sorted", columnName);
  if (snapshot === undefined) {
    throw new Error(`Missing lifecycle for ${columnName}`);
  }

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

function createItems() {
  return new Table(schema, 2, {
    storages: {
      id: NumericColumnStorage.withSharedBuffer("uint32", 2, undefined, 0, 2),
      score: NumericColumnStorage.withSharedBuffer("uint32", 2, undefined, 0, 2),
      status: DictionaryColumnStorage.withSharedBuffer(
        ["active", "passive"] as const,
        2,
        undefined,
        0,
        2,
      ),
    },
  }).insertMany([
    { id: 1, score: 30, status: "active" },
    { id: 2, score: 10, status: "passive" },
    { id: 3, score: 20, status: "active" },
    { id: 4, score: 10, status: "passive" },
    { id: 5, score: 30, status: "active" },
  ]);
}

function createSyncItems() {
  return table(schema).insertMany([
    { id: 1, score: 30, status: "active" },
    { id: 2, score: 10, status: "passive" },
    { id: 3, score: 20, status: "active" },
    { id: 4, score: 10, status: "passive" },
    { id: 5, score: 30, status: "active" },
  ]);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function runBackgroundJob(
  manager: IndexManager,
  job: BackgroundChunkJob<SortedBackgroundRebuildTaskPayload> & { rowCount: number },
): Promise<SortedEncodedChunkResult[]> {
  const results: SortedEncodedChunkResult[] = [];
  const pool = new BackgroundWorkerPool({
    workerCount: 2,
    executor: executeSortedChunkRebuild,
    onJobQueued: () => {
      manager.startSortedBackgroundRebuild(job, "update:indexed-column");
    },
    onJobStarted: () => {
      manager.markSortedBackgroundRebuildStarted(job, "update:indexed-column");
    },
    onTaskComplete: (result) => {
      results.push(result.result);
    },
    onJobCompleted: () => {
      manager.completeSortedBackgroundRebuild(job, results);
    },
    onJobFailed: (_snapshot, error) => {
      manager.failSortedBackgroundRebuild(
        job,
        error instanceof Error ? error.message : String(error),
      );
    },
  });

  pool.submitJob(job);
  await flushMicrotasks();
  return results;
}

describe("sorted background rebuild", () => {
  it("accepts SAB-backed numeric descriptors and rejects ArrayBuffer-backed numeric descriptors", () => {
    const shared = NumericColumnStorage.withSharedBuffer("uint32", 1, undefined, 0, 2);
    shared.append(2);
    shared.append(1);
    const arrayBacked = new NumericColumnStorage("uint32", 1, undefined, 0, 2);
    arrayBacked.append(1);
    const dictionary = DictionaryColumnStorage.withSharedBuffer(["a", "b"] as const, 1, undefined, 0, 2);
    dictionary.append("a");

    expect(sortedBackgroundRebuildEligibility(shared.describeChunks())).toEqual(expect.objectContaining({
      eligible: true,
      reason: "shared-chunks",
      chunkCount: 1,
    }));
    expect(sortedBackgroundRebuildEligibility(arrayBacked.describeChunks())).toEqual(expect.objectContaining({
      eligible: false,
      reason: "non-shared-buffer",
    }));
    expect(sortedBackgroundRebuildEligibility(dictionary.describeChunks())).toEqual(expect.objectContaining({
      eligible: false,
      reason: "unsupported-column-kind",
    }));
    expect(sortedBackgroundRebuildEligibility(shared.describeChunks(), 1)).toEqual(expect.objectContaining({
      eligible: false,
      reason: "memory-budget",
    }));
  });

  it("encodes chunk-local sorted output as typed buffers", () => {
    const storage = NumericColumnStorage.withSharedBuffer("uint32", 1, undefined, 0, 4);
    for (const value of [30, 10, 30, 20]) {
      storage.append(value);
    }
    const manager = new IndexManager();
    manager.createSorted("score", column.uint32(), 4, (row) => storage.get(row));
    manager.markSortedColumnsDirty(["score"]);
    const metadata = metadataFor(manager, "score", 4);
    const job = createSortedBackgroundJob(metadata, storage.describeChunks());

    const result = executeSortedChunkRebuild({
      ...job.tasks[0],
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    });

    expect(result).toEqual(expect.objectContaining({
      valueArrayName: "Uint32Array",
      rowCount: 4,
      valuesBuffer: expect.any(ArrayBuffer),
      rowIdsBuffer: expect.any(ArrayBuffer),
      minValue: 10,
      maxValue: 30,
    }));
    expect([...new Uint32Array(result.valuesBuffer)]).toEqual([10, 20, 30, 30]);
    expect([...new Uint32Array(result.rowIdsBuffer)]).toEqual([1, 3, 0, 2]);
    expect(Object.values(result).some((value) => Array.isArray(value))).toBe(false);
  });

  it("k-way merges chunk-local output with deterministic tie-breaking across chunks", async () => {
    const items = createItems().createSortedIndex("score");
    items.update(2, { score: 10 });
    const manager = indexManager(items);
    const metadata = metadataFor(manager, "score", items.rowCount);
    const descriptor = storages(items).score.describeChunks();
    const job = createSortedBackgroundJob(metadata, descriptor);

    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    const results = job.tasks.map((task) => executeSortedChunkRebuild({
      ...task,
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));
    expect(manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);
    expect(manager.completeSortedBackgroundRebuild(metadata, results)).toBe("applied");

    expect(sortedRows(items, "score")).toEqual([1, 2, 3, 0, 4]);
  });

  it("matches synchronous sorted index range behavior after valid apply", async () => {
    const items = createItems().createSortedIndex("score");
    const syncItems = createSyncItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    syncItems.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount);
    const job = createSortedBackgroundJob(metadata, descriptor);

    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    await runBackgroundJob(manager, job);

    expect(items.where("score", ">=", 15).toArray()).toEqual(syncItems.where("score", ">=", 15).toArray());
    expect(items.where("score", "<=", 20).toArray()).toEqual(syncItems.where("score", "<=", 20).toArray());
    expect(indexManager(items).lifecycleSnapshot("sorted", "score")).toMatchObject({ state: "fresh" });
  });

  it("keeps queries on fallback scan while sorted rebuild is active", async () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount);
    const job = createSortedBackgroundJob(metadata, descriptor);
    const gate = deferred<void>();
    const results: SortedEncodedChunkResult[] = [];
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => {
        const result = executeSortedChunkRebuild(task);
        return gate.promise.then(() => result);
      },
      onJobQueued: () => manager.startSortedBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onTaskComplete: (result) => results.push(result.result),
      onJobCompleted: () => {
        manager.completeSortedBackgroundRebuild(metadata, results);
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();

    items.resetScanCounter();
    expect(items.where("score", ">=", 15).toArray()).toEqual([
      { id: 1, score: 15, status: "active" },
      { id: 3, score: 20, status: "active" },
      { id: 5, score: 30, status: "active" },
    ]);
    expect(items.scannedRowCount).toBe(items.rowCount);

    gate.resolve();
    await flushMicrotasks();
  });

  it("can use another fresh index while sorted rebuild is active", () => {
    const items = createItems().createSortedIndex("score").createIndex("id");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const metadata = metadataFor(manager, "score", items.rowCount);
    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);

    items.resetScanCounter();
    expect(items.where("score", ">=", 15).where("id", "=", 3).toArray()).toEqual([
      { id: 3, score: 20, status: "active" },
    ]);
    expect(items.scannedRowCount).toBe(1);
  });

  it("discards stale sorted rebuild results after mutation while queued", () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount);
    const job = createSortedBackgroundJob(metadata, descriptor);
    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);

    items.update(1, { score: 25 });
    const results = job.tasks.map((task) => executeSortedChunkRebuild({
      ...task,
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));

    expect(manager.completeSortedBackgroundRebuild(metadata, results)).toBe("stale");
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("discards stale sorted rebuild results after mutation while active", async () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount);
    const job = createSortedBackgroundJob(metadata, descriptor);
    const gate = deferred<void>();
    const results: SortedEncodedChunkResult[] = [];
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => {
        const result = executeSortedChunkRebuild(task);
        return gate.promise.then(() => result);
      },
      onJobQueued: () => manager.startSortedBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onTaskComplete: (result) => results.push(result.result),
      onJobCompleted: () => {
        manager.completeSortedBackgroundRebuild(metadata, results);
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();
    items.update(1, { score: 25 });

    gate.resolve();
    await flushMicrotasks();

    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("discards stale sorted rebuild results after delete while active", async () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount);
    const job = createSortedBackgroundJob(metadata, descriptor);
    const gate = deferred<void>();
    const results: SortedEncodedChunkResult[] = [];
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => {
        const result = executeSortedChunkRebuild(task);
        return gate.promise.then(() => result);
      },
      onJobQueued: () => manager.startSortedBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onTaskComplete: (result) => results.push(result.result),
      onJobCompleted: () => {
        manager.completeSortedBackgroundRebuild(metadata, results);
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();
    items.delete(1);

    gate.resolve();
    await flushMicrotasks();

    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("rejects sorted rebuild completion for a mismatched job id", () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount, "job:accepted");
    const staleMetadata = { ...metadata, jobId: "job:stale" };
    const job = createSortedBackgroundJob(metadata, descriptor);
    const results = job.tasks.map((task) => executeSortedChunkRebuild({
      ...task,
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));

    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(staleMetadata, "update:indexed-column")).toBe(false);
    expect(manager.completeSortedBackgroundRebuild(staleMetadata, results)).toBe("stale");
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "queued",
      generation: metadata.generation,
      columnEpoch: metadata.columnEpoch,
    });
  });

  it("marks sorted rebuild failed when execution or output validation fails", async () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const descriptor = storages(items).score.describeChunks();
    const metadata = metadataFor(manager, "score", items.rowCount);
    const job = createSortedBackgroundJob(metadata, descriptor);
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: () => {
        throw new Error("mock sorted failure");
      },
      onJobQueued: () => manager.startSortedBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onJobFailed: (_snapshot, error) => {
        manager.failSortedBackgroundRebuild(
          metadata,
          error instanceof Error ? error.message : String(error),
        );
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });

    items.update(2, { score: 22 });
    const nextMetadata = metadataFor(manager, "score", items.rowCount);
    expect(manager.startSortedBackgroundRebuild(nextMetadata, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(nextMetadata, "update:indexed-column")).toBe(true);
    expect(() => manager.completeSortedBackgroundRebuild(nextMetadata, [{
      columnName: "score",
      chunkIndex: 0,
      rowStart: 0,
      rowCount: 1,
      valueArrayName: "Uint32Array",
      valuesBuffer: new ArrayBuffer(0),
      rowIdsBuffer: new Uint32Array([0]).buffer,
      byteLength: 4,
    }])).toThrow(/Invalid sorted background rebuild output/);
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });
  });

  it("rejects malformed sorted output with duplicate or out-of-range row ids", () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const metadata = metadataFor(manager, "score", items.rowCount);
    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);

    expect(() => manager.completeSortedBackgroundRebuild(metadata, [{
      columnName: "score",
      chunkIndex: 0,
      rowStart: 0,
      rowCount: items.rowCount,
      valueArrayName: "Uint32Array",
      valuesBuffer: new Uint32Array([10, 15, 20, 30, 40]).buffer,
      rowIdsBuffer: new Uint32Array([0, 1, 1, 3, 99]).buffer,
      byteLength: items.rowCount * 8,
    }])).toThrow(/Invalid sorted background rebuild output/);

    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });
  });

  it("does not serialize sorted background job state", () => {
    const items = createItems().createSortedIndex("score");
    items.update(0, { score: 15 });
    const manager = indexManager(items);
    const metadata = metadataFor(manager, "score", items.rowCount);
    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);

    const restored = table.deserialize(items.serialize());

    expect(restored.sortedIndexes()).toEqual([]);
    expect(restored.toArray()).toEqual(items.toArray());
  });

  it("keeps query APIs synchronous and does not introduce worker threads", () => {
    const items = createItems().createSortedIndex("score");
    const result = items.where("score", ">=", 20).toArray();

    expect(result).toEqual([
      { id: 1, score: 30, status: "active" },
      { id: 3, score: 20, status: "active" },
      { id: 5, score: 30, status: "active" },
    ]);
    expect(result).not.toBeInstanceOf(Promise);
  });
});

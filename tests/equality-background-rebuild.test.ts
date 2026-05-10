import { describe, expect, it } from "vitest";
import { column, table } from "../src";
import {
  createEqualityBackgroundJob,
  equalityBackgroundRebuildEligibility,
  executeEqualityChunkRebuild,
  type EqualityBackgroundRebuildJobMetadata,
  type EqualityBackgroundRebuildTaskPayload,
  type EqualityEncodedChunkResult,
} from "../src/indexing/background/equality-rebuild";
import { BackgroundWorkerPool } from "../src/indexing/background/worker-pool";
import type { BackgroundChunkJob } from "../src/indexing/background/types";
import { IndexManager } from "../src/indexing/index-manager";
import type {
  DictionaryCodeColumnChunkDescriptorSet,
  NumericColumnChunkDescriptorSet,
} from "../src/storage/chunk-descriptor";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";
import { Table } from "../src/table";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive", "trial"] as const),
};

function indexManager(instance: unknown): IndexManager {
  return (instance as { indexManager: IndexManager }).indexManager;
}

function storages(instance: unknown) {
  return (instance as {
    storages: {
      id: NumericColumnStorage;
      age: NumericColumnStorage;
      status: DictionaryColumnStorage<readonly ["active", "passive", "trial"]>;
    };
  }).storages;
}

function metadataFor(
  manager: IndexManager,
  columnName: string,
  jobId = `job:${columnName}`,
): EqualityBackgroundRebuildJobMetadata {
  const snapshot = manager.lifecycleSnapshot("equality", columnName);
  if (snapshot === undefined) {
    throw new Error(`Missing lifecycle for ${columnName}`);
  }

  return {
    jobId,
    indexId: `equality:${columnName}`,
    indexKind: "equality",
    columnName,
    generation: snapshot.generation,
    columnEpoch: snapshot.columnEpoch,
  };
}

function createUsers() {
  return new Table(schema, 2, {
    storages: {
      id: NumericColumnStorage.withSharedBuffer("uint32", 2, undefined, 0, 2),
      age: NumericColumnStorage.withSharedBuffer("uint8", 2, undefined, 0, 2),
      status: DictionaryColumnStorage.withSharedBuffer(
        ["active", "passive", "trial"] as const,
        2,
        undefined,
        0,
        2,
      ),
    },
  }).insertMany([
    { id: 1, age: 20, status: "active" },
    { id: 2, age: 30, status: "passive" },
    { id: 3, age: 40, status: "active" },
    { id: 4, age: 50, status: "trial" },
    { id: 5, age: 60, status: "passive" },
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
  job: BackgroundChunkJob<EqualityBackgroundRebuildTaskPayload>,
): Promise<EqualityEncodedChunkResult[]> {
  const results: EqualityEncodedChunkResult[] = [];
  const pool = new BackgroundWorkerPool({
    workerCount: 2,
    executor: executeEqualityChunkRebuild,
    onJobQueued: () => {
      manager.startEqualityBackgroundRebuild(job, "update:indexed-column");
    },
    onJobStarted: () => {
      manager.markEqualityBackgroundRebuildStarted(job, "update:indexed-column");
    },
    onTaskComplete: (result) => {
      results.push(result.result);
    },
    onJobCompleted: () => {
      manager.completeEqualityBackgroundRebuild(job, results);
    },
    onJobFailed: (_snapshot, error) => {
      manager.failEqualityBackgroundRebuild(
        job,
        error instanceof Error ? error.message : String(error),
      );
    },
  });

  pool.submitJob(job);
  await flushMicrotasks();
  return results;
}

describe("equality background rebuild", () => {
  it("accepts SAB-backed numeric and dictionary descriptors and rejects ArrayBuffer-backed descriptors", () => {
    const numeric = NumericColumnStorage.withSharedBuffer("uint32", 1, undefined, 0, 2);
    numeric.append(1);
    numeric.append(2);
    const dictionary = DictionaryColumnStorage.withSharedBuffer(["a", "b"] as const, 1, undefined, 0, 2);
    dictionary.append("a");
    dictionary.append("b");
    const arrayBacked = new NumericColumnStorage("uint32", 1, undefined, 0, 2);
    arrayBacked.append(1);

    expect(equalityBackgroundRebuildEligibility(numeric.describeChunks())).toEqual(expect.objectContaining({
      eligible: true,
      reason: "shared-chunks",
      chunkCount: 1,
    }));
    expect(equalityBackgroundRebuildEligibility(dictionary.describeChunks())).toEqual(expect.objectContaining({
      eligible: true,
      reason: "shared-chunks",
      chunkCount: 1,
    }));
    expect(equalityBackgroundRebuildEligibility(arrayBacked.describeChunks())).toEqual(expect.objectContaining({
      eligible: false,
      reason: "non-shared-buffer",
    }));
    expect(equalityBackgroundRebuildEligibility(numeric.describeChunks(), 1)).toEqual(expect.objectContaining({
      eligible: false,
      reason: "memory-budget",
    }));
  });

  it("encodes chunk output as typed buffers without returning Map state", () => {
    const storage = NumericColumnStorage.withSharedBuffer("uint32", 1, undefined, 0, 4);
    for (const value of [2, 1, 2, 3]) {
      storage.append(value);
    }
    const descriptor = storage.describeChunks();
    const manager = new IndexManager();
    manager.create("id", column.uint32(), 4, (row) => storage.get(row));
    manager.markEqualityColumnsDirty(["id"]);
    const metadata = metadataFor(manager, "id");
    const job = createEqualityBackgroundJob(metadata, descriptor);

    const result = executeEqualityChunkRebuild({
      ...job.tasks[0],
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    });

    expect(result).toEqual(expect.objectContaining({
      keyArrayName: "Uint32Array",
      keyCount: 3,
      rowIdCount: 4,
      keyBuffer: expect.any(ArrayBuffer),
      offsetsBuffer: expect.any(ArrayBuffer),
      rowIdsBuffer: expect.any(ArrayBuffer),
    }));
    expect([...new Uint32Array(result.keyBuffer)]).toEqual([1, 2, 3]);
    expect([...new Uint32Array(result.offsetsBuffer)]).toEqual([0, 1, 3, 4]);
    expect([...new Uint32Array(result.rowIdsBuffer)]).toEqual([1, 0, 2, 3]);
    expect(Object.values(result).some((value) => value instanceof Map)).toBe(false);
  });

  it("rebuilds numeric equality indexes from encoded multi-chunk output", async () => {
    const users = createUsers().createIndex("id");
    users.update(2, { id: 2 });
    const manager = indexManager(users);
    const descriptor = storages(users).id.describeChunks();
    const metadata = metadataFor(manager, "id");
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    const job = createEqualityBackgroundJob(metadata, descriptor);

    const results: EqualityEncodedChunkResult[] = [];
    for (const task of job.tasks) {
      results.push(executeEqualityChunkRebuild({
        ...task,
        jobId: job.jobId,
        indexId: job.indexId,
        indexKind: job.indexKind,
        columnName: job.columnName,
        generation: job.generation,
        columnEpoch: job.columnEpoch,
      }));
    }

    manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column");
    expect(manager.completeEqualityBackgroundRebuild(metadata, results)).toBe("applied");
    expect(manager.lifecycleSnapshot("equality", "id")).toMatchObject({ state: "fresh" });
    expect(users.where("id", "=", 2).toArray()).toEqual([
      { id: 2, age: 30, status: "passive" },
      { id: 2, age: 40, status: "active" },
    ]);
  });

  it("rebuilds dictionary-code equality indexes without descriptor string values", async () => {
    const users = createUsers().createIndex("status");
    users.update(3, { status: "active" });
    const manager = indexManager(users);
    const descriptor = storages(users).status.describeChunks();
    const metadata = metadataFor(manager, "status");
    const job = createEqualityBackgroundJob(metadata, descriptor);

    expect(Object.keys(descriptor)).not.toContain("values");
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    await runBackgroundJob(manager, job);

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({ state: "fresh" });
    expect(users.where("status", "=", "active").toArray()).toEqual([
      { id: 1, age: 20, status: "active" },
      { id: 3, age: 40, status: "active" },
      { id: 4, age: 50, status: "active" },
    ]);
  });

  it("keeps queries on fallback scan while equality rebuild is active", async () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const descriptor = storages(users).status.describeChunks();
    const metadata = metadataFor(manager, "status");
    const job = createEqualityBackgroundJob(metadata, descriptor);
    const gate = deferred<EqualityEncodedChunkResult>();
    const results: EqualityEncodedChunkResult[] = [];
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => {
        const result = executeEqualityChunkRebuild(task);
        return gate.promise.then(() => result);
      },
      onJobQueued: () => manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onTaskComplete: (result) => {
        results.push(result.result);
      },
      onJobCompleted: () => {
        manager.completeEqualityBackgroundRebuild(metadata, results);
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();

    users.resetScanCounter();
    expect(users.where("status", "=", "trial").toArray()).toEqual([
      { id: 1, age: 20, status: "trial" },
      { id: 4, age: 50, status: "trial" },
    ]);
    expect(users.scannedRowCount).toBe(users.rowCount);

    gate.resolve(executeEqualityChunkRebuild({
      ...job.tasks[0],
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));
    await flushMicrotasks();
  });

  it("can use another fresh index while equality rebuild is active", async () => {
    const users = createUsers().createIndex("status").createIndex("id");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const metadata = metadataFor(manager, "status");
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);

    users.resetScanCounter();
    expect(users.where("status", "=", "trial").where("id", "=", 4).toArray()).toEqual([
      { id: 4, age: 50, status: "trial" },
    ]);
    expect(users.scannedRowCount).toBe(1);
  });

  it("discards stale equality rebuild results after mutation while queued", () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const descriptor = storages(users).status.describeChunks();
    const metadata = metadataFor(manager, "status");
    const job = createEqualityBackgroundJob(metadata, descriptor);
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);

    users.update(1, { status: "trial" });
    const results = job.tasks.map((task) => executeEqualityChunkRebuild({
      ...task,
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));

    expect(manager.completeEqualityBackgroundRebuild(metadata, results)).toBe("stale");
    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("rejects equality rebuild completion for a mismatched job id", () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const descriptor = storages(users).status.describeChunks();
    const metadata = metadataFor(manager, "status", "job:accepted");
    const staleMetadata = { ...metadata, jobId: "job:stale" };
    const job = createEqualityBackgroundJob(metadata, descriptor);
    const results = job.tasks.map((task) => executeEqualityChunkRebuild({
      ...task,
      jobId: job.jobId,
      indexId: job.indexId,
      indexKind: job.indexKind,
      columnName: job.columnName,
      generation: job.generation,
      columnEpoch: job.columnEpoch,
    }));

    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(staleMetadata, "update:indexed-column")).toBe(false);
    expect(manager.completeEqualityBackgroundRebuild(staleMetadata, results)).toBe("stale");
    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "queued",
      generation: metadata.generation,
      columnEpoch: metadata.columnEpoch,
    });
  });

  it("discards stale equality rebuild results after mutation while active", async () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const descriptor = storages(users).status.describeChunks();
    const metadata = metadataFor(manager, "status");
    const job = createEqualityBackgroundJob(metadata, descriptor);
    const results: EqualityEncodedChunkResult[] = [];
    const gate = deferred<void>();
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => {
        const result = executeEqualityChunkRebuild(task);
        return gate.promise.then(() => result);
      },
      onJobQueued: () => manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onTaskComplete: (result) => results.push(result.result),
      onJobCompleted: () => {
        manager.completeEqualityBackgroundRebuild(metadata, results);
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();
    users.update(1, { status: "trial" });

    gate.resolve();
    await flushMicrotasks();

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("represents failed equality background rebuilds without changing normal query behavior", async () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const descriptor = storages(users).status.describeChunks();
    const metadata = metadataFor(manager, "status");
    const job = createEqualityBackgroundJob(metadata, descriptor);
    const pool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: () => {
        throw new Error("mock equality failure");
      },
      onJobQueued: () => manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column"),
      onJobStarted: () => manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column"),
      onJobFailed: (_snapshot, error) => {
        manager.failEqualityBackgroundRebuild(
          metadata,
          error instanceof Error ? error.message : String(error),
        );
      },
    });

    pool.submitJob(job);
    await flushMicrotasks();

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });
    expect(users.where("status", "=", "trial").toArray()).toEqual([
      { id: 1, age: 20, status: "trial" },
      { id: 4, age: 50, status: "trial" },
    ]);
  });

  it("marks equality rebuild failed when encoded output validation fails", () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const metadata = metadataFor(manager, "status");
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);

    expect(() => manager.completeEqualityBackgroundRebuild(metadata, [{
      columnName: "status",
      chunkIndex: 0,
      rowStart: 0,
      keyArrayName: "Uint8Array",
      keyCount: 1,
      rowIdCount: 1,
      keyBuffer: new ArrayBuffer(0),
      offsetsBuffer: new Uint32Array([0, 1]).buffer,
      rowIdsBuffer: new Uint32Array([0]).buffer,
      byteLength: 12,
    }])).toThrow(/Invalid equality background rebuild output/);

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });
    expect(users.where("status", "=", "trial").toArray()).toEqual([
      { id: 1, age: 20, status: "trial" },
      { id: 4, age: 50, status: "trial" },
    ]);
  });

  it("does not serialize equality background job state", () => {
    const users = createUsers().createIndex("status");
    users.update(0, { status: "trial" });
    const manager = indexManager(users);
    const metadata = metadataFor(manager, "status");
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);

    const restored = table.deserialize(users.serialize());

    expect(restored.indexes()).toEqual([]);
    expect(restored.toArray()).toEqual(users.toArray());
  });

  it("does not introduce sorted background rebuild behavior", () => {
    const users = createUsers().createSortedIndex("age");
    users.update(0, { age: 21 });

    expect(indexManager(users).lifecycleSnapshot("sorted", "age")).toMatchObject({
      state: "dirty",
    });
    expect(users.where("age", ">=", 21).explain()).toEqual(expect.objectContaining({
      indexState: "dirty",
      fallbackReason: "dirty-index",
    }));
  });
});

import { describe, expect, it } from "vitest";
import { column, type QueryInfo } from "../src";
import {
  createEqualityBackgroundJob,
  executeEqualityChunkRebuild,
  type EqualityBackgroundRebuildJobMetadata,
  type EqualityEncodedChunkResult,
} from "../src/indexing/background/equality-rebuild";
import {
  createSortedBackgroundJob,
  executeSortedChunkRebuild,
  type SortedBackgroundRebuildJobMetadata,
  type SortedEncodedChunkResult,
} from "../src/indexing/background/sorted-rebuild";
import { BackgroundWorkerPool } from "../src/indexing/background/worker-pool";
import type { IndexManager } from "../src/indexing/index-manager";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";
import { Table } from "../src/table";

const schema = {
  id: column.uint32(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive", "trial"] as const),
};

function createRows(onQuery?: (info: QueryInfo) => void) {
  return new Table(schema, 2, {
    onQuery,
    storages: {
      id: NumericColumnStorage.withSharedBuffer("uint32", 2, undefined, 0, 2),
      score: NumericColumnStorage.withSharedBuffer("uint32", 2, undefined, 0, 2),
      status: DictionaryColumnStorage.withSharedBuffer(
        ["active", "passive", "trial"] as const,
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
    { id: 4, score: 10, status: "trial" },
  ]);
}

function indexManager(instance: unknown): IndexManager {
  return (instance as { indexManager: IndexManager }).indexManager;
}

function storages(instance: unknown) {
  return (instance as {
    storages: {
      score: NumericColumnStorage;
      status: DictionaryColumnStorage<readonly ["active", "passive", "trial"]>;
    };
  }).storages;
}

function equalityMetadata(
  manager: IndexManager,
  columnName: string,
  jobId = `eq:${columnName}`,
): EqualityBackgroundRebuildJobMetadata {
  const snapshot = manager.lifecycleSnapshot("equality", columnName);
  if (snapshot === undefined) throw new Error(`Missing equality ${columnName}`);
  return {
    jobId,
    indexId: `equality:${columnName}`,
    indexKind: "equality",
    columnName,
    generation: snapshot.generation,
    columnEpoch: snapshot.columnEpoch,
  };
}

function sortedMetadata(
  manager: IndexManager,
  columnName: string,
  rowCount: number,
  jobId = `sort:${columnName}`,
): SortedBackgroundRebuildJobMetadata {
  const snapshot = manager.lifecycleSnapshot("sorted", columnName);
  if (snapshot === undefined) throw new Error(`Missing sorted ${columnName}`);
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

function equalityResults(
  metadata: EqualityBackgroundRebuildJobMetadata,
  storage: DictionaryColumnStorage<readonly ["active", "passive", "trial"]>,
): EqualityEncodedChunkResult[] {
  const job = createEqualityBackgroundJob(metadata, storage.describeChunks());
  return job.tasks.map((task) => executeEqualityChunkRebuild({
    ...task,
    jobId: job.jobId,
    indexId: job.indexId,
    indexKind: job.indexKind,
    columnName: job.columnName,
    generation: job.generation,
    columnEpoch: job.columnEpoch,
  }));
}

function sortedResults(
  metadata: SortedBackgroundRebuildJobMetadata,
  storage: NumericColumnStorage,
): SortedEncodedChunkResult[] {
  const job = createSortedBackgroundJob(metadata, storage.describeChunks());
  return job.tasks.map((task) => executeSortedChunkRebuild({
    ...task,
    jobId: job.jobId,
    indexId: job.indexId,
    indexKind: job.indexKind,
    columnName: job.columnName,
    generation: job.generation,
    columnEpoch: job.columnEpoch,
  }));
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("background indexing diagnostics", () => {
  it("reports fresh and dirty index diagnostics cheaply", () => {
    const rows = createRows().createIndex("status").createSortedIndex("score");

    expect(rows.__debugIndexDiagnostics()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "equality",
        column: "status",
        state: "fresh",
        generation: 0,
        columnEpoch: 0,
        queued: false,
        rebuilding: false,
        staleResultsDiscarded: 0,
        fallbackCount: 0,
      }),
      expect.objectContaining({
        kind: "sorted",
        column: "score",
        state: "fresh",
      }),
    ]));

    rows.update(0, { status: "trial", score: 31 });

    expect(rows.__debugIndexDiagnostics()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "equality",
        column: "status",
        state: "dirty",
        dirtyReason: "update:indexed-column",
        generation: 1,
        columnEpoch: 1,
      }),
      expect.objectContaining({
        kind: "sorted",
        column: "score",
        state: "dirty",
        dirtyReason: "update:indexed-column",
        generation: 1,
        columnEpoch: 1,
      }),
    ]));
  });

  it("reports queued/rebuilding equality and sorted background jobs without per-chunk detail", async () => {
    const rows = createRows().createIndex("status").createSortedIndex("score");
    rows.update(0, { status: "trial", score: 31 });
    const manager = indexManager(rows);
    const equality = equalityMetadata(manager, "status");
    const sorted = sortedMetadata(manager, "score", rows.rowCount);
    const gate = deferred<void>();
    const eqJob = createEqualityBackgroundJob(equality, storages(rows).status.describeChunks());
    const sortJob = createSortedBackgroundJob(sorted, storages(rows).score.describeChunks());
    const eqPool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => gate.promise.then(() => executeEqualityChunkRebuild(task)),
      onJobQueued: () => manager.startEqualityBackgroundRebuild(equality, "update:indexed-column", {
        chunksTotal: eqJob.tasks.length,
        workerCount: 1,
      }),
      onJobStarted: () => manager.markEqualityBackgroundRebuildStarted(equality, "update:indexed-column"),
    });
    const sortPool = new BackgroundWorkerPool({
      workerCount: 1,
      executor: (task) => gate.promise.then(() => executeSortedChunkRebuild(task)),
      onJobQueued: () => manager.startSortedBackgroundRebuild(sorted, "update:indexed-column", {
        chunksTotal: sortJob.tasks.length,
        workerCount: 1,
      }),
      onJobStarted: () => manager.markSortedBackgroundRebuildStarted(sorted, "update:indexed-column"),
    });

    eqPool.submitJob(eqJob);
    sortPool.submitJob(sortJob);
    await flushMicrotasks();

    const diagnostics = rows.__debugIndexDiagnostics();
    expect(diagnostics).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "equality",
        column: "status",
        state: "rebuilding",
        rebuilding: true,
        jobId: equality.jobId,
        chunksDone: 0,
        chunksTotal: eqJob.tasks.length,
        workerCount: 1,
        lastRebuildMode: "mock-background",
      }),
      expect.objectContaining({
        kind: "sorted",
        column: "score",
        state: "rebuilding",
        rebuilding: true,
        jobId: sorted.jobId,
        chunksDone: 0,
        chunksTotal: sortJob.tasks.length,
      }),
    ]));
    expect(Object.keys(diagnostics[0])).not.toContain("chunks");

    gate.resolve();
    await flushMicrotasks();
  });

  it("reports valid apply, stale discard, and failed background diagnostics", () => {
    const rows = createRows().createIndex("status").createSortedIndex("score");
    rows.update(0, { status: "trial", score: 31 });
    const manager = indexManager(rows);
    const equality = equalityMetadata(manager, "status");
    const sorted = sortedMetadata(manager, "score", rows.rowCount);
    const equalityOutput = equalityResults(equality, storages(rows).status);
    const sortedOutput = sortedResults(sorted, storages(rows).score);

    expect(manager.startEqualityBackgroundRebuild(equality, "update:indexed-column", {
      chunksTotal: equalityOutput.length,
      workerCount: 2,
    })).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(equality, "update:indexed-column")).toBe(true);
    expect(manager.completeEqualityBackgroundRebuild(equality, equalityOutput)).toBe("applied");
    expect(rows.__debugIndexDiagnostics()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "equality",
        column: "status",
        state: "fresh",
        chunksDone: equalityOutput.length,
        chunksTotal: equalityOutput.length,
        lastRebuildMode: "mock-background",
      }),
    ]));

    expect(manager.startSortedBackgroundRebuild(sorted, "update:indexed-column", {
      chunksTotal: sortedOutput.length,
      workerCount: 2,
    })).toBe(true);
    rows.update(1, { score: 25 });
    expect(manager.completeSortedBackgroundRebuild(sorted, sortedOutput)).toBe("stale");
    expect(rows.__debugIndexDiagnostics()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "sorted",
        column: "score",
        state: "dirty",
        staleResultsDiscarded: 1,
        lastDiscardReason: "generation",
      }),
    ]));

    const nextSorted = sortedMetadata(manager, "score", rows.rowCount);
    expect(manager.startSortedBackgroundRebuild(nextSorted, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(nextSorted, "update:indexed-column")).toBe(true);
    expect(() => manager.completeSortedBackgroundRebuild(nextSorted, [{
      columnName: "score",
      chunkIndex: 0,
      rowStart: 0,
      rowCount: 1,
      valueArrayName: "Uint32Array",
      valuesBuffer: new ArrayBuffer(0),
      rowIdsBuffer: new Uint32Array([0]).buffer,
      byteLength: 4,
    }])).toThrow();
    expect(rows.__debugIndexDiagnostics()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "sorted",
        column: "score",
        state: "failed",
        lastErrorCode: "COLQL_BACKGROUND_REBUILD_FAILED",
        lastRebuildMode: "mock-background",
      }),
    ]));
  });

  it("increments fallback diagnostics and reports fallback through explain and onQuery", () => {
    const events: QueryInfo[] = [];
    const rows = createRows((info) => events.push(info)).createIndex("status");
    rows.update(0, { status: "trial" });
    const manager = indexManager(rows);
    const equality = equalityMetadata(manager, "status");
    expect(manager.startEqualityBackgroundRebuild(equality, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(equality, "update:indexed-column")).toBe(true);

    rows.resetScanCounter();
    rows.resetMaterializationCounter();
    const explain = rows.where("status", "=", "trial").explain();

    expect(explain).toEqual(expect.objectContaining({
      scanType: "full",
      indexState: "rebuilding",
      backgroundIndexing: "sync",
      backgroundRebuildState: "rebuilding",
      fallbackReason: "rebuilding-index",
    }));
    expect(rows.scannedRowCount).toBe(0);
    expect(rows.materializedRowCount).toBe(0);

    expect(rows.where("status", "=", "trial").count()).toBe(2);
    expect(events[0]).toEqual(expect.objectContaining({
      indexUsed: false,
      scanType: "full",
      indexState: "rebuilding",
      backgroundRebuildScheduled: false,
      backgroundRebuildState: "rebuilding",
      fallbackReason: "rebuilding-index",
    }));
    expect(rows.__debugIndexDiagnostics()).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "equality",
        column: "status",
        fallbackCount: 1,
      }),
    ]));
  });
});

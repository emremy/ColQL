import { describe, expect, it } from "vitest";
import { column, table } from "../src";
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
import type { IndexManager } from "../src/indexing/index-manager";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";
import { Table } from "../src/table";

const schema = {
  id: column.uint32(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive", "trial"] as const),
};

function createRows() {
  return new Table(schema, 2, {
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
      id: NumericColumnStorage;
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
  descriptor: ReturnType<DictionaryColumnStorage<readonly string[]>["describeChunks"]>,
): EqualityEncodedChunkResult[] {
  return createEqualityBackgroundJob(metadata, descriptor).tasks.map((task) => executeEqualityChunkRebuild({
    ...task,
    jobId: metadata.jobId,
    indexId: metadata.indexId,
    indexKind: metadata.indexKind,
    columnName: metadata.columnName,
    generation: metadata.generation,
    columnEpoch: metadata.columnEpoch,
  }));
}

function sortedResults(
  metadata: SortedBackgroundRebuildJobMetadata,
  descriptor: ReturnType<NumericColumnStorage["describeChunks"]>,
): SortedEncodedChunkResult[] {
  return createSortedBackgroundJob(metadata, descriptor).tasks.map((task) => executeSortedChunkRebuild({
    ...task,
    jobId: metadata.jobId,
    indexId: metadata.indexId,
    indexKind: metadata.indexKind,
    columnName: metadata.columnName,
    generation: metadata.generation,
    columnEpoch: metadata.columnEpoch,
  }));
}

describe("background atomic apply guard", () => {
  it("keeps equality lifecycle non-fresh until the atomic apply point", () => {
    const rows = createRows().createIndex("status");
    rows.update(1, { status: "active" });
    const manager = indexManager(rows);
    const metadata = equalityMetadata(manager, "status");
    const results = equalityResults(metadata, storages(rows).status.describeChunks());

    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);
    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({ state: "rebuilding" });

    rows.resetScanCounter();
    expect(rows.where("status", "=", "active").toArray()).toEqual([
      { id: 1, score: 30, status: "active" },
      { id: 2, score: 10, status: "active" },
      { id: 3, score: 20, status: "active" },
    ]);
    expect(rows.scannedRowCount).toBe(rows.rowCount);

    expect(manager.completeEqualityBackgroundRebuild(metadata, results)).toBe("applied");
    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({ state: "fresh" });
    rows.resetScanCounter();
    expect(rows.where("status", "=", "trial").count()).toBe(1);
    expect(rows.scannedRowCount).toBe(1);
  });

  it("keeps sorted lifecycle non-fresh until the atomic apply point", () => {
    const rows = createRows().createSortedIndex("score");
    rows.update(0, { score: 15 });
    const manager = indexManager(rows);
    const metadata = sortedMetadata(manager, "score", rows.rowCount);
    const results = sortedResults(metadata, storages(rows).score.describeChunks());

    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(metadata, "update:indexed-column")).toBe(true);
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({ state: "rebuilding" });

    rows.resetScanCounter();
    expect(rows.where("score", ">=", 15).toArray()).toEqual([
      { id: 1, score: 15, status: "active" },
      { id: 3, score: 20, status: "active" },
    ]);
    expect(rows.scannedRowCount).toBe(rows.rowCount);

    expect(manager.completeSortedBackgroundRebuild(metadata, results)).toBe("applied");
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({ state: "fresh" });
    rows.resetScanCounter();
    expect(rows.where("score", ">", 20).count()).toBe(0);
    expect(rows.scannedRowCount).toBe(0);
  });

  it("discards equality results with wrong identity or old generation/epoch", () => {
    const rows = createRows().createIndex("status");
    rows.update(1, { status: "active" });
    const manager = indexManager(rows);
    const metadata = equalityMetadata(manager, "status");
    const results = equalityResults(metadata, storages(rows).status.describeChunks());
    expect(manager.startEqualityBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);

    expect(manager.completeEqualityBackgroundRebuild({ ...metadata, indexId: "equality:id" }, results)).toBe("stale");
    expect(manager.completeEqualityBackgroundRebuild({ ...metadata, indexKind: "sorted" } as EqualityBackgroundRebuildJobMetadata, results)).toBe("stale");

    rows.update(2, { status: "trial" });
    expect(manager.completeEqualityBackgroundRebuild(metadata, results)).toBe("stale");
    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("discards sorted results with wrong identity or old generation/epoch", () => {
    const rows = createRows().createSortedIndex("score");
    rows.update(0, { score: 15 });
    const manager = indexManager(rows);
    const metadata = sortedMetadata(manager, "score", rows.rowCount);
    const results = sortedResults(metadata, storages(rows).score.describeChunks());
    expect(manager.startSortedBackgroundRebuild(metadata, "update:indexed-column")).toBe(true);

    expect(manager.completeSortedBackgroundRebuild({ ...metadata, indexId: "sorted:id" }, results)).toBe("stale");
    expect(manager.completeSortedBackgroundRebuild({ ...metadata, indexKind: "equality" } as SortedBackgroundRebuildJobMetadata, results)).toBe("stale");

    rows.delete(1);
    expect(manager.completeSortedBackgroundRebuild(metadata, results)).toBe("stale");
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("fails apply safely when encoded output targets the wrong column", () => {
    const rows = createRows().createIndex("status").createSortedIndex("score");
    rows.update(1, { status: "active", score: 25 });
    const manager = indexManager(rows);
    const equality = equalityMetadata(manager, "status");
    const sorted = sortedMetadata(manager, "score", rows.rowCount);
    const equalityOutput = equalityResults(equality, storages(rows).status.describeChunks());
    const sortedOutput = sortedResults(sorted, storages(rows).score.describeChunks());
    expect(manager.startEqualityBackgroundRebuild(equality, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(equality, "update:indexed-column")).toBe(true);
    expect(manager.startSortedBackgroundRebuild(sorted, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(sorted, "update:indexed-column")).toBe(true);

    expect(() => manager.completeEqualityBackgroundRebuild(equality, [
      { ...equalityOutput[0], columnName: "id" },
    ])).toThrow(/does not match/);
    expect(() => manager.completeSortedBackgroundRebuild(sorted, [
      { ...sortedOutput[0], columnName: "id" },
    ])).toThrow(/does not match/);

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });
    expect(manager.lifecycleSnapshot("sorted", "score")).toMatchObject({
      state: "failed",
      dirtyReason: "worker-failed",
    });
    expect(rows.where("status", "=", "active").count()).toBe(3);
    expect(rows.where("score", ">=", 20).count()).toBe(3);
  });

  it("discards late equality and sorted results after a valid apply already won", () => {
    const rows = createRows().createIndex("status").createSortedIndex("score");
    rows.update(1, { status: "active", score: 25 });
    const manager = indexManager(rows);
    const equality = equalityMetadata(manager, "status");
    const sorted = sortedMetadata(manager, "score", rows.rowCount);
    const equalityOutput = equalityResults(equality, storages(rows).status.describeChunks());
    const sortedOutput = sortedResults(sorted, storages(rows).score.describeChunks());

    expect(manager.startEqualityBackgroundRebuild(equality, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(equality, "update:indexed-column")).toBe(true);
    expect(manager.completeEqualityBackgroundRebuild(equality, equalityOutput)).toBe("applied");
    expect(manager.completeEqualityBackgroundRebuild(equality, equalityOutput)).toBe("stale");

    expect(manager.startSortedBackgroundRebuild(sorted, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(sorted, "update:indexed-column")).toBe(true);
    expect(manager.completeSortedBackgroundRebuild(sorted, sortedOutput)).toBe("applied");
    expect(manager.completeSortedBackgroundRebuild(sorted, sortedOutput)).toBe("stale");
  });

  it("does not let apply-pending job state leak through serialization or restore", () => {
    const rows = createRows().createIndex("status").createSortedIndex("score");
    rows.update(1, { status: "active", score: 25 });
    const manager = indexManager(rows);
    const equality = equalityMetadata(manager, "status");
    const sorted = sortedMetadata(manager, "score", rows.rowCount);
    expect(manager.startEqualityBackgroundRebuild(equality, "update:indexed-column")).toBe(true);
    expect(manager.markEqualityBackgroundRebuildStarted(equality, "update:indexed-column")).toBe(true);
    expect(manager.startSortedBackgroundRebuild(sorted, "update:indexed-column")).toBe(true);
    expect(manager.markSortedBackgroundRebuildStarted(sorted, "update:indexed-column")).toBe(true);

    const restored = table.deserialize(rows.serialize());

    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
    expect(restored.toArray()).toEqual(rows.toArray());
  });
});

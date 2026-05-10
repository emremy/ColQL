import { describe, expect, it } from "vitest";
import { column, table } from "../src";
import { IndexLifecycle } from "../src/indexing/index-lifecycle";
import { IndexManager } from "../src/indexing/index-manager";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive"] as const),
};

const rows = [
  { id: 1, age: 20, score: 100, status: 0 },
  { id: 2, age: 30, score: 200, status: 1 },
  { id: 3, age: 40, score: 300, status: 0 },
];

function comparable(rowIndex: number, columnName: string): number {
  return rows[rowIndex][columnName as keyof (typeof rows)[number]];
}

function numeric(rowIndex: number, columnName: string): number {
  return comparable(rowIndex, columnName);
}

function createManager(): IndexManager {
  const manager = new IndexManager();
  manager.create("status", schema.status, rows.length, comparable);
  manager.createSorted("age", schema.age, rows.length, numeric);
  manager.createUnique("id", schema.id, rows.length, comparable);
  return manager;
}

function lifecycleDiagnostics(instance: unknown): Pick<IndexManager, "lifecycleSnapshot"> {
  return (instance as { indexManager: Pick<IndexManager, "lifecycleSnapshot"> }).indexManager;
}

describe("index lifecycle state machine", () => {
  it("marks created indexes fresh", () => {
    const manager = createManager();

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
  });

  it("marks indexed-column updates dirty without dirtying unrelated indexes", () => {
    const manager = createManager();

    manager.markPerformanceColumnsDirty(["status"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "dirty",
      generation: 1,
      columnEpoch: 1,
      dirtyReason: "update:indexed-column",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
  });

  it("does not dirty indexes for non-indexed column updates", () => {
    const manager = createManager();

    manager.markPerformanceColumnsDirty(["score"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
  });

  it("marks row-position-sensitive indexes dirty after delete", () => {
    const manager = createManager();

    manager.markDeletedRow(1);

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "dirty",
      generation: 1,
      columnEpoch: 1,
      dirtyReason: "delete:row-position-shift",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "dirty",
      generation: 1,
      columnEpoch: 1,
      dirtyReason: "delete:row-position-shift",
    });
    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "fresh",
      generation: 1,
      columnEpoch: 1,
    });
  });

  it("marks synchronously rebuilt indexes fresh", () => {
    const manager = createManager();
    manager.markPerformanceColumnsDirty(["status", "age"], "update:indexed-column");

    manager.rebuild("status", rows.length, comparable);
    manager.rebuildSorted("age", rows.length, numeric);

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
      generation: 1,
      columnEpoch: 1,
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
      generation: 1,
      columnEpoch: 1,
    });
  });

  it("increments equality and sorted generations monotonically when columns are dirtied", () => {
    const manager = createManager();

    manager.markPerformanceColumnsDirty(["status", "age"], "update:indexed-column");
    manager.markPerformanceColumnsDirty(["status", "age"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toMatchObject({
      state: "dirty",
      generation: 2,
      columnEpoch: 2,
    });
  });

  it("increments unique index generation when its column is dirtied", () => {
    const manager = createManager();

    manager.markUniqueDirty(["id"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "dirty",
      generation: 1,
      columnEpoch: 1,
      dirtyReason: "update:indexed-column",
    });
  });

  it("does not increment unrelated index generations for non-indexed updates", () => {
    const manager = createManager();

    manager.markPerformanceColumnsDirty(["score"], "update:indexed-column");
    manager.markUniqueDirty(["score"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("equality", "status")).toMatchObject({ generation: 0, columnEpoch: 0 });
    expect(manager.lifecycleSnapshot("sorted", "age")).toMatchObject({ generation: 0, columnEpoch: 0 });
    expect(manager.lifecycleSnapshot("unique", "id")).toMatchObject({ generation: 0, columnEpoch: 0 });
  });

  it("does not mutate lifecycle generation or epoch during explain", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      status: column.dictionary(["active", "passive"] as const),
    })
      .insertMany([
        { id: 1, age: 20, status: "active" },
        { id: 2, age: 30, status: "passive" },
        { id: 3, age: 40, status: "active" },
      ])
      .createIndex("status")
      .createSortedIndex("age");
    const diagnostics = lifecycleDiagnostics(users);

    users.update(0, { status: "passive", age: 25 });
    const equalityBefore = diagnostics.lifecycleSnapshot("equality", "status");
    const sortedBefore = diagnostics.lifecycleSnapshot("sorted", "age");

    expect(users.where("status", "=", "passive").explain().reasonCode).toBe("INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION");
    expect(users.where("age", ">=", 25).explain().reasonCode).toBe("INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION");

    expect(diagnostics.lifecycleSnapshot("equality", "status")).toEqual(equalityBefore);
    expect(diagnostics.lifecycleSnapshot("sorted", "age")).toEqual(sortedBefore);
  });

  it("does not increment generation or dirty state for failed mutations", () => {
    const users = table({
      id: column.uint32(),
      status: column.dictionary(["active", "passive"] as const),
    })
      .insertMany([
        { id: 1, status: "active" },
        { id: 2, status: "passive" },
      ])
      .createUniqueIndex("id")
      .createIndex("status");
    const diagnostics = lifecycleDiagnostics(users);
    const uniqueBefore = diagnostics.lifecycleSnapshot("unique", "id");
    const equalityBefore = diagnostics.lifecycleSnapshot("equality", "status");

    expect(() => users.update(0, { id: 2 })).toThrow();

    expect(diagnostics.lifecycleSnapshot("unique", "id")).toEqual(uniqueBefore);
    expect(diagnostics.lifecycleSnapshot("equality", "status")).toEqual(equalityBefore);
  });

  it("does not preserve index generation state across serialization and restore", () => {
    const users = table({
      id: column.uint32(),
      status: column.dictionary(["active", "passive"] as const),
    })
      .insertMany([
        { id: 1, status: "active" },
        { id: 2, status: "passive" },
      ])
      .createIndex("status");
    const diagnostics = lifecycleDiagnostics(users);

    users.update(0, { status: "passive" });
    expect(diagnostics.lifecycleSnapshot("equality", "status")).toMatchObject({
      state: "dirty",
      generation: 1,
      columnEpoch: 1,
    });

    const restored = table.deserialize(users.serialize());
    expect(restored.indexes()).toEqual([]);

    restored.createIndex("status");
    expect(lifecycleDiagnostics(restored).lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
      generation: 0,
      columnEpoch: 0,
    });
  });

  it("can represent failed and config-change lifecycle states internally", () => {
    const lifecycle = new IndexLifecycle();

    lifecycle.markFailed("worker exited");
    expect(lifecycle.snapshot()).toEqual({
      state: "failed",
      generation: 1,
      dirtyReason: "worker-failed",
      failureReason: "worker exited",
    });

    lifecycle.markDirty("config-change");
    expect(lifecycle.snapshot()).toEqual({
      state: "dirty",
      generation: 2,
      dirtyReason: "config-change",
    });
  });
});

import { describe, expect, it } from "vitest";
import { column } from "../src";
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

describe("index lifecycle state machine", () => {
  it("marks created indexes fresh", () => {
    const manager = createManager();

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
    });
    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "fresh",
    });
  });

  it("marks indexed-column updates dirty without dirtying unrelated indexes", () => {
    const manager = createManager();

    manager.markPerformanceColumnsDirty(["status"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "dirty",
      dirtyReason: "update:indexed-column",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
    });
    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "fresh",
    });
  });

  it("does not dirty indexes for non-indexed column updates", () => {
    const manager = createManager();

    manager.markPerformanceColumnsDirty(["score"], "update:indexed-column");

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
    });
  });

  it("marks row-position-sensitive indexes dirty after delete", () => {
    const manager = createManager();

    manager.markDeletedRow(1);

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "dirty",
      dirtyReason: "delete:row-position-shift",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "dirty",
      dirtyReason: "delete:row-position-shift",
    });
    expect(manager.lifecycleSnapshot("unique", "id")).toEqual({
      state: "fresh",
    });
  });

  it("marks synchronously rebuilt indexes fresh", () => {
    const manager = createManager();
    manager.markPerformanceColumnsDirty(["status", "age"], "update:indexed-column");

    manager.rebuild("status", rows.length, comparable);
    manager.rebuildSorted("age", rows.length, numeric);

    expect(manager.lifecycleSnapshot("equality", "status")).toEqual({
      state: "fresh",
    });
    expect(manager.lifecycleSnapshot("sorted", "age")).toEqual({
      state: "fresh",
    });
  });

  it("can represent failed and config-change lifecycle states internally", () => {
    const lifecycle = new IndexLifecycle();

    lifecycle.markFailed("worker exited");
    expect(lifecycle.snapshot()).toEqual({
      state: "failed",
      dirtyReason: "worker-failed",
      failureReason: "worker exited",
    });

    lifecycle.markDirty("config-change");
    expect(lifecycle.snapshot()).toEqual({
      state: "dirty",
      dirtyReason: "config-change",
    });
  });
});

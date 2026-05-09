import { describe, expect, it } from "vitest";
import { column, table, type QueryInfo } from "../src";
import type { IndexManager, IndexLifecycleKind } from "../src/indexing/index-manager";
import type { IndexDirtyReason } from "../src/indexing/index-lifecycle";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
};

function createUsers(options?: { onQuery?: (info: QueryInfo) => void }) {
  return table(schema, options).insertMany([
    { id: 1, age: 20, status: "active" },
    { id: 2, age: 30, status: "passive" },
    { id: 3, age: 40, status: "active" },
    { id: 4, age: 50, status: "passive" },
  ]);
}

function indexManager(instance: unknown): IndexManager {
  return (instance as { indexManager: IndexManager }).indexManager;
}

function markQueued(
  instance: unknown,
  kind: IndexLifecycleKind,
  column: string,
  reason?: IndexDirtyReason,
): void {
  indexManager(instance).markLifecycleQueued(kind, column, reason);
}

function markRebuilding(
  instance: unknown,
  kind: IndexLifecycleKind,
  column: string,
  reason?: IndexDirtyReason,
): void {
  indexManager(instance).markLifecycleRebuilding(kind, column, reason);
}

function markFailed(instance: unknown, kind: IndexLifecycleKind, column: string): void {
  indexManager(instance).markLifecycleFailed(kind, column, "test failure");
}

describe("query fallback semantics", () => {
  it("still uses fresh indexes", () => {
    const users = createUsers().createIndex("id");

    users.resetScanCounter();
    expect(users.where("id", "=", 3).toArray()).toEqual([
      { id: 3, age: 40, status: "active" },
    ]);
    expect(users.scannedRowCount).toBe(1);
    expect(users.where("id", "=", 3).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        selectedIndex: "equality:id",
        indexState: "fresh",
      }),
    );
  });

  it("keeps current dirty synchronous rebuild behavior by default", () => {
    const users = createUsers().createIndex("id");
    users.update(2, { id: 30 });

    expect(users.where("id", "=", 30).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexState: "dirty",
        fallbackReason: "dirty-index",
        reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
      }),
    );

    users.resetScanCounter();
    expect(users.where("id", "=", 30).toArray()).toEqual([
      { id: 30, age: 40, status: "active" },
    ]);
    expect(users.scannedRowCount).toBe(1);
    expect(users.where("id", "=", 30).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexState: "fresh",
      }),
    );
  });

  it("does not rebuild or schedule work during dirty explain", () => {
    const users = createUsers().createSortedIndex("age");
    users.insert({ id: 5, age: 60, status: "active" });
    const statsBefore = users.sortedIndexStats();

    users.resetScanCounter();
    users.resetMaterializationCounter();
    expect(users.where("age", ">=", 50).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexState: "dirty",
        fallbackReason: "dirty-index",
      }),
    );

    expect(users.scannedRowCount).toBe(0);
    expect(users.materializedRowCount).toBe(0);
    expect(users.sortedIndexStats()).toEqual(statsBefore);
  });

  it("falls back to a scan when an equality index is queued", () => {
    const users = createUsers().createIndex("status");
    markQueued(users, "equality", "status", "update:indexed-column");

    users.resetScanCounter();
    expect(users.where("status", "=", "active").toArray()).toEqual([
      { id: 1, age: 20, status: "active" },
      { id: 3, age: 40, status: "active" },
    ]);
    expect(users.scannedRowCount).toBe(users.rowCount);
    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        indexState: "queued",
        fallbackReason: "queued-index",
      }),
    );
  });

  it("falls back to a scan when a sorted index is rebuilding", () => {
    const users = createUsers().createSortedIndex("age");
    markRebuilding(users, "sorted", "age", "update:indexed-column");

    users.resetScanCounter();
    expect(users.where("age", ">=", 40).toArray()).toEqual([
      { id: 3, age: 40, status: "active" },
      { id: 4, age: 50, status: "passive" },
    ]);
    expect(users.scannedRowCount).toBe(users.rowCount);
    expect(users.where("age", ">=", 40).explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        indexState: "rebuilding",
        fallbackReason: "rebuilding-index",
      }),
    );
  });

  it("falls back to a scan when an index has failed", () => {
    const users = createUsers().createIndex("status");
    markFailed(users, "equality", "status");

    users.resetScanCounter();
    expect(users.where("status", "=", "passive").toArray()).toEqual([
      { id: 2, age: 30, status: "passive" },
      { id: 4, age: 50, status: "passive" },
    ]);
    expect(users.scannedRowCount).toBe(users.rowCount);
    expect(users.where("status", "=", "passive").explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        indexState: "failed",
        fallbackReason: "failed-index",
      }),
    );
  });

  it("uses another fresh index when the matching index is rebuilding", () => {
    const users = createUsers().createIndex("status").createIndex("id");
    markRebuilding(users, "equality", "status", "update:indexed-column");

    users.resetScanCounter();
    expect(users.where("status", "=", "active").where("id", "=", 3).toArray()).toEqual([
      { id: 3, age: 40, status: "active" },
    ]);
    expect(users.scannedRowCount).toBe(1);
    expect(users.where("status", "=", "active").where("id", "=", 3).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        selectedIndex: "equality:id",
        indexState: "fresh",
      }),
    );
  });

  it("reports fallback state through onQuery when a queued index falls back", () => {
    const events: QueryInfo[] = [];
    const users = createUsers({ onQuery: (info) => events.push(info) }).createIndex("status");
    markQueued(users, "equality", "status", "update:indexed-column");

    expect(users.where("status", "=", "active").count()).toBe(2);

    expect(events).toHaveLength(1);
    expect(events[0]).toEqual(
      expect.objectContaining({
        indexUsed: false,
        scanType: "full",
        indexState: "queued",
        fallbackReason: "queued-index",
      }),
    );
  });

  it("keeps query APIs synchronous", () => {
    const users = createUsers().createIndex("id");
    const result = users.where("id", "=", 1).toArray();

    expect(result).toEqual([{ id: 1, age: 20, status: "active" }]);
    expect(result).not.toBeInstanceOf(Promise);
  });
});

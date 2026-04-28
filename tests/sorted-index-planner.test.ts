import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function usersTable() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive"] as const),
  });

  for (let i = 0; i < 100; i += 1) {
    users.insert({ id: i, age: i, status: i < 50 ? "active" : "passive" });
  }

  return users;
}

describe("sorted index planner", () => {
  it("uses a sorted index for selective range queries", () => {
    const users = usersTable();
    const expected = users.where("age", ">", 90).toArray();

    users.createSortedIndex("age");
    users.resetScanCounter();

    const query = users.where("age", ">", 90);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      source: "sorted",
      column: "age",
      operator: ">",
      candidateCount: 9,
    }));
    expect(query.toArray()).toEqual(expected);
    expect(users.scannedRowCount).toBe(9);
  });

  it("falls back to scan for broad range queries", () => {
    const users = usersTable();
    const expected = users.where("age", ">", 10).toArray();

    users.createSortedIndex("age");
    users.resetScanCounter();

    const query = users.where("age", ">", 10);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "scan",
      source: "sorted",
      column: "age",
      operator: ">",
      candidateCount: 89,
    }));
    expect(query.toArray()).toEqual(expected);
    expect(users.scannedRowCount).toBe(100);
  });

  it("prefers the equality index when it has fewer candidates", () => {
    const users = usersTable();
    const expected = users.where("id", "=", 95).where("age", ">", 90).toArray();

    users.createIndex("id").createSortedIndex("age");
    users.resetScanCounter();

    const query = users.where("age", ">", 90).where("id", "=", 95);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      source: "equality",
      column: "id",
      candidateCount: 1,
    }));
    expect(query.toArray()).toEqual(expected);
    expect(users.scannedRowCount).toBe(1);
  });

  it("selects the sorted index when it is smaller than an equality candidate set", () => {
    const users = usersTable();
    const expected = users.where("status", "=", "active").where("age", "<", 5).toArray();

    users.createIndex("status").createSortedIndex("age");
    users.resetScanCounter();

    const query = users.where("status", "=", "active").where("age", "<", 5);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      source: "sorted",
      column: "age",
      candidateCount: 5,
    }));
    expect(query.toArray()).toEqual(expected);
    expect(users.scannedRowCount).toBe(5);
  });

  it("uses zero-candidate sorted indexes to terminate quickly", () => {
    const users = usersTable();
    users.createSortedIndex("age");
    users.resetScanCounter();

    const query = users.where("age", ">", 200);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      source: "sorted",
      candidateCount: 0,
    }));
    expect(query.toArray()).toEqual([]);
    expect(users.scannedRowCount).toBe(0);
  });

  it("keeps results correct with multiple filters", () => {
    const users = usersTable();
    const expected = users.where("status", "=", "passive").where("age", "<=", 55).toArray();

    users.createIndex("status").createSortedIndex("age");

    expect(users.where("age", "<=", 55).where("status", "=", "passive").toArray()).toEqual(expected);
  });
});

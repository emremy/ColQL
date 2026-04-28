import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function usersTable() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive"] as const),
  });

  for (let i = 0; i < 100; i += 1) {
    users.insert({ id: i, age: i % 50, status: i % 2 === 0 ? "active" : "passive" });
  }

  return users;
}

describe("index planner", () => {
  it("uses indexed candidates for equality filters", () => {
    const users = usersTable();
    users.createIndex("id");
    users.resetScanCounter();

    expect(users.where("id", "=", 90).toArray()).toEqual([{ id: 90, age: 40, status: "active" }]);
    expect(users.scannedRowCount).toBe(1);
  });

  it("uses indexed candidates for in filters", () => {
    const users = usersTable();
    users.createIndex("id");
    users.resetScanCounter();

    expect(users.whereIn("id", [10, 20, 30]).count()).toBe(3);
    expect(users.scannedRowCount).toBe(3);
  });

  it("applies remaining non-indexed filters after indexed candidate selection", () => {
    const users = usersTable();
    const expected = users.where("id", "in", [10, 20, 30, 90]).where("age", ">", 25).toArray();

    users.createIndex("id");
    users.resetScanCounter();

    expect(users.where("id", "in", [10, 20, 30, 90]).where("age", ">", 25).toArray()).toEqual(expected);
    expect(users.scannedRowCount).toBe(4);
  });

  it("chooses the smallest candidate set when multiple indexed filters exist", () => {
    const users = usersTable();
    users.createIndex("status");
    users.createIndex("id");
    users.resetScanCounter();

    expect(users.where("status", "=", "active").where("id", "=", 90).toArray()).toEqual([
      { id: 90, age: 40, status: "active" },
    ]);
    expect(users.scannedRowCount).toBe(1);
  });

  it("does not use indexes for unsupported operators", () => {
    const users = usersTable();
    users.createIndex("id");
    users.resetScanCounter();

    expect(users.where("id", ">", 95).count()).toBe(4);
    expect(users.scannedRowCount).toBe(100);
  });
});

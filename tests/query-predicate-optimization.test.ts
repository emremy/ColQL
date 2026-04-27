import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("predicate optimization", () => {
  it("multiple where conditions produce the same result in one row scan", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      status: column.dictionary(["active", "passive"] as const),
      score: column.uint32(),
    });

    for (let i = 0; i < 50; i += 1) {
      users.insert({
        id: i,
        age: i % 40,
        status: i % 2 === 0 ? "active" : "passive",
        score: i * 10,
      });
    }

    users.resetScanCounter();
    const result = users
      .where("age", ">=", 18)
      .where("status", "=", "active")
      .where("score", "<", 400)
      .select(["id", "age", "status"])
      .toArray();

    expect(result).toEqual([
      { id: 18, age: 18, status: "active" },
      { id: 20, age: 20, status: "active" },
      { id: 22, age: 22, status: "active" },
      { id: 24, age: 24, status: "active" },
      { id: 26, age: 26, status: "active" },
      { id: 28, age: 28, status: "active" },
      { id: 30, age: 30, status: "active" },
      { id: 32, age: 32, status: "active" },
      { id: 34, age: 34, status: "active" },
      { id: 36, age: 36, status: "active" },
      { id: 38, age: 38, status: "active" },
    ]);
    expect(users.scannedRowCount).toBe(users.rowCount);
  });

  it("limit stops scanning early", () => {
    const rows = table({ id: column.uint32() });
    for (let i = 0; i < 100; i += 1) {
      rows.insert({ id: i });
    }

    rows.resetScanCounter();
    expect(rows.where("id", ">=", 10).limit(1).toArray()).toEqual([{ id: 10 }]);
    expect(rows.scannedRowCount).toBe(11);
  });
});

import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("query optimizer", () => {
  it("chooses indexed predicates independent of where order", () => {
    const users = table({ id: column.uint32(), age: column.uint8(), status: column.dictionary(["active", "passive"] as const) });
    for (let i = 0; i < 100; i += 1) {
      users.insert({ id: i, age: i, status: i % 2 === 0 ? "active" : "passive" });
    }
    const expected = users.where("id", "=", 90).where("age", ">", 18).toArray();

    users.createIndex("id");
    users.resetScanCounter();

    const query = users.where("age", ">", 18).where("id", "=", 90);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({ mode: "index", source: "equality", column: "id" }));
    expect(query.toArray()).toEqual(expected);
    expect(users.scannedRowCount).toBe(1);
  });

  it("reorders non-index filters without changing AND semantics", () => {
    const users = table({ id: column.uint32(), age: column.uint8(), score: column.float64() });
    for (let i = 0; i < 100; i += 1) {
      users.insert({ id: i, age: i % 100, score: i / 10 });
    }

    expect(users.where("score", ">", 4).where("age", "=", 42).toArray()).toEqual(
      users.where("age", "=", 42).where("score", ">", 4).toArray(),
    );
  });
});

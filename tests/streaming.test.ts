import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("streaming and iterators", () => {
  it("iterates table rows lazily", () => {
    const rows = table({ id: column.uint32(), age: column.uint8() });
    rows.insert({ id: 1, age: 10 }).insert({ id: 2, age: 20 });

    const seen: number[] = [];
    for (const row of rows) {
      seen.push(row.id);
    }

    expect(seen).toEqual([1, 2]);
  });

  it("iterates filtered selected queries with limit and offset", () => {
    const rows = table({
      id: column.uint32(),
      age: column.uint8(),
      status: column.dictionary(["active", "passive"] as const),
    });

    for (let i = 0; i < 10; i += 1) {
      rows.insert({ id: i, age: i + 10, status: i % 2 === 0 ? "active" : "passive" });
    }

    const result: Array<{ id: number; status: "active" | "passive" }> = [];
    for (const row of rows.where("age", ">=", 12).whereIn("status", ["active"]).select(["id", "status"]).offset(1).limit(2)) {
      result.push(row);
    }

    expect(result).toEqual([
      { id: 4, status: "active" },
      { id: 6, status: "active" },
    ]);
  });

  it("does not allocate a full result array during iteration", () => {
    const rows = table({ id: column.uint32() });
    for (let i = 0; i < 100; i += 1) {
      rows.insert({ id: i });
    }

    rows.resetMaterializationCounter();
    let count = 0;
    for (const _row of rows.limit(3)) {
      count += 1;
    }

    expect(count).toBe(3);
    expect(rows.materializedRowCount).toBe(3);
  });
});

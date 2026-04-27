import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("query limit/offset", () => {
  it("limits, offsets, and combines both", () => {
    const rows = table({ id: column.uint32(), age: column.uint8() });
    for (let i = 0; i < 10; i += 1) {
      rows.insert({ id: i, age: i });
    }

    expect(rows.limit(3).toArray()).toHaveLength(3);
    expect(rows.offset(8).toArray().map((row) => row.id)).toEqual([8, 9]);
    expect(rows.offset(2).limit(3).toArray().map((row) => row.id)).toEqual([2, 3, 4]);
  });

  it("stops after enough matches when limit is reached", () => {
    const rows = table({ id: column.uint32() });
    for (let i = 0; i < 100; i += 1) {
      rows.insert({ id: i });
    }

    rows.resetMaterializationCounter();
    expect(rows.where("id", ">=", 10).limit(1).toArray()).toEqual([{ id: 10 }]);
    expect(rows.materializedRowCount).toBe(1);
  });
});

import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function scoreTable() {
  const rows = table({
    id: column.uint32(),
    score: column.int32(),
    status: column.dictionary(["active", "passive"] as const),
  });

  for (let i = 0; i < 100; i += 1) {
    rows.insert({ id: i, score: (i * 37) % 101, status: i % 2 === 0 ? "active" : "passive" });
  }

  return rows;
}

describe("top/bottom", () => {
  it("returns the top N rows by numeric column", () => {
    const rows = scoreTable();
    const top = rows.top(5, "score");

    expect(top).toHaveLength(5);
    expect(top.map((row) => row.score)).toEqual([100, 99, 98, 97, 96]);
  });

  it("returns the bottom N rows by numeric column", () => {
    const rows = scoreTable();
    const bottom = rows.bottom(4, "score");

    expect(bottom).toHaveLength(4);
    expect(bottom.map((row) => row.score)).toEqual([0, 1, 2, 3]);
  });

  it("works with filters and selected columns", () => {
    const rows = scoreTable();
    const top = rows.where("status", "=", "active").select(["id", "score"]).top(3, "score");

    expect(top).toHaveLength(3);
    expect(top[0]).toEqual({ id: 30, score: 100 });
    expect(top[0]).not.toHaveProperty("status");
  });

  it("handles N greater than rowCount, zero, and empty datasets", () => {
    const rows = table({ id: column.uint32(), score: column.int32() });
    expect(rows.top(10, "score")).toEqual([]);

    rows.insert({ id: 1, score: 9 }).insert({ id: 2, score: 3 });
    expect(rows.top(10, "score").map((row) => row.id)).toEqual([1, 2]);
    expect(() => rows.bottom(0, "score")).toThrow(/Invalid bottom count/);
  });

  it("respects an upstream query limit", () => {
    const rows = scoreTable();
    const topWithinFirstTen = rows.limit(10).top(3, "score");

    expect(topWithinFirstTen.map((row) => row.score)).toEqual([94, 84, 74]);
  });

  it("stores only N rows for output and does not materialize the full dataset", () => {
    const rows = scoreTable();
    rows.resetMaterializationCounter();

    expect(rows.top(7, "score")).toHaveLength(7);
    expect(rows.materializedRowCount).toBe(7);
  });
});

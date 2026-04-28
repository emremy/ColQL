import { describe, expect, it } from "vitest";
import { ColQLError } from "../src";
import { experimentalChunkedTable } from "../src/experimental/chunked/chunked-table";

function createTable(chunkSize = 4) {
  const rows = experimentalChunkedTable({
    id: { kind: "numeric", type: "uint32" },
    age: { kind: "numeric", type: "uint8" },
    score: { kind: "numeric", type: "float64" },
    status: { kind: "dictionary", values: ["active", "passive", "archived"] as const },
    is_active: { kind: "boolean" },
  }, chunkSize);

  for (let i = 0; i < 13; i += 1) {
    rows.insert({
      id: i,
      age: i % 100,
      score: i + 0.25,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived",
      is_active: i % 2 === 0,
    });
  }

  return rows;
}

describe("experimental physical delete table", () => {
  it("gets, counts, inserts many, and materializes rows", () => {
    const rows = createTable();

    expect(rows.count()).toBe(13);
    expect(rows.get(3)).toEqual({ id: 3, age: 3, score: 3.25, status: "active", is_active: false });

    rows.insertMany([{ id: 99, age: 9, score: 9.9, status: "passive", is_active: true }]);
    expect(rows.count()).toBe(14);
    expect(rows.get(13).id).toBe(99);
  });

  it("matches an array baseline for first, middle, and last physical deletes", () => {
    const rows = createTable();
    const baseline = rows.toArray();

    baseline.splice(0, 1);
    rows.delete(0);
    expect(rows.toArray()).toEqual(baseline);

    baseline.splice(5, 1);
    rows.delete(5);
    expect(rows.toArray()).toEqual(baseline);

    baseline.splice(baseline.length - 1, 1);
    rows.delete(rows.rowCount - 1);
    expect(rows.toArray()).toEqual(baseline);
  });

  it("matches an array baseline across chunk sizes", () => {
    for (const chunkSize of [2, 3, 8]) {
      const rows = createTable(chunkSize);
      const baseline = rows.toArray();

      for (const index of [1, 3, 4, 0]) {
        baseline.splice(index, 1);
        rows.delete(index);
        expect(rows.toArray()).toEqual(baseline);
      }
    }
  });

  it("supports scan-based where queries after deletes", () => {
    const rows = createTable();
    rows.delete(0).delete(4);

    expect(rows.where("status", "=", "active").toArray()).toEqual(
      rows.toArray().filter((row) => row.status === "active"),
    );
    expect(rows.where("age", ">", 8).count()).toBe(rows.toArray().filter((row) => row.age > 8).length);
  });

  it("marks indexes dirty after mutations for future rebuild integration", () => {
    const rows = createTable();

    expect(rows.dirtyIndexes).toBe(true);
    rows.delete(0);
    expect(rows.dirtyIndexes).toBe(true);
  });

  it("throws ColQLError for invalid row indexes", () => {
    const rows = createTable();

    expect(() => rows.delete(rows.rowCount)).toThrow(ColQLError);
    expect(() => rows.get(-1)).toThrow(ColQLError);
  });
});

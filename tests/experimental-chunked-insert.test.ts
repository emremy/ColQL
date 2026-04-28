import { describe, expect, it } from "vitest";
import { experimentalChunkedTable } from "../src/experimental/chunked/chunked-table";

const schema = {
  id: { kind: "numeric", type: "uint32" },
  age: { kind: "numeric", type: "uint8" },
  score: { kind: "numeric", type: "float64" },
  status: { kind: "dictionary", values: ["active", "passive", "archived"] as const },
  is_active: { kind: "boolean" },
} as const;

function makeRow(i: number) {
  return {
    id: i,
    age: i % 100,
    score: i / 10,
    status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived",
    is_active: i % 2 === 0,
  } as const;
}

describe("experimental chunked insert behavior", () => {
  it("insert preserves order across multiple chunks", () => {
    const chunked = experimentalChunkedTable(schema, 3);
    const rows = Array.from({ length: 14 }, (_, index) => makeRow(index));

    rows.forEach((row) => chunked.insert(row));

    expect(chunked.count()).toBe(14);
    expect(chunked.toArray()).toEqual(rows);
  });

  it("insertMany preserves order and validates before mutation", () => {
    const chunked = experimentalChunkedTable(schema, 4);
    const rows = Array.from({ length: 9 }, (_, index) => makeRow(index));

    chunked.insertMany(rows);
    expect(chunked.toArray()).toEqual(rows);

    expect(() => chunked.insertMany([
      makeRow(10),
      { ...makeRow(11), age: 999 },
    ])).toThrow();

    expect(chunked.count()).toBe(9);
    expect(chunked.toArray()).toEqual(rows);
  });
});

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

describe("experimental chunked query behavior", () => {
  it("scan-based numeric and dictionary queries match array baselines", () => {
    const chunked = experimentalChunkedTable(schema, 7);
    const baseline = Array.from({ length: 51 }, (_, index) => makeRow(index));
    baseline.forEach((row) => chunked.insert(row));

    expect(chunked.where("age", ">", 18).toArray()).toEqual(baseline.filter((row) => row.age > 18));
    expect(chunked.where("status", "=", "active").toArray()).toEqual(baseline.filter((row) => row.status === "active"));
    expect(chunked.where("id", "=", 42).toArray()).toEqual(baseline.filter((row) => row.id === 42));
  });

  it("queries remain correct after physical deletes", () => {
    const chunked = experimentalChunkedTable(schema, 6);
    const baseline = Array.from({ length: 35 }, (_, index) => makeRow(index));
    baseline.forEach((row) => chunked.insert(row));

    for (const rowIndex of [3, 10, 0]) {
      baseline.splice(rowIndex, 1);
      chunked.delete(rowIndex);
    }

    expect(chunked.where("age", ">", 18).count()).toBe(baseline.filter((row) => row.age > 18).length);
    expect(chunked.where("status", "=", "passive").toArray()).toEqual(baseline.filter((row) => row.status === "passive"));
  });
});

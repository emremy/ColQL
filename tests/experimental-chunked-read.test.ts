import { describe, expect, it } from "vitest";
import { table, column } from "../src";
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

describe("experimental chunked read behavior", () => {
  it("get(rowIndex) matches production table rows across chunk boundaries", () => {
    const baseline = table({
      id: column.uint32(),
      age: column.uint8(),
      score: column.float64(),
      status: column.dictionary(["active", "passive", "archived"] as const),
      is_active: column.boolean(),
    });
    const chunked = experimentalChunkedTable(schema, 5);

    for (let i = 0; i < 23; i += 1) {
      baseline.insert(makeRow(i));
      chunked.insert(makeRow(i));
    }

    for (const rowIndex of [0, 4, 5, 9, 10, 22]) {
      expect(chunked.get(rowIndex)).toEqual(baseline.get(rowIndex));
    }
  });

  it("scan-style value reads preserve row order after deletes", () => {
    const chunked = experimentalChunkedTable(schema, 4);
    const baseline = Array.from({ length: 17 }, (_, index) => makeRow(index));
    baseline.forEach((row) => chunked.insert(row));

    for (const rowIndex of [0, 6, 14]) {
      baseline.splice(rowIndex, 1);
      chunked.delete(rowIndex);
    }

    expect(chunked.toArray()).toEqual(baseline);
    expect(Array.from({ length: chunked.rowCount }, (_, index) => chunked.getValue(index, "age"))).toEqual(
      baseline.map((row) => row.age),
    );
  });
});

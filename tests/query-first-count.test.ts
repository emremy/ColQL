import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("query first/count", () => {
  it("returns the first matching row or undefined", () => {
    const rows = table({ id: column.uint32(), age: column.uint8() });
    rows.insert({ id: 1, age: 10 }).insert({ id: 2, age: 20 });

    expect(rows.where("age", ">", 15).first()).toEqual({ id: 2, age: 20 });
    expect(rows.where("age", ">", 99).first()).toBeUndefined();
  });

  it("counts without materializing rows", () => {
    const rows = table({ id: column.uint32() });
    rows.insert({ id: 1 }).insert({ id: 2 });
    rows.resetMaterializationCounter();

    expect(rows.count()).toBe(2);
    expect(rows.materializedRowCount).toBe(0);
  });
});

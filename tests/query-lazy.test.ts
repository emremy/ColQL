import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("lazy query behavior", () => {
  it("where/select/limit do not execute immediately", () => {
    const rows = table({ id: column.uint32(), status: column.dictionary(["active", "passive"] as const) });
    rows.insert({ id: 1, status: "active" }).insert({ id: 2, status: "passive" });
    rows.resetMaterializationCounter();

    rows.where("status", "=", "active").select(["id"]).limit(1);
    expect(rows.materializedRowCount).toBe(0);
  });

  it("limit(1).toArray materializes one row and first materializes one row", () => {
    const rows = table({ id: column.uint32() });
    rows.insert({ id: 1 }).insert({ id: 2 }).insert({ id: 3 });

    rows.resetMaterializationCounter();
    expect(rows.limit(1).toArray()).toEqual([{ id: 1 }]);
    expect(rows.materializedRowCount).toBe(1);

    rows.resetMaterializationCounter();
    expect(rows.where("id", ">", 1).first()).toEqual({ id: 2 });
    expect(rows.materializedRowCount).toBe(1);
  });
});

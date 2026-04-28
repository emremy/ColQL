import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("projection pushdown", () => {
  it("select materializes only selected fields in toArray", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      status: column.dictionary(["active", "passive"] as const),
    });
    users.insert({ id: 1, age: 25, status: "active" });

    expect(users.select(["id"]).toArray()).toEqual([{ id: 1 }]);
    expect(users.select(["id", "status"]).toArray()).toEqual([{ id: 1, status: "active" }]);
  });

  it("first materializes one selected row", () => {
    const users = table({ id: column.uint32(), age: column.uint8(), status: column.dictionary(["active", "passive"] as const) });
    users.insert({ id: 1, age: 17, status: "passive" }).insert({ id: 2, age: 25, status: "active" });
    users.resetMaterializationCounter();

    expect(users.where("age", ">", 18).select(["id"]).first()).toEqual({ id: 2 });
    expect(users.materializedRowCount).toBe(1);
  });

  it("for-of iteration is lazy and respects select", () => {
    const users = table({ id: column.uint32(), age: column.uint8(), status: column.dictionary(["active", "passive"] as const) });
    for (let i = 0; i < 10; i += 1) {
      users.insert({ id: i, age: i, status: i % 2 === 0 ? "active" : "passive" });
    }
    users.resetMaterializationCounter();

    const rows: Array<{ id: number }> = [];
    for (const row of users.where("age", ">=", 5).select(["id"])) {
      rows.push(row);
      break;
    }

    expect(rows).toEqual([{ id: 5 }]);
    expect(users.materializedRowCount).toBe(1);
  });
});

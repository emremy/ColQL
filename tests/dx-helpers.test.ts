import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("DX helpers", () => {
  it("whereIn and whereNotIn reuse query logic", () => {
    const users = table({ id: column.uint32(), status: column.dictionary(["active", "passive", "blocked"] as const) });
    users.insert({ id: 1, status: "active" }).insert({ id: 2, status: "passive" }).insert({ id: 3, status: "blocked" });

    expect(users.whereIn("status", ["active", "passive"]).count()).toBe(2);
    expect(users.whereNotIn("status", ["blocked"]).count()).toBe(2);
    expect(users.where("id", ">", 1).whereIn("status", ["passive"]).first()).toEqual({ id: 2, status: "passive" });
  });

  it("size, isEmpty, get, stream, and getSchema work", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    expect(users.isEmpty()).toBe(true);
    expect(users.size()).toBe(0);

    users.insert({ id: 1, age: 25 });
    expect(users.isEmpty()).toBe(false);
    expect(users.size()).toBe(1);
    expect(users.get(0)).toEqual({ id: 1, age: 25 });
    expect(users.getSchema().id.kind).toBe("numeric");
    expect([...users.stream()]).toEqual([{ id: 1, age: 25 }]);
  });

  it("query size and isEmpty work without materializing rows", () => {
    const users = table({ id: column.uint32() });
    users.insert({ id: 1 }).insert({ id: 2 });
    users.resetMaterializationCounter();

    expect(users.where("id", ">", 1).size()).toBe(1);
    expect(users.where("id", ">", 2).isEmpty()).toBe(true);
    expect(users.materializedRowCount).toBe(0);
  });
});

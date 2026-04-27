import { describe, expect, it } from "vitest";
import { Table, table } from "../src/table";
import { column } from "../src/column";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
};

describe("table insert", () => {
  it("inserts one row and returns it from toArray", () => {
    const users = table(schema);
    users.insert({ id: 1, age: 25, status: "active", is_active: true });
    expect(users.rowCount).toBe(1);
    expect(users.toArray()).toEqual([{ id: 1, age: 25, status: "active", is_active: true }]);
  });

  it("grows beyond initial capacity", () => {
    const users = new Table(schema, 2);
    for (let i = 0; i < 5; i += 1) {
      users.insert({ id: i, age: i, status: i % 2 === 0 ? "active" : "passive", is_active: i % 2 === 0 });
    }

    expect(users.rowCount).toBe(5);
    expect(users.capacity).toBe(8);
    expect(users.where("id", "=", 4).first()).toEqual({ id: 4, age: 4, status: "active", is_active: true });
  });
});

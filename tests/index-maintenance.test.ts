import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("index maintenance", () => {
  it("reflects inserts after createIndex", () => {
    const users = table({ id: column.uint32(), status: column.dictionary(["active", "passive"] as const), age: column.uint8() });
    users.createIndex("status");

    users.insert({ id: 1, status: "active", age: 20 });

    expect(users.where("status", "=", "active").toArray()).toEqual([{ id: 1, status: "active", age: 20 }]);
    expect(users.indexStats()[0]).toEqual(expect.objectContaining({ rowCount: 1, uniqueValues: 1 }));
  });

  it("reflects insertMany after createIndex", () => {
    const users = table({ id: column.uint32(), status: column.dictionary(["active", "passive"] as const), age: column.uint8() });
    users.createIndex("id");

    users.insertMany([
      { id: 1, status: "active", age: 20 },
      { id: 2, status: "passive", age: 30 },
    ]);

    expect(users.where("id", "=", 2).first()).toEqual({ id: 2, status: "passive", age: 30 });
  });

  it("failed insert and insertMany do not corrupt indexes", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    users.insert({ id: 1, age: 20 });
    users.createIndex("id");

    expect(() => users.insert({ id: 2, age: 999 })).toThrow();
    expect(users.where("id", "=", 2).toArray()).toEqual([]);
    expect(users.indexStats()[0].rowCount).toBe(1);

    expect(() => users.insertMany([{ id: 2, age: 21 }, { id: 3, age: 300 }])).toThrow();
    expect(users.where("id", "=", 2).toArray()).toEqual([]);
    expect(users.indexStats()[0].rowCount).toBe(1);
  });

  it("does not serialize indexes and can rebuild after deserialization", () => {
    const users = table({ id: column.uint32(), status: column.dictionary(["active", "passive"] as const) });
    users.insert({ id: 1, status: "active" });
    users.createIndex("status");

    const restored = table.deserialize(users.serialize());

    expect(restored.indexes()).toEqual([]);
    expect(restored.where("status", "=", "active").toArray()).toEqual([{ id: 1, status: "active" }]);

    restored.createIndex("status");
    expect(restored.hasIndex("status")).toBe(true);
  });
});

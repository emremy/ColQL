import { describe, expect, it } from "vitest";
import { column, table } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.float64(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  is_active: column.boolean(),
};

describe("delete serialization", () => {
  it("serializes logical rows after deletes and does not serialize indexes", () => {
    const users = table(schema);
    for (let id = 0; id < 30; id += 1) {
      users.insert({
        id,
        age: id % 100,
        score: id + 0.25,
        status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
        is_active: id % 2 === 0,
      });
    }

    users.createIndex("status").createSortedIndex("age");
    users.delete(0);
    users.delete(10);
    users.delete(users.rowCount - 1);
    const expected = users.toArray();

    const restored = table.deserialize(users.serialize());

    expect(restored.toArray()).toEqual(expected);
    expect(restored.rowCount).toBe(expected.length);
    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
    restored.createIndex("status").createSortedIndex("age");
    expect(restored.where("status", "=", "active").where("age", ">", 10).toArray()).toEqual(
      expected.filter((row) => row.status === "active" && row.age > 10),
    );
  });

  it("serializes correctly after deleting every row and inserting again", () => {
    const users = table(schema);
    users.insert({ id: 1, age: 10, score: 1.5, status: "active", is_active: true });
    users.insert({ id: 2, age: 20, score: 2.5, status: "passive", is_active: false });
    users.delete(0);
    users.delete(0);
    users.insert({ id: 3, age: 30, score: 3.5, status: "archived", is_active: true });

    const restored = table.deserialize(users.serialize());

    expect(restored.toArray()).toEqual([{ id: 3, age: 30, score: 3.5, status: "archived", is_active: true }]);
  });
});

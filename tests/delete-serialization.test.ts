import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("delete serialization", () => {
  it("serializes logical row order after deletes and does not serialize indexes", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      score: column.float64(),
      status: column.dictionary(["active", "passive", "archived"] as const),
      is_active: column.boolean(),
    });

    for (let i = 0; i < 20; i += 1) {
      users.insert({
        id: i,
        age: i,
        score: i + 0.25,
        status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived",
        is_active: i % 2 === 0,
      });
    }
    users.createIndex("id");

    const expected = users.toArray();
    expected.splice(5, 1);
    expected.splice(0, 1);
    users.delete(5).delete(0);

    const restored = table.deserialize(users.serialize());

    expect(restored.toArray()).toEqual(expected);
    expect(restored.indexes()).toEqual([]);
  });
});

import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("query select", () => {
  it("selects a subset of columns and excludes unselected fields", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      status: column.dictionary(["active", "passive"] as const),
    });
    users.insert({ id: 1, age: 25, status: "active" });

    expect(users.select(["id", "status"]).toArray()).toEqual([{ id: 1, status: "active" }]);
    expect(users.select(["id", "status"]).toArray()[0]).not.toHaveProperty("age");
  });
});

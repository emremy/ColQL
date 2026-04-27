import { describe, expect, it } from "vitest";
import { Table } from "../src/table";
import { column } from "../src/column";

describe("memory behavior", () => {
  it("uses compact storage and grows by resizing columns", () => {
    const users = new Table(
      {
        id: column.uint32(),
        status: column.dictionary(["active", "passive"] as const),
        is_active: column.boolean(),
      },
      1,
    );

    users.insert({ id: 1, status: "active", is_active: true });
    users.insert({ id: 2, status: "passive", is_active: false });

    expect(users.capacity).toBe(2);
    expect(users.toArray()).toEqual([
      { id: 1, status: "active", is_active: true },
      { id: 2, status: "passive", is_active: false },
    ]);
  });
});

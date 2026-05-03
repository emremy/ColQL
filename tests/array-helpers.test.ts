import { describe, expect, it } from "vitest";
import { column, fromRows, table } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
};

const rows = [
  { id: 1, age: 20, status: "active" },
  { id: 2, age: 30, status: "passive" },
  { id: 3, age: 40, status: "active" },
] as const;

describe("JS Array migration helpers", () => {
  it("fromRows creates a table through insertMany", () => {
    const users = fromRows(schema, rows);

    expect(users.toArray()).toEqual(rows);
    expect(() => fromRows(schema, [{ id: 4, age: 999, status: "active" }])).toThrow();
  });

  it("firstWhere delegates to structured where and callback filter", () => {
    const users = fromRows(schema, rows);

    expect(users.firstWhere({ status: "active" })).toEqual(rows[0]);
    expect(users.firstWhere("id", "=", 2)).toEqual(rows[1]);
    expect(users.firstWhere((row) => row.age > 35)).toEqual(rows[2]);
    expect(users.firstWhere({ id: 999 })).toBeUndefined();
  });

  it("countWhere delegates to structured where and callback filter", () => {
    const users = fromRows(schema, rows);

    expect(users.countWhere({ status: "active" })).toBe(2);
    expect(users.countWhere("age", ">=", 30)).toBe(2);
    expect(users.countWhere((row) => row.status === "passive")).toBe(1);
  });

  it("exists uses a limited query path", () => {
    const users = table(schema).insertMany(rows);

    expect(users.exists({ status: "active" })).toBe(true);
    expect(users.exists("id", "=", 999)).toBe(false);
    expect(users.exists((row) => row.age === 30)).toBe(true);
  });

  it("preserves index behavior for structured helpers", () => {
    const events: boolean[] = [];
    const users = table(schema, {
      onQuery(info) {
        events.push(info.indexUsed);
      },
    }).insertMany(rows);

    users.createIndex("id");
    expect(users.firstWhere("id", "=", 3)).toEqual(rows[2]);
    expect(events.at(-1)).toBe(true);
  });
});

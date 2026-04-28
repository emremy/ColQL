import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("sorted index maintenance", () => {
  it("reflects inserts after createSortedIndex", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    users.insert({ id: 1, age: 20 }).insert({ id: 2, age: 30 });
    users.createSortedIndex("age");

    users.insert({ id: 3, age: 90 });

    expect(users.where("age", ">", 80).toArray()).toEqual([{ id: 3, age: 90 }]);
    expect(users.sortedIndexStats()[0]).toEqual(expect.objectContaining({ dirty: false, rowCount: 3 }));
  });

  it("reflects insertMany after createSortedIndex", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    users.insert({ id: 1, age: 20 });
    users.createSortedIndex("age");

    users.insertMany([
      { id: 2, age: 91 },
      { id: 3, age: 92 },
    ]);

    expect(users.where("age", ">", 90).toArray()).toEqual([
      { id: 2, age: 91 },
      { id: 3, age: 92 },
    ]);
  });

  it("failed insert does not corrupt a sorted index", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    users.insert({ id: 1, age: 20 });
    users.createSortedIndex("age");

    expect(() => users.insert({ id: 2, age: 999 })).toThrow();

    expect(users.rowCount).toBe(1);
    expect(users.where("age", ">=", 20).toArray()).toEqual([{ id: 1, age: 20 }]);
  });

  it("failed insertMany does not partially update sorted indexes", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    users.insert({ id: 1, age: 20 });
    users.createSortedIndex("age");

    expect(() => users.insertMany([
      { id: 2, age: 30 },
      { id: 3, age: 999 },
    ])).toThrow();

    expect(users.rowCount).toBe(1);
    expect(users.where("age", ">=", 20).toArray()).toEqual([{ id: 1, age: 20 }]);
  });
});

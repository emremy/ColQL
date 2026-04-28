import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("production chunked storage", () => {
  it("handles deletes across more than one production chunk", () => {
    const users = table({ id: column.uint32(), age: column.uint8() });
    const count = 70_000;
    for (let i = 0; i < count; i += 1) {
      users.insert({ id: i, age: i % 100 });
    }

    users.delete(65_535);
    users.delete(65_535);

    expect(users.get(65_534)).toEqual({ id: 65_534, age: 34 });
    expect(users.get(65_535)).toEqual({ id: 65_537, age: 37 });
    expect(users.rowCount).toBe(count - 2);
  });
});

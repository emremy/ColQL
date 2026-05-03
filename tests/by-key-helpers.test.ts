import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

const schema = {
  id: column.uint32(),
  status: column.dictionary(["active", "passive"] as const),
  age: column.uint8(),
};

function createUsers() {
  return table(schema)
    .insertMany([
      { id: 1, status: "active", age: 20 },
      { id: 2, status: "passive", age: 30 },
      { id: 3, status: "active", age: 40 },
    ])
    .createUniqueIndex("id");
}

function expectCode(fn: () => unknown, code: string): void {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
  }
}

describe("by-key helpers", () => {
  it("findBy returns existing rows and undefined for missing keys", () => {
    const users = createUsers();

    expect(users.findBy("id", 2)).toEqual({ id: 2, status: "passive", age: 30 });
    expect(users.findBy("id", 999)).toBeUndefined();
  });

  it("updateBy updates existing rows and returns zero for missing keys", () => {
    const users = createUsers();

    expect(users.updateBy("id", 2, { age: 31 })).toEqual({ affectedRows: 1 });
    expect(users.findBy("id", 2)).toEqual({ id: 2, status: "passive", age: 31 });
    expect(users.updateBy("id", 999, { age: 10 })).toEqual({ affectedRows: 0 });
  });

  it("deleteBy deletes existing rows and returns zero for missing keys", () => {
    const users = createUsers();

    expect(users.deleteBy("id", 2)).toEqual({ affectedRows: 1 });
    expect(users.findBy("id", 2)).toBeUndefined();
    expect(users.deleteBy("id", 999)).toEqual({ affectedRows: 0 });
  });

  it("requires a unique index and does not scan without one", () => {
    const users = table(schema).insertMany([
      { id: 1, status: "active", age: 20 },
      { id: 1, status: "passive", age: 30 },
    ]);

    expectCode(() => users.findBy("id", 1), "COLQL_UNIQUE_INDEX_NOT_FOUND");
    expectCode(() => users.updateBy("id", 1, { age: 40 }), "COLQL_UNIQUE_INDEX_NOT_FOUND");
    expectCode(() => users.deleteBy("id", 1), "COLQL_UNIQUE_INDEX_NOT_FOUND");
  });

  it("does not leak rowIndex stability after deletes shift rows", () => {
    const users = createUsers();

    users.deleteBy("id", 1);
    expect(users.findBy("id", 2)).toEqual({ id: 2, status: "passive", age: 30 });
    expect(users.updateBy("id", 3, { age: 41 })).toEqual({ affectedRows: 1 });
    expect(users.findBy("id", 3)).toEqual({ id: 3, status: "active", age: 41 });
  });
});

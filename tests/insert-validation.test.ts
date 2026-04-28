import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function usersTable() {
  return table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float32(),
    status: column.dictionary(["active", "passive"] as const),
    is_active: column.boolean(),
  });
}

function expectCode(fn: () => unknown, code: string, message: RegExp): void {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
    expect((error as Error).message).toMatch(message);
  }
}

describe("insert validation", () => {
  it("rejects invalid row shape, missing values, and unknown fields without mutation", () => {
    const users = usersTable();

    expectCode(() => users.insert(null as never), "COLQL_TYPE_MISMATCH", /expected a non-null object/);
    expectCode(() => users.insert({ id: 1, age: 10, score: 1, status: "active" } as never), "COLQL_MISSING_VALUE", /Missing value for column "is_active"/);
    expectCode(() => users.insert({ id: 1, age: 10, score: 1, status: "active", is_active: true, email: "x" } as never), "COLQL_INVALID_COLUMN", /Unknown column "email"/);
    expect(users.rowCount).toBe(0);
  });

  it("range-checks integer typed arrays before writing", () => {
    const users = usersTable();

    expectCode(() => users.insert({ id: 1, age: 300, score: 1, status: "active", is_active: true }), "COLQL_OUT_OF_RANGE", /expected uint8 integer between 0 and 255/);
    expectCode(() => users.insert({ id: -1, age: 30, score: 1, status: "active", is_active: true }), "COLQL_OUT_OF_RANGE", /expected uint32 integer/);
    expect(users.rowCount).toBe(0);
  });

  it("rejects NaN, Infinity, unknown dictionary values, and non-booleans", () => {
    const users = usersTable();

    expectCode(() => users.insert({ id: 1, age: 30, score: Number.NaN, status: "active", is_active: true }), "COLQL_TYPE_MISMATCH", /finite float32 number/);
    expectCode(() => users.insert({ id: 1, age: 30, score: Infinity, status: "active", is_active: true }), "COLQL_TYPE_MISMATCH", /finite float32 number/);
    expectCode(() => users.insert({ id: 1, age: 30, score: 1, status: "deleted" as "active", is_active: true }), "COLQL_UNKNOWN_VALUE", /Invalid dictionary value/);
    expectCode(() => users.insert({ id: 1, age: 30, score: 1, status: "active", is_active: "true" as unknown as boolean }), "COLQL_TYPE_MISMATCH", /expected boolean/);
    expect(users.rowCount).toBe(0);
  });

  it("insertMany validates all rows before inserting any", () => {
    const users = usersTable();

    expectCode(
      () => users.insertMany([
        { id: 1, age: 20, score: 1, status: "active", is_active: true },
        { id: 2, age: 999, score: 2, status: "passive", is_active: false },
      ]),
      "COLQL_OUT_OF_RANGE",
      /Invalid row at index 1/,
    );

    expect(users.rowCount).toBe(0);
  });
});

import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function usersTable() {
  return table({
    age: column.uint8(),
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

describe("aggregation validation", () => {
  it("rejects non-numeric aggregation columns", () => {
    const users = usersTable();

    expectCode(() => users.sum("status" as "age"), "COLQL_INVALID_COLUMN_TYPE", /numeric column/);
    expectCode(() => users.avg("is_active" as "age"), "COLQL_INVALID_COLUMN_TYPE", /numeric column/);
    expectCode(() => users.min("status" as "age"), "COLQL_INVALID_COLUMN_TYPE", /numeric column/);
    expectCode(() => users.max("status" as "age"), "COLQL_INVALID_COLUMN_TYPE", /numeric column/);
  });

  it("keeps explicit empty aggregation behavior", () => {
    const users = usersTable();

    expect(users.sum("age")).toBe(0);
    expect(users.avg("age")).toBeUndefined();
    expect(users.min("age")).toBeUndefined();
    expect(users.max("age")).toBeUndefined();
  });

  it("validates top and bottom count and columns", () => {
    const users = usersTable();

    expectCode(() => users.top(0, "age"), "COLQL_INVALID_LIMIT", /Invalid top count/);
    expectCode(() => users.bottom(-1, "age"), "COLQL_INVALID_LIMIT", /Invalid bottom count/);
    expectCode(() => users.top(1, "status" as "age"), "COLQL_INVALID_COLUMN_TYPE", /numeric column/);
  });
});

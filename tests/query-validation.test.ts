import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function usersTable() {
  return table({
    id: column.uint32(),
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

describe("query validation", () => {
  it("validates where columns, operators, values, and unsupported comparisons", () => {
    const users = usersTable();

    expectCode(() => users.where("email" as "age", "=", 1), "COLQL_INVALID_COLUMN", /Unknown column "email"/);
    expectCode(() => users.where("age", "contains" as "=", 1), "COLQL_INVALID_OPERATOR", /Invalid operator/);
    expectCode(() => users.where("age", "=", "active" as unknown as number), "COLQL_TYPE_MISMATCH", /expected uint8 integer/);
    expectCode(() => users.where("status", "=", "deleted" as "active"), "COLQL_UNKNOWN_VALUE", /Invalid dictionary value/);
    expectCode(() => users.where("is_active", ">", true), "COLQL_INVALID_OPERATOR", /not supported for boolean column/);
    expectCode(() => users.where("status", ">", "active"), "COLQL_INVALID_OPERATOR", /not supported for dictionary column/);
  });

  it("validates whereIn and whereNotIn arrays", () => {
    const users = usersTable();

    expectCode(() => users.where("id", "in", []), "COLQL_TYPE_MISMATCH", /non-empty array/);
    expectCode(() => users.whereIn("status", []), "COLQL_TYPE_MISMATCH", /non-empty array/);
    expectCode(() => users.whereIn("status", ["active", "deleted" as "active"]), "COLQL_UNKNOWN_VALUE", /Invalid dictionary value/);
    expectCode(() => users.whereNotIn("age", [1, 999]), "COLQL_OUT_OF_RANGE", /uint8 integer/);
  });

  it("validates object predicates", () => {
    const users = usersTable();

    expectCode(() => users.where({}), "COLQL_INVALID_PREDICATE", /at least one column condition/);
    expectCode(() => users.where({ age: {} }), "COLQL_INVALID_PREDICATE", /at least one operator/);
    expectCode(() => users.where({ age: { between: [18, 30] } } as never), "COLQL_INVALID_PREDICATE", /Invalid where predicate operator "between"/);
    expectCode(() => users.where({ status: { gt: "active" } } as never), "COLQL_INVALID_PREDICATE", /dictionary column "status"/);
    expectCode(() => users.where({ is_active: { lt: true } } as never), "COLQL_INVALID_PREDICATE", /boolean column "is_active"/);
    expectCode(() => users.where({ status: { in: [] } }), "COLQL_TYPE_MISMATCH", /non-empty array/);
    expectCode(() => users.where({ age: undefined }), "COLQL_INVALID_PREDICATE", /at least one column condition/);
    expectCode(
      () => users.where({ age: { gt: 18 }, status: { between: ["active", "passive"] } } as never),
      "COLQL_INVALID_PREDICATE",
      /Invalid where predicate operator "between"/,
    );
  });

  it("validates select, limit, offset, and get", () => {
    const users = usersTable();
    users.insert({ id: 1, age: 20, status: "active", is_active: true });

    expectCode(() => users.select([]), "COLQL_INVALID_COLUMN", /non-empty array/);
    expectCode(() => users.select(["age", "age"]), "COLQL_DUPLICATE_COLUMN", /Duplicate column "age"/);
    expectCode(() => users.select(["email" as "age"]), "COLQL_INVALID_COLUMN", /Unknown column "email"/);
    expectCode(() => users.limit(-1), "COLQL_INVALID_LIMIT", /Invalid limit/);
    expectCode(() => users.offset(1.5), "COLQL_INVALID_OFFSET", /Invalid offset/);
    expectCode(() => users.get(1), "COLQL_INVALID_ROW_INDEX", /Invalid row index/);
  });
});

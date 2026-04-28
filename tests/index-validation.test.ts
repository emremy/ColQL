import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function expectCode(fn: () => unknown, code: string, message: RegExp): void {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
    expect((error as Error).message).toMatch(message);
  }
}

describe("index validation", () => {
  it("throws for unknown, duplicate, missing, and unsupported indexes", () => {
    const users = table({
      id: column.uint32(),
      status: column.dictionary(["active", "passive"] as const),
      is_active: column.boolean(),
    });

    expectCode(() => users.createIndex("email" as "id"), "COLQL_INVALID_COLUMN", /Unknown column "email"/);
    expectCode(() => users.createIndex("is_active"), "COLQL_INDEX_UNSUPPORTED_COLUMN", /not supported for boolean column "is_active"/);

    users.createIndex("id");
    expectCode(() => users.createIndex("id"), "COLQL_INDEX_EXISTS", /Index already exists for column "id"/);
    expectCode(() => users.dropIndex("status"), "COLQL_INDEX_NOT_FOUND", /Index not found for column "status"/);
  });
});

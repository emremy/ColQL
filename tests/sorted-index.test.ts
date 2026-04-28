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

function usersTable() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    delta: column.int16(),
    status: column.dictionary(["active", "passive"] as const),
    is_active: column.boolean(),
  });

  for (let i = 0; i < 100; i += 1) {
    users.insert({
      id: i,
      age: i % 50,
      score: i + 0.5,
      delta: i - 50,
      status: i % 2 === 0 ? "active" : "passive",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

describe("sorted index API", () => {
  it("creates, lists, reports, and drops sorted indexes", () => {
    const users = usersTable();

    expect(users.hasSortedIndex("age")).toBe(false);
    users.createSortedIndex("age");

    expect(users.hasSortedIndex("age")).toBe(true);
    expect(users.sortedIndexes()).toEqual(["age"]);
    expect(users.sortedIndexStats()).toEqual([
      expect.objectContaining({
        column: "age",
        rowCount: 100,
        dirty: false,
      }),
    ]);
    expect(users.sortedIndexStats()[0].memoryBytesApprox).toBe(100 * Uint32Array.BYTES_PER_ELEMENT);

    users.dropSortedIndex("age");
    expect(users.hasSortedIndex("age")).toBe(false);
    expect(users.sortedIndexes()).toEqual([]);
  });

  it("throws structured errors for invalid sorted index operations", () => {
    const users = usersTable();

    expectCode(() => users.createSortedIndex("email" as "age"), "COLQL_INVALID_COLUMN", /Unknown column "email"/);
    expectCode(
      () => users.createSortedIndex("status" as "age"),
      "COLQL_SORTED_INDEX_UNSUPPORTED_COLUMN",
      /not supported for dictionary column "status"/,
    );
    expectCode(
      () => users.createSortedIndex("is_active" as "age"),
      "COLQL_SORTED_INDEX_UNSUPPORTED_COLUMN",
      /not supported for boolean column "is_active"/,
    );

    users.createSortedIndex("age");
    expectCode(() => users.createSortedIndex("age"), "COLQL_SORTED_INDEX_EXISTS", /Sorted index already exists/);
    expectCode(() => users.dropSortedIndex("score"), "COLQL_SORTED_INDEX_NOT_FOUND", /Sorted index not found/);
  });
});

describe("sorted index correctness", () => {
  it("matches scan behavior for range operators", () => {
    const users = usersTable();
    const gt = users.where("age", ">", 30).toArray();
    const gte = users.where("age", ">=", 30).toArray();
    const lt = users.where("age", "<", 10).toArray();
    const lte = users.where("age", "<=", 10).toArray();

    users.createSortedIndex("age");

    expect(users.where("age", ">", 30).toArray()).toEqual(gt);
    expect(users.where("age", ">=", 30).toArray()).toEqual(gte);
    expect(users.where("age", "<", 10).toArray()).toEqual(lt);
    expect(users.where("age", "<=", 10).toArray()).toEqual(lte);
  });

  it("handles no matches, all matches, duplicate values, floats, and signed values", () => {
    const users = usersTable();
    const none = users.where("age", ">", 250).toArray();
    const all = users.where("age", ">=", 0).toArray();
    const duplicate = users.where("age", "=", 7).toArray();
    const floats = users.where("score", "<", 3.75).toArray();
    const signed = users.where("delta", "<", -45).toArray();

    users.createSortedIndex("age").createSortedIndex("score").createSortedIndex("delta");

    expect(users.where("age", ">", 250).toArray()).toEqual(none);
    expect(users.where("age", ">=", 0).toArray()).toEqual(all);
    expect(users.where("age", "=", 7).toArray()).toEqual(duplicate);
    expect(users.where("score", "<", 3.75).toArray()).toEqual(floats);
    expect(users.where("delta", "<", -45).toArray()).toEqual(signed);
  });

  it("does not serialize sorted indexes", () => {
    const users = usersTable();
    users.createSortedIndex("age");

    const restored = table.deserialize(users.serialize());

    expect(restored.sortedIndexes()).toEqual([]);
    restored.createSortedIndex("age");
    expect(restored.where("age", ">", 45).count()).toBe(users.where("age", ">", 45).count());
  });
});

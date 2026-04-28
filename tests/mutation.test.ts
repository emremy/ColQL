import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.float64(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  is_active: column.boolean(),
};

function createUsers(count = 12) {
  const users = table(schema);
  for (let id = 0; id < count; id += 1) {
    users.insert({
      id,
      age: id,
      score: id * 10,
      status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
      is_active: id % 2 === 0,
    });
  }
  return users;
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

describe("mutations", () => {
  it("updates a single row and returns affectedRows", () => {
    const users = createUsers();

    expect(users.update(1, { age: 42, status: "active", is_active: true })).toEqual({ affectedRows: 1 });
    expect(users.get(1)).toEqual({ id: 1, age: 42, score: 10, status: "active", is_active: true });
  });

  it("validates update input before mutating", () => {
    const users = createUsers();
    const before = users.toArray();

    expectCode(() => users.update(1, {}), "COLQL_MISSING_VALUE", /at least one column/);
    expectCode(() => users.update(1, { email: "x" } as never), "COLQL_INVALID_COLUMN", /Unknown column "email"/);
    expectCode(() => users.update(1, { age: 999 }), "COLQL_OUT_OF_RANGE", /uint8 integer/);
    expectCode(() => users.update(1, { status: "deleted" as "active" }), "COLQL_UNKNOWN_VALUE", /Invalid dictionary value/);
    expectCode(() => users.update(1, { is_active: "true" as unknown as boolean }), "COLQL_TYPE_MISMATCH", /expected boolean/);
    expectCode(() => users.update(-1, { age: 1 }), "COLQL_INVALID_ROW_INDEX", /Invalid row index/);
    expect(users.toArray()).toEqual(before);
  });

  it("updates only supplied columns and works after delete", () => {
    const users = createUsers();

    users.update(2, { age: 42 });
    expect(users.get(2)).toEqual({ id: 2, age: 42, score: 20, status: "archived", is_active: true });

    users.delete(0);
    expect(users.update(0, { status: "active" })).toEqual({ affectedRows: 1 });
    expect(users.get(0)).toEqual({ id: 1, age: 1, score: 10, status: "active", is_active: false });
  });

  it("updates after serialization and deserialization", () => {
    const users = createUsers();
    users.updateWhere("id", "=", 3, { age: 30 });

    const restored = table.deserialize(users.serialize());
    expect(restored.updateWhere("id", "=", 3, { status: "active" })).toEqual({ affectedRows: 1 });
    expect(restored.where("id", "=", 3).first()).toEqual({ id: 3, age: 30, score: 30, status: "active", is_active: false });
  });

  it("updates predicate matches all-or-nothing and snapshots before mutating predicate columns", () => {
    const users = createUsers();

    expect(users.updateWhere("status", "=", "active", { status: "passive", age: 99 })).toEqual({ affectedRows: 4 });
    expect(users.where("status", "=", "active").count()).toBe(0);
    expect(users.where("age", "=", 99).toArray().map((row) => row.id)).toEqual([0, 3, 6, 9]);

    const before = users.toArray();
    expectCode(
      () => users.updateWhere("status", "=", "passive", { age: 300 }),
      "COLQL_OUT_OF_RANGE",
      /uint8 integer/,
    );
    expect(users.toArray()).toEqual(before);
  });

  it("returns zero for valid no-match predicate mutations", () => {
    const users = createUsers();
    const before = users.toArray();

    expect(users.updateWhere("id", "=", 999, { age: 1 })).toEqual({ affectedRows: 0 });
    expect(users.deleteWhere("id", "=", 999)).toEqual({ affectedRows: 0 });
    expect(users.toArray()).toEqual(before);
  });

  it("deletes predicate matches after snapshotting row indexes", () => {
    const users = createUsers();

    expect(users.deleteWhere("status", "=", "passive")).toEqual({ affectedRows: 4 });
    expect(users.toArray().map((row) => row.id)).toEqual([0, 2, 3, 5, 6, 8, 9, 11]);
  });

  it("query update and delete respect offset and limit but ignore select", () => {
    const users = createUsers();

    expect(
      users
        .where("age", ">=", 2)
        .select(["id"])
        .offset(2)
        .limit(3)
        .update({ status: "archived" }),
    ).toEqual({ affectedRows: 3 });
    expect(users.where("status", "=", "archived").toArray().map((row) => row.id)).toEqual([2, 4, 5, 6, 8, 11]);

    expect(users.where("age", ">=", 2).select(["id"]).offset(2).limit(3).delete()).toEqual({ affectedRows: 3 });
    expect(users.toArray().map((row) => row.id)).toEqual([0, 1, 2, 3, 7, 8, 9, 10, 11]);
  });

  it("applies query update limit and offset independently", () => {
    const limitUsers = createUsers();
    expect(limitUsers.where("age", ">=", 2).limit(2).update({ status: "active" })).toEqual({ affectedRows: 2 });
    expect(limitUsers.toArray().filter((row) => row.status === "active").map((row) => row.id)).toEqual([0, 2, 3, 6, 9]);

    const offsetUsers = createUsers();
    expect(offsetUsers.where("age", ">=", 2).offset(8).update({ status: "active" })).toEqual({ affectedRows: 2 });
    expect(offsetUsers.where("age", ">=", 2).where("status", "=", "active").toArray().map((row) => row.id)).toEqual([3, 6, 9, 10, 11]);
  });

  it("deletes all predicate matches and keeps counts, arrays, streams, and aggregations consistent", () => {
    const users = createUsers();
    const baseline = users.toArray().filter((row) => row.age < 9);

    expect(users.deleteWhere("age", ">=", 9)).toEqual({ affectedRows: 3 });
    expect(users.rowCount).toBe(9);
    expect(users.count()).toBe(9);
    expect(users.size()).toBe(9);
    expect(users.toArray()).toEqual(baseline);
    expect([...users.stream()]).toEqual(baseline);
    expect([...users]).toEqual(baseline);
    expect(users.sum("age")).toBe(baseline.reduce((total, row) => total + row.age, 0));
    expect(users.avg("age")).toBe(4);
    expect(users.top(2, "score").map((row) => row.id)).toEqual([8, 7]);
    expect(users.bottom(2, "score").map((row) => row.id)).toEqual([0, 1]);

    expect(users.deleteWhere("age", ">=", 0)).toEqual({ affectedRows: 9 });
    expect(users.rowCount).toBe(0);
    expect(users.isEmpty()).toBe(true);
    expect(users.toArray()).toEqual([]);
  });

  it("query delete respects limit and offset independently", () => {
    const limitUsers = createUsers();
    expect(limitUsers.where("age", ">=", 2).limit(2).delete()).toEqual({ affectedRows: 2 });
    expect(limitUsers.toArray().map((row) => row.id)).toEqual([0, 1, 4, 5, 6, 7, 8, 9, 10, 11]);

    const offsetUsers = createUsers();
    expect(offsetUsers.where("age", ">=", 2).offset(8).delete()).toEqual({ affectedRows: 2 });
    expect(offsetUsers.toArray().map((row) => row.id)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it("keeps equality and sorted indexes correct after mutations and explicit rebuilds", () => {
    const users = createUsers(30);
    users.createIndex("id").createIndex("status").createSortedIndex("age");

    users.updateWhere("id", "in", [5, 10, 15], { age: 25, status: "active" });
    expect(users.where("status", "=", "active").toArray()).toEqual(
      users.toArray().filter((row) => row.status === "active"),
    );
    expect(users.where("age", ">=", 25).toArray()).toEqual(
      users.toArray().filter((row) => row.age >= 25),
    );

    users.deleteWhere("age", ">=", 25);
    users.rebuildIndex("status").rebuildSortedIndex("age").rebuildIndexes();
    expect(users.where("status", "=", "active").toArray()).toEqual(
      users.toArray().filter((row) => row.status === "active"),
    );
    expect(users.where("age", ">=", 20).toArray()).toEqual(
      users.toArray().filter((row) => row.age >= 20),
    );
  });

  it("keeps indexes correct after lazy rebuild from query delete and later indexed query", () => {
    const users = createUsers(40);
    users.createIndex("status").createSortedIndex("age");

    const expectedDeleted = users.where("status", "=", "passive").limit(5).toArray().map((row) => row.id);
    expect(users.where("status", "=", "passive").limit(5).delete()).toEqual({ affectedRows: 5 });
    expect(users.toArray().some((row) => expectedDeleted.includes(row.id))).toBe(false);

    const expectedActive = users.toArray().filter((row) => row.status === "active");
    expect(users.where("status", "=", "active").toArray()).toEqual(expectedActive);
    expect(users.where("age", ">=", 30).toArray()).toEqual(users.toArray().filter((row) => row.age >= 30));
  });

  it("serializes mutated data without serializing indexes", () => {
    const users = createUsers();
    users.createIndex("status").createSortedIndex("age");
    users.updateWhere("id", "=", 2, { status: "active", age: 50 });
    users.deleteWhere("id", "=", 3);

    const restored = table.deserialize(users.serialize());
    expect(restored.toArray()).toEqual(users.toArray());
    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
  });
});

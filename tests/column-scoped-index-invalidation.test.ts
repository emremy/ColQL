import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function usersTable() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.uint32(),
    status: column.dictionary(["active", "passive", "archived"] as const),
    active: column.boolean(),
  });

  users.insertMany(
    Array.from({ length: 80 }, (_unused, id) => ({
      id,
      age: (id * 7) % 100,
      score: id * 10,
      status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
      active: id % 2 === 0,
    })),
  );

  return users
    .createUniqueIndex("id")
    .createIndex("id")
    .createIndex("status")
    .createSortedIndex("age");
}

describe("column-scoped index invalidation", () => {
  it("does not dirty equality or sorted indexes when updating an unrelated column", () => {
    const users = usersTable();

    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );

    expect(users.updateMany({ status: "active" }, { score: 999 })).toEqual({
      affectedRows: 27,
    });

    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
  });

  it("dirties only equality indexes for updated equality-indexed columns", () => {
    const users = usersTable();

    users.updateMany({ status: "active" }, { status: "passive" });

    expect(users.where("status", "=", "passive").explain()).toEqual(
      expect.objectContaining({
        selectedIndex: "equality:status",
        indexState: "dirty",
        reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
      }),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ selectedIndex: "sorted:age", indexState: "fresh" }),
    );
    expect(users.where("status", "=", "passive").toArray()).toEqual(
      users.toArray().filter((row) => row.status === "passive"),
    );
    expect(users.where("status", "=", "passive").explain()).toEqual(
      expect.objectContaining({ reasonCode: "INDEX_CANDIDATE_SET_TOO_LARGE" }),
    );
  });

  it("dirties only sorted indexes for updated sorted-indexed columns", () => {
    const users = usersTable();

    users.updateMany({ status: "passive" }, { age: 99 });

    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({
        selectedIndex: "sorted:age",
        indexState: "dirty",
        reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
      }),
    );
    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({ selectedIndex: "equality:status", indexState: "fresh" }),
    );
    expect(users.where("age", ">=", 95).toArray()).toEqual(
      users.toArray().filter((row) => row.age >= 95),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
  });

  it("does not dirty indexes for no-match or failed updates", () => {
    const users = usersTable();
    const before = users.toArray();

    expect(users.updateMany({ id: 999_999 }, { status: "archived" })).toEqual({
      affectedRows: 0,
    });
    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );

    expect(() => users.updateMany({ status: "active" }, { age: 300 })).toThrow(
      ColQLError,
    );
    expect(users.toArray()).toEqual(before);
    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ indexState: "fresh" }),
    );
  });

  it("keeps delete behavior broad because row positions shift", () => {
    const users = usersTable();

    users.deleteMany({ status: "archived", age: { lt: 50 } });

    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({ indexState: "dirty" }),
    );
    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({ indexState: "dirty" }),
    );
    expect(users.findBy("id", 10)).toEqual(
      users.toArray().find((row) => row.id === 10),
    );
  });
});

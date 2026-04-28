import { describe, expect, it } from "vitest";
import { column, table } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  is_active: column.boolean(),
};

function createUsers(count = 60) {
  const users = table(schema);
  for (let id = 0; id < count; id += 1) {
    users.insert({
      id,
      age: id % 50,
      status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
      is_active: id % 2 === 0,
    });
  }
  return users;
}

describe("delete index maintenance", () => {
  it("rebuilds equality indexes lazily after delete", () => {
    const users = createUsers();
    users.createIndex("id").createIndex("status");

    users.delete(10);
    users.delete(0);

    const expectedById = users.toArray().filter((row) => row.id === 11);
    expect(users.where("id", "=", 11).toArray()).toEqual(expectedById);
    expect(users.where("id", "=", 10).toArray()).toEqual([]);

    const expectedActive = users.toArray().filter((row) => row.status === "active");
    expect(users.where("status", "=", "active").toArray()).toEqual(expectedActive);
  });

  it("rebuilds sorted indexes lazily after delete", () => {
    const users = createUsers();
    users.createSortedIndex("age");

    users.delete(3);
    users.delete(20);

    const scan = users.toArray().filter((row) => row.age >= 40);
    expect(users.where("age", ">=", 40).toArray()).toEqual(scan);
  });

  it("keeps indexes correct after deleteMany-like loops and later inserts", () => {
    const users = createUsers(80);
    users.createIndex("id").createIndex("status").createSortedIndex("age");

    for (const index of [70, 40, 5, 0]) {
      users.delete(index);
    }

    users.insert({ id: 999, age: 42, status: "active", is_active: true });

    expect(users.where("id", "=", 999).toArray()).toEqual([{ id: 999, age: 42, status: "active", is_active: true }]);
    expect(users.where("age", ">=", 42).where("status", "=", "active").toArray()).toEqual(
      users.toArray().filter((row) => row.age >= 42 && row.status === "active"),
    );
  });

  it("queries still work after dropping indexes post-delete", () => {
    const users = createUsers();
    users.createIndex("status").createSortedIndex("age");
    users.delete(12);

    const indexed = users.where("status", "=", "passive").where("age", "<", 25).toArray();
    users.dropIndex("status").dropSortedIndex("age");

    expect(users.where("status", "=", "passive").where("age", "<", 25).toArray()).toEqual(indexed);
  });
});

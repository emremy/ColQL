import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function usersFixture() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive", "blocked"] as const),
    is_active: column.boolean(),
  });

  users.insert({ id: 1, age: 17, status: "active", is_active: true });
  users.insert({ id: 2, age: 18, status: "passive", is_active: false });
  users.insert({ id: 3, age: 30, status: "active", is_active: true });
  users.insert({ id: 4, age: 40, status: "blocked", is_active: false });
  return users;
}

describe("query where", () => {
  it("supports numeric comparisons", () => {
    const users = usersFixture();
    expect(users.where("age", ">=", 18).count()).toBe(3);
    expect(users.where("age", "<", 30).count()).toBe(2);
  });

  it("supports dictionary equality and inequality", () => {
    const users = usersFixture();
    expect(users.where("status", "=", "active").count()).toBe(2);
    expect(users.where("status", "!=", "active").count()).toBe(2);
  });

  it("supports boolean equality", () => {
    const users = usersFixture();
    expect(users.where("is_active", "=", true).count()).toBe(2);
  });

  it("supports chained where conditions", () => {
    const users = usersFixture();
    expect(users.where("age", ">", 18).where("status", "=", "active").toArray()).toEqual([
      { id: 3, age: 30, status: "active", is_active: true },
    ]);
  });

  it("supports object predicates as existing where conditions", () => {
    const users = usersFixture();

    expect(users.where({ age: { gt: 25 }, is_active: true }).toArray()).toEqual(
      users.where("age", ">", 25).where("is_active", "=", true).toArray(),
    );
    expect(users.where({ age: { gte: 18, lt: 40 }, status: { in: ["active", "passive"] } }).toArray()).toEqual(
      users
        .where("age", ">=", 18)
        .where("age", "<", 40)
        .where("status", "in", ["active", "passive"])
        .toArray(),
    );
    expect(users.where({ status: { eq: "blocked" } }).first()).toEqual(users.where("status", "=", "blocked").first());
  });

  it("supports object predicates on query chains", () => {
    const users = usersFixture();

    expect(users.where("age", ">=", 18).where({ status: "active", is_active: true }).toArray()).toEqual([
      { id: 3, age: 30, status: "active", is_active: true },
    ]);
  });

  it("preserves index planning by translating object predicates to existing filters", () => {
    const users = usersFixture();
    users.createIndex("status").createSortedIndex("age");

    expect(users.where({ status: "active" }).__debugPlan()).toEqual(users.where("status", "=", "active").__debugPlan());
    expect(users.where({ age: { gt: 25 } }).__debugPlan()).toEqual(users.where("age", ">", 25).__debugPlan());
  });

  it("supports in and not in operators", () => {
    const users = usersFixture();
    expect(users.where("age", "in", [17, 30]).count()).toBe(2);
    expect(users.where("status", "not in", ["blocked"]).count()).toBe(3);
  });
});

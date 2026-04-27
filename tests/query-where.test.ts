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

  it("supports in and not in operators", () => {
    const users = usersFixture();
    expect(users.where("age", "in", [17, 30]).count()).toBe(2);
    expect(users.where("status", "not in", ["blocked"]).count()).toBe(3);
  });
});

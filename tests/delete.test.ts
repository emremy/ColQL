import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function makeUsers(count = 12) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    status: column.dictionary(["active", "passive", "archived"] as const),
    is_active: column.boolean(),
  });

  for (let i = 0; i < count; i += 1) {
    users.insert({
      id: i,
      age: i % 100,
      score: i + 0.5,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

describe("physical delete", () => {
  it("deletes first, middle, and last rows while preserving logical order", () => {
    const users = makeUsers();
    const baseline = users.toArray();

    baseline.splice(0, 1);
    users.delete(0);
    expect(users.toArray()).toEqual(baseline);

    baseline.splice(4, 1);
    users.delete(4);
    expect(users.toArray()).toEqual(baseline);

    baseline.splice(baseline.length - 1, 1);
    users.delete(users.rowCount - 1);
    expect(users.toArray()).toEqual(baseline);
  });

  it("deletes all rows one by one and updates count helpers", () => {
    const users = makeUsers(5);

    while (!users.isEmpty()) {
      users.delete(0);
    }

    expect(users.rowCount).toBe(0);
    expect(users.count()).toBe(0);
    expect(users.size()).toBe(0);
    expect(users.toArray()).toEqual([]);
  });

  it("throws ColQLError for invalid row indexes", () => {
    const users = makeUsers(3);

    expect(() => users.delete(3)).toThrow(ColQLError);
    expect(() => users.delete(-1)).toThrow(ColQLError);
    expect(() => users.delete(1.5)).toThrow(ColQLError);
  });

  it("keeps query behavior correct after delete", () => {
    const users = makeUsers(20);
    const baseline = users.toArray();

    baseline.splice(3, 1);
    users.delete(3);

    expect(users.where("age", ">", 10).toArray()).toEqual(baseline.filter((row) => row.age > 10));
    expect(users.where("status", "=", "active").select(["id", "status"]).toArray()).toEqual(
      baseline.filter((row) => row.status === "active").map((row) => ({ id: row.id, status: row.status })),
    );
    expect(users.offset(2).limit(3).toArray()).toEqual(baseline.slice(2, 5));
    expect(users.where("age", ">", 5).first()).toEqual(baseline.find((row) => row.age > 5));
    expect([...users.where("id", "<", 3)]).toEqual(baseline.filter((row) => row.id < 3));
    expect(users.sum("age")).toBe(baseline.reduce((total, row) => total + row.age, 0));
    expect(users.top(2, "score")).toEqual([...baseline].sort((a, b) => b.score - a.score).slice(0, 2));
    expect(users.bottom(2, "score")).toEqual([...baseline].sort((a, b) => a.score - b.score).slice(0, 2));
  });

  it("supports insert after delete in logical order", () => {
    const users = makeUsers(4);
    users.delete(1);
    users.insert({ id: 99, age: 42, score: 99.5, status: "active", is_active: true });

    expect(users.toArray().map((row) => row.id)).toEqual([0, 2, 3, 99]);
    expect(users.where("id", "=", 99).first()).toEqual({ id: 99, age: 42, score: 99.5, status: "active", is_active: true });
  });
});

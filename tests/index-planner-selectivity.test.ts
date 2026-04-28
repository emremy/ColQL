import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function usersTable() {
  const users = table({
    id: column.uint32(),
    status: column.dictionary(["active", "passive"] as const),
    age: column.uint8(),
  });

  for (let i = 0; i < 100; i += 1) {
    users.insert({
      id: i,
      status: i < 70 ? "active" : "passive",
      age: i % 100,
    });
  }

  return users;
}

describe("index planner selectivity", () => {
  it("uses index for high-selectivity equality", () => {
    const users = usersTable();
    const expected = users.where("id", "=", 42).toArray();
    users.createIndex("id");

    const query = users.where("id", "=", 42);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      column: "id",
      operator: "=",
      candidateCount: 1,
      rowCount: 100,
      threshold: 0.4,
    }));
    expect(query.toArray()).toEqual(expected);
  });

  it("falls back for low-selectivity equality", () => {
    const users = usersTable();
    const expected = users.where("status", "=", "active").toArray();
    users.createIndex("status");

    const query = users.where("status", "=", "active");
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "scan",
      candidateCount: 70,
      rowCount: 100,
      threshold: 0.4,
    }));
    expect(query.toArray()).toEqual(expected);
  });

  it("falls back for in covering all rows", () => {
    const users = usersTable();
    const expected = users.whereIn("status", ["active", "passive"]).toArray();
    users.createIndex("status");

    const query = users.whereIn("status", ["active", "passive"]);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "scan",
      candidateCount: 100,
      rowCount: 100,
      threshold: 0.4,
    }));
    expect(query.toArray()).toEqual(expected);
  });

  it("uses index for selective in", () => {
    const users = usersTable();
    const expected = users.where("id", "in", [10, 20, 30]).toArray();
    users.createIndex("id");

    const query = users.where("id", "in", [10, 20, 30]);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      column: "id",
      operator: "in",
      candidateCount: 3,
      rowCount: 100,
      threshold: 0.4,
    }));
    expect(query.toArray()).toEqual(expected);
  });

  it("uses index for zero candidates", () => {
    const users = usersTable();
    users.createIndex("id");

    const query = users.where("id", "=", 999_999);
    expect(query.__debugPlan()).toEqual(expect.objectContaining({
      mode: "index",
      column: "id",
      candidateCount: 0,
      rowCount: 100,
      threshold: 0.4,
    }));
    expect(query.toArray()).toEqual([]);
  });
});

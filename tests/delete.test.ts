import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.float64(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  is_active: column.boolean(),
};

function row(id: number) {
  return {
    id,
    age: id % 100,
    score: id + 0.5,
    status: id % 3 === 0 ? "active" as const : id % 3 === 1 ? "passive" as const : "archived" as const,
    is_active: id % 2 === 0,
  };
}

function usersTable(count = 12) {
  const users = table(schema);
  for (let id = 0; id < count; id += 1) {
    users.insert(row(id));
  }
  return users;
}

describe("delete", () => {
  it("deletes first, middle, and last rows while preserving logical order", () => {
    const users = usersTable(8);
    const baseline = users.toArray();

    users.delete(0);
    baseline.splice(0, 1);
    expect(users.toArray()).toEqual(baseline);
    expect(users.rowCount).toBe(7);
    expect(users.count()).toBe(7);
    expect(users.size()).toBe(7);

    users.delete(3);
    baseline.splice(3, 1);
    expect(users.toArray()).toEqual(baseline);

    users.delete(users.rowCount - 1);
    baseline.splice(baseline.length - 1, 1);
    expect(users.toArray()).toEqual(baseline);
  });

  it("deletes all rows one by one", () => {
    const users = usersTable(5);

    while (!users.isEmpty()) {
      users.delete(0);
    }

    expect(users.rowCount).toBe(0);
    expect(users.count()).toBe(0);
    expect(users.toArray()).toEqual([]);
  });

  it("throws ColQLError for invalid row indexes", () => {
    const users = usersTable(2);

    expect(() => users.delete(-1)).toThrow(ColQLError);
    expect(() => users.delete(2)).toThrow(ColQLError);
    expect(() => users.delete(0.5)).toThrow(ColQLError);

    try {
      users.delete(2);
    } catch (error) {
      expect((error as ColQLError).code).toBe("COLQL_INVALID_ROW_INDEX");
      expect((error as Error).message).toMatch(/Invalid row index/);
    }
  });

  it("keeps query, select, offset, first, iteration, aggregation, and top/bottom correct after delete", () => {
    const users = usersTable(20);
    const baseline = users.toArray();

    users.delete(4);
    baseline.splice(4, 1);
    users.delete(10);
    baseline.splice(10, 1);

    expect(users.where("status", "=", "active").toArray()).toEqual(
      baseline.filter((item) => item.status === "active"),
    );
    expect(users.where("age", ">=", 10).select(["id", "age"]).limit(3).toArray()).toEqual(
      baseline.filter((item) => item.age >= 10).slice(0, 3).map(({ id, age }) => ({ id, age })),
    );
    expect(users.offset(5).limit(2).toArray()).toEqual(baseline.slice(5, 7));
    expect(users.where("is_active", "=", false).first()).toEqual(baseline.find((item) => !item.is_active));
    expect([...users.where("id", "<", 3)]).toEqual(baseline.filter((item) => item.id < 3));
    expect(users.sum("age")).toBe(baseline.reduce((sum, item) => sum + item.age, 0));
    expect(users.top(3, "score")).toEqual([...baseline].sort((a, b) => b.score - a.score).slice(0, 3));
    expect(users.bottom(3, "score")).toEqual([...baseline].sort((a, b) => a.score - b.score).slice(0, 3));
  });

  it("supports insert after delete", () => {
    const users = usersTable(4);
    users.delete(1);
    users.insert(row(99));

    expect(users.toArray().map((item) => item.id)).toEqual([0, 2, 3, 99]);
    expect(users.where("id", "=", 99).first()).toEqual(row(99));
  });
});

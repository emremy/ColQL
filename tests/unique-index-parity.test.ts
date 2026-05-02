import { describe, expect, it } from "vitest";
import { column, table, type RowForSchema } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive", "archived"] as const),
};

type User = RowForSchema<typeof schema>;

function seedRows(count: number): User[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    age: (id * 7) % 100,
    score: (id * 13) % 1_000,
    status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
  }));
}

describe("unique index parity", () => {
  it("matches plain array oracle across mutation and by-key sequences", () => {
    const initial = seedRows(120);
    const users = table(schema).insertMany(initial).createUniqueIndex("id").createIndex("status").createSortedIndex("age");
    const oracle = initial.map((row) => ({ ...row }));

    users.updateMany({ status: "active", age: { gte: 40 } }, { score: 999 });
    for (const row of oracle) {
      if (row.status === "active" && row.age >= 40) {
        row.score = 999;
      }
    }

    users.deleteMany({ status: "archived", age: { lt: 50 } });
    for (let index = oracle.length - 1; index >= 0; index -= 1) {
      if (oracle[index].status === "archived" && oracle[index].age < 50) {
        oracle.splice(index, 1);
      }
    }

    users.insert({ id: 500, age: 50, score: 500, status: "passive" });
    oracle.push({ id: 500, age: 50, score: 500, status: "passive" });

    expect(users.findBy("id", 500)).toEqual(oracle.find((row) => row.id === 500));
    expect(users.updateBy("id", 500, { age: 51 })).toEqual({ affectedRows: 1 });
    oracle.find((row) => row.id === 500)!.age = 51;

    expect(users.deleteBy("id", 1)).toEqual({ affectedRows: oracle.some((row) => row.id === 1) ? 1 : 0 });
    const deleteIndex = oracle.findIndex((row) => row.id === 1);
    if (deleteIndex >= 0) {
      oracle.splice(deleteIndex, 1);
    }

    expect(users.toArray()).toEqual(oracle);
    expect(users.where("status", "=", "passive").where("age", ">=", 30).toArray()).toEqual(
      oracle.filter((row) => row.status === "passive" && row.age >= 30),
    );
    expect(users.countWhere("score", "=", 999)).toBe(oracle.filter((row) => row.score === 999).length);
  });
});

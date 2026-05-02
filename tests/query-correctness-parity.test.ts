import { describe, expect, it } from "vitest";
import { column, table, type RowForSchema } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  active: column.boolean(),
};

type User = RowForSchema<typeof schema>;

function seedRows(count = 80): User[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    age: (id * 7) % 90,
    score: (id * 13) % 1_000,
    status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
    active: id % 4 !== 0,
  }));
}

function createUsers(rows = seedRows()) {
  const users = table(schema);
  users.insertMany(rows);
  return users;
}

function ids(rows: readonly User[]): number[] {
  return rows.map((row) => row.id);
}

describe("query correctness parity", () => {
  it("returns identical equality-index and scan results in logical row order", () => {
    const scan = createUsers();
    const indexed = createUsers();
    indexed.createIndex("id").createIndex("status");

    expect(indexed.where("status", "=", "active").toArray()).toEqual(scan.where("status", "=", "active").toArray());
    expect(indexed.where("id", "in", [3, 25, 72]).toArray()).toEqual(scan.where("id", "in", [3, 25, 72]).toArray());
    expect(indexed.where("status", "=", "archived").where("age", ">=", 40).toArray()).toEqual(
      scan.where("status", "=", "archived").where("age", ">=", 40).toArray(),
    );
    expect(indexed.where("id", "=", 999).toArray()).toEqual([]);
    expect(ids(indexed.where("status", "=", "active").toArray())).toEqual(ids(seedRows().filter((row) => row.status === "active")));
  });

  it("returns identical sorted-index and scan results for ranges", () => {
    const scan = createUsers();
    const indexed = createUsers();
    indexed.createSortedIndex("age").createSortedIndex("score");

    expect(indexed.where("age", ">", 70).toArray()).toEqual(scan.where("age", ">", 70).toArray());
    expect(indexed.where("age", ">=", 35).toArray()).toEqual(scan.where("age", ">=", 35).toArray());
    expect(indexed.where("score", "<", 100).toArray()).toEqual(scan.where("score", "<", 100).toArray());
    expect(indexed.where("score", "<=", 250).where("status", "!=", "archived").toArray()).toEqual(
      scan.where("score", "<=", 250).where("status", "!=", "archived").toArray(),
    );
    expect(indexed.where("age", ">", 250).toArray()).toEqual([]);
  });

  it("returns identical results with equality and sorted indexes together", () => {
    const scan = createUsers();
    const indexed = createUsers();
    indexed.createIndex("id").createIndex("status").createSortedIndex("age").createSortedIndex("score");

    const expected = scan.where("status", "=", "passive").where("age", ">=", 30).where("score", "<", 700).toArray();
    const actual = indexed.where("score", "<", 700).where("age", ">=", 30).where("status", "=", "passive").toArray();

    expect(actual).toEqual(expected);
    expect(ids(actual)).toEqual([...ids(actual)].sort((left, right) => left - right));
  });

  it("keeps dirty indexes correct after updateMany, deleteMany, and insertMany", () => {
    const rows = seedRows();
    const users = createUsers(rows);
    users.createIndex("id").createIndex("status").createSortedIndex("age").createSortedIndex("score");

    const oracle = rows.map((row) => ({ ...row }));

    const updateStatus = users.updateMany({ status: "passive" }, { status: "active" });
    for (const row of oracle) {
      if (row.status === "passive") {
        row.status = "active";
      }
    }
    expect(updateStatus.affectedRows).toBe(27);
    expect(users.where("status", "=", "active").toArray()).toEqual(oracle.filter((row) => row.status === "active"));

    const updateAge = users.updateMany({ id: { in: [5, 12, 47] } }, { age: 88 });
    for (const row of oracle) {
      if ([5, 12, 47].includes(row.id)) {
        row.age = 88;
      }
    }
    expect(updateAge.affectedRows).toBe(3);
    expect(users.where("age", ">=", 80).toArray()).toEqual(oracle.filter((row) => row.age >= 80));

    const deleted = users.deleteMany({ status: "archived", age: { lt: 50 } });
    for (let index = oracle.length - 1; index >= 0; index -= 1) {
      if (oracle[index].status === "archived" && oracle[index].age < 50) {
        oracle.splice(index, 1);
      }
    }
    expect(deleted.affectedRows).toBeGreaterThan(0);
    expect(users.where("status", "=", "archived").where("age", "<", 50).toArray()).toEqual([]);
    expect(users.where("status", "=", "active").toArray()).toEqual(oracle.filter((row) => row.status === "active"));

    const inserted: User[] = [
      { id: 1_001, age: 21, score: 901, status: "active", active: true },
      { id: 1_002, age: 77, score: 902, status: "archived", active: false },
    ];
    users.insertMany(inserted);
    oracle.push(...inserted);

    expect(users.where("id", "in", [1_001, 1_002]).toArray()).toEqual(inserted);
    expect(users.where("age", ">=", 75).toArray()).toEqual(oracle.filter((row) => row.age >= 75));
  });

  it("returns deterministic results across repeated equivalent queries", () => {
    const users = createUsers();
    users.createIndex("status").createSortedIndex("age");

    const query = () => users.where("status", "=", "active").where("age", ">=", 30).where("age", "<", 80).toArray();
    const first = query();

    expect(query()).toEqual(first);
    expect(query()).toEqual(first);
    expect(users.where("age", "<", 80).where("status", "=", "active").where("age", ">=", 30).toArray()).toEqual(first);
  });

  it("keeps mutation and query sequences equal to a plain array oracle", () => {
    const users = createUsers(seedRows(120));
    users.createIndex("status").createIndex("id").createSortedIndex("age");
    const oracle = seedRows(120).map((row) => ({ ...row }));

    users.updateMany({ status: "active", age: { gte: 40 } }, { score: 999 });
    for (const row of oracle) {
      if (row.status === "active" && row.age >= 40) {
        row.score = 999;
      }
    }

    users.deleteMany({ active: false, age: { lt: 30 } });
    for (let index = oracle.length - 1; index >= 0; index -= 1) {
      if (!oracle[index].active && oracle[index].age < 30) {
        oracle.splice(index, 1);
      }
    }

    users.insertMany([{ id: 500, age: 50, score: 500, status: "passive", active: true }]);
    oracle.push({ id: 500, age: 50, score: 500, status: "passive", active: true });

    const predicate = (row: User) => row.status !== "archived" && row.age >= 35 && [500, 999].includes(row.score);
    expect(users.where("status", "not in", ["archived"]).where("age", ">=", 35).where("score", "in", [500, 999]).toArray()).toEqual(
      oracle.filter(predicate),
    );
  });
});

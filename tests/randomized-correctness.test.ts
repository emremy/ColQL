import { describe, expect, it } from "vitest";
import { column, table, type RowForSchema } from "../src";
import type { Query } from "../src/query";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  active: column.boolean(),
};

type User = RowForSchema<typeof schema>;
type UserQuery = Query<typeof schema, User>;
type PredicateCase = {
  readonly apply: (query: UserQuery) => UserQuery;
  readonly test: (row: User) => boolean;
};

function rng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

function integer(random: () => number, maxExclusive: number): number {
  return Math.floor(random() * maxExclusive);
}

function sampleRows(random: () => number, count: number): User[] {
  const statuses: User["status"][] = ["active", "passive", "archived"];
  return Array.from({ length: count }, (_unused, index) => ({
    id: index,
    age: integer(random, 100),
    score: integer(random, 1_000),
    status: statuses[integer(random, statuses.length)],
    active: integer(random, 2) === 0,
  }));
}

function createUsers(rows: readonly User[], mode: "scan" | "equality" | "sorted" | "both") {
  const users = table(schema);
  users.insertMany(rows);

  if (mode === "equality" || mode === "both") {
    users.createIndex("id").createIndex("age").createIndex("score").createIndex("status");
  }

  if (mode === "sorted" || mode === "both") {
    users.createSortedIndex("age").createSortedIndex("score");
  }

  return users;
}

function randomPredicate(random: () => number): PredicateCase {
  const statuses: User["status"][] = ["active", "passive", "archived"];
  const choice = integer(random, 8);

  switch (choice) {
    case 0: {
      const id = integer(random, 220);
      return {
        apply: (query) => query.where("id", "=", id),
        test: (row) => row.id === id,
      };
    }
    case 1: {
      const status = statuses[integer(random, statuses.length)];
      return {
        apply: (query) => query.where("status", "=", status),
        test: (row) => row.status === status,
      };
    }
    case 2: {
      const age = integer(random, 110);
      return {
        apply: (query) => query.where("age", ">=", age),
        test: (row) => row.age >= age,
      };
    }
    case 3: {
      const age = integer(random, 110);
      return {
        apply: (query) => query.where("age", "<", age),
        test: (row) => row.age < age,
      };
    }
    case 4: {
      const score = integer(random, 1_100);
      return {
        apply: (query) => query.where("score", ">", score),
        test: (row) => row.score > score,
      };
    }
    case 5: {
      const values = [integer(random, 100), integer(random, 100), integer(random, 100)];
      return {
        apply: (query) => query.where("age", "in", values),
        test: (row) => values.includes(row.age),
      };
    }
    case 6: {
      const first = statuses[integer(random, statuses.length)];
      const second = statuses[integer(random, statuses.length)];
      const values = [...new Set([first, second])];
      return {
        apply: (query) => query.where("status", "in", values),
        test: (row) => values.includes(row.status),
      };
    }
    default: {
      const active = integer(random, 2) === 0;
      return {
        apply: (query) => query.where("active", "=", active),
        test: (row) => row.active === active,
      };
    }
  }
}

describe("randomized query correctness", () => {
  it("matches plain array filtering across scan and index modes", () => {
    const random = rng(0xC01C);
    const rows = sampleRows(random, 160);

    for (let iteration = 0; iteration < 100; iteration += 1) {
      const predicateCount = 1 + integer(random, 3);
      const predicates = Array.from({ length: predicateCount }, () => randomPredicate(random));
      const expected = rows.filter((row) => predicates.every((predicate) => predicate.test(row)));

      for (const mode of ["scan", "equality", "sorted", "both"] as const) {
        const users = createUsers(rows, mode);
        const query = predicates.reduce((next, predicate) => predicate.apply(next), users.query());
        expect(query.toArray()).toEqual(expected);
      }
    }
  });
});

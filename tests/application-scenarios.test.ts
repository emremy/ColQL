import { describe, expect, it } from "vitest";
import { ColQLError, column, table, type RowForSchema } from "../src";

function expectCode(fn: () => unknown, code: string): void {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
  }
}

describe("application scenarios", () => {
  it("supports a user directory with stable id and email lookups", () => {
    const schema = {
      id: column.uint32(),
      email: column.dictionary(["a@example.com", "b@example.com", "c@example.com", "d@example.com", "e@example.com"] as const),
      country: column.dictionary(["US", "TR", "DE"] as const),
      age: column.uint8(),
      active: column.boolean(),
      score: column.uint32(),
    };
    type User = RowForSchema<typeof schema>;
    const rows: User[] = [
      { id: 1, email: "a@example.com", country: "US", age: 21, active: true, score: 90 },
      { id: 2, email: "b@example.com", country: "TR", age: 34, active: true, score: 70 },
      { id: 3, email: "c@example.com", country: "US", age: 42, active: false, score: 50 },
      { id: 4, email: "d@example.com", country: "DE", age: 28, active: true, score: 80 },
    ];
    const users = table(schema)
      .insertMany(rows)
      .createUniqueIndex("id")
      .createUniqueIndex("email")
      .createIndex("country")
      .createSortedIndex("age");
    const oracle = rows.map((row) => ({ ...row }));

    expect(users.findBy("id", 2)).toEqual(oracle.find((row) => row.id === 2));
    expect(users.findBy("email", "c@example.com")).toEqual(oracle.find((row) => row.email === "c@example.com"));

    expect(users.updateBy("email", "b@example.com", { active: false, score: 75 })).toEqual({ affectedRows: 1 });
    Object.assign(oracle.find((row) => row.email === "b@example.com")!, { active: false, score: 75 });

    expect(users.deleteBy("id", 3)).toEqual({ affectedRows: 1 });
    oracle.splice(oracle.findIndex((row) => row.id === 3), 1);

    users.insert({ id: 3, email: "c@example.com", country: "TR", age: 37, active: true, score: 95 });
    oracle.push({ id: 3, email: "c@example.com", country: "TR", age: 37, active: true, score: 95 });

    expectCode(() => users.insert({ id: 5, email: "a@example.com", country: "US", age: 22, active: true, score: 1 }), "COLQL_DUPLICATE_KEY");
    expect(users.where({ country: "TR", age: { gte: 30 } }).select(["id", "email", "score"]).toArray()).toEqual(
      oracle
        .filter((row) => row.country === "TR" && row.age >= 30)
        .map((row) => ({ id: row.id, email: row.email, score: row.score })),
    );
  });

  it("supports a product catalog with SKU uniqueness", () => {
    const schema = {
      sku: column.dictionary(["SKU-1", "SKU-2", "SKU-3", "SKU-4", "SKU-5"] as const),
      category: column.dictionary(["books", "games", "tools"] as const),
      price: column.uint32(),
      stock: column.uint16(),
      active: column.boolean(),
    };
    type Product = RowForSchema<typeof schema>;
    const rows: Product[] = [
      { sku: "SKU-1", category: "books", price: 1500, stock: 10, active: true },
      { sku: "SKU-2", category: "games", price: 6000, stock: 0, active: false },
      { sku: "SKU-3", category: "books", price: 2500, stock: 2, active: true },
      { sku: "SKU-4", category: "tools", price: 4000, stock: 5, active: true },
    ];
    const products = table(schema).insertMany(rows).createUniqueIndex("sku").createIndex("category").createSortedIndex("price");
    const oracle = rows.map((row) => ({ ...row }));

    expect(products.findBy("sku", "SKU-3")).toEqual(oracle.find((row) => row.sku === "SKU-3"));
    expect(products.where({ category: "books", price: { gte: 2000, lte: 3000 } }).toArray()).toEqual(
      oracle.filter((row) => row.category === "books" && row.price >= 2000 && row.price <= 3000),
    );

    expect(products.updateBy("sku", "SKU-1", { stock: 9 })).toEqual({ affectedRows: 1 });
    oracle.find((row) => row.sku === "SKU-1")!.stock = 9;

    expect(products.deleteMany({ active: false, stock: 0 })).toEqual({ affectedRows: 1 });
    oracle.splice(oracle.findIndex((row) => !row.active && row.stock === 0), 1);

    products.insert({ sku: "SKU-2", category: "games", price: 5500, stock: 3, active: true });
    oracle.push({ sku: "SKU-2", category: "games", price: 5500, stock: 3, active: true });

    const before = products.toArray();
    expectCode(
      () =>
        products.insertMany([
          { sku: "SKU-5", category: "tools", price: 1000, stock: 1, active: true },
          { sku: "SKU-5", category: "books", price: 1100, stock: 2, active: true },
        ]),
      "COLQL_DUPLICATE_KEY",
    );
    expect(products.toArray()).toEqual(before);
    expect(products.toArray()).toEqual(oracle);
  });

  it("supports a session token registry", () => {
    const schema = {
      token: column.dictionary(["t1", "t2", "t3", "t4", "t5"] as const),
      userId: column.uint32(),
      expiresAt: column.uint32(),
      revoked: column.boolean(),
    };
    const sessions = table(schema)
      .insertMany([
        { token: "t1", userId: 1, expiresAt: 100, revoked: false },
        { token: "t2", userId: 1, expiresAt: 200, revoked: false },
        { token: "t3", userId: 2, expiresAt: 50, revoked: false },
      ])
      .createUniqueIndex("token")
      .createIndex("userId")
      .createSortedIndex("expiresAt");

    expect(sessions.findBy("token", "t2")).toEqual({ token: "t2", userId: 1, expiresAt: 200, revoked: false });
    expect(sessions.updateBy("token", "t2", { revoked: true })).toEqual({ affectedRows: 1 });
    expect(sessions.findBy("token", "t2")).toEqual({ token: "t2", userId: 1, expiresAt: 200, revoked: true });

    expect(sessions.deleteMany({ expiresAt: { lt: 100 } })).toEqual({ affectedRows: 1 });
    sessions.insert({ token: "t3", userId: 3, expiresAt: 300, revoked: false });
    expect(sessions.findBy("token", "t3")).toEqual({ token: "t3", userId: 3, expiresAt: 300, revoked: false });
    expectCode(() => sessions.insert({ token: "t1", userId: 9, expiresAt: 999, revoked: false }), "COLQL_DUPLICATE_KEY");
  });

  it("supports a feature flag rule table", () => {
    const schema = {
      key: column.dictionary(["checkout", "search", "profile", "billing"] as const),
      environment: column.dictionary(["dev", "staging", "prod"] as const),
      enabled: column.boolean(),
      rollout: column.uint8(),
    };
    const rows = [
      { key: "checkout", environment: "prod", enabled: true, rollout: 25 },
      { key: "search", environment: "prod", enabled: false, rollout: 0 },
      { key: "profile", environment: "staging", enabled: true, rollout: 100 },
    ] as const;
    const flags = table(schema).insertMany(rows).createUniqueIndex("key").createIndex("environment");
    const oracle = rows.map((row) => ({ ...row }));

    expect(flags.findBy("key", "checkout")).toEqual(oracle.find((row) => row.key === "checkout"));
    expect(flags.updateBy("key", "search", { enabled: true, rollout: 10 })).toEqual({ affectedRows: 1 });
    Object.assign(oracle.find((row) => row.key === "search")!, { enabled: true, rollout: 10 });

    expect(flags.where({ environment: "prod", enabled: true }).toArray()).toEqual(
      oracle.filter((row) => row.environment === "prod" && row.enabled),
    );
    expectCode(() => flags.createUniqueIndex("enabled" as never), "COLQL_UNIQUE_INDEX_UNSUPPORTED");
  });

  it("keeps a mixed long operation sequence equal to a JS array oracle", () => {
    const schema = {
      id: column.uint32(),
      age: column.uint8(),
      score: column.uint32(),
      status: column.dictionary(["active", "passive", "archived"] as const),
      active: column.boolean(),
    };
    type User = RowForSchema<typeof schema>;
    const seedRows = (start: number, count: number): User[] =>
      Array.from({ length: count }, (_unused, offset) => {
        const id = start + offset;
        return {
          id,
          age: (id * 7) % 100,
          score: (id * 11) % 1_000,
          status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
          active: id % 4 !== 0,
        };
      });
    const rows = seedRows(0, 2_000);
    let users = table(schema).insertMany(rows).createUniqueIndex("id").createIndex("status").createSortedIndex("age");
    const oracle = rows.map((row) => ({ ...row }));
    const expectParity = () => {
      expect(users.where({ status: "active", age: { gte: 50 } }).toArray()).toEqual(
        oracle.filter((row) => row.status === "active" && row.age >= 50),
      );
      expect(users.where("id", "in", [10, 500, 1_500, 2_100]).toArray()).toEqual(
        oracle.filter((row) => [10, 500, 1_500, 2_100].includes(row.id)),
      );
      expect(users.countWhere({ active: true })).toBe(oracle.filter((row) => row.active).length);
    };

    expectParity();
    users.updateMany({ status: "passive", age: { gte: 40 } }, { status: "active", score: 999 });
    for (const row of oracle) {
      if (row.status === "passive" && row.age >= 40) Object.assign(row, { status: "active", score: 999 });
    }
    expectParity();

    users.updateBy("id", 123, { age: 88 });
    oracle.find((row) => row.id === 123)!.age = 88;
    expectParity();

    users.deleteMany({ status: "archived", age: { lt: 30 } });
    for (let index = oracle.length - 1; index >= 0; index -= 1) {
      if (oracle[index].status === "archived" && oracle[index].age < 30) oracle.splice(index, 1);
    }
    expectParity();

    const inserted = seedRows(2_000, 100);
    users.insertMany(inserted);
    oracle.push(...inserted.map((row) => ({ ...row })));
    users.rebuildIndexes().rebuildUniqueIndexes();
    expectParity();

    users = table.deserialize(users.serialize()).createUniqueIndex("id").createIndex("status").createSortedIndex("age") as typeof users;
    expect(users.toArray()).toEqual(oracle);
    expectParity();
  });

  it("handles unique-index edge cases", () => {
    const schema = {
      id: column.uint32(),
      status: column.dictionary(["active", "passive"] as const),
    };

    const empty = table(schema).createUniqueIndex("id");
    expect(empty.findBy("id", 1)).toBeUndefined();
    empty.insert({ id: 1, status: "active" });
    expect(empty.updateBy("id", 1, { id: 1 })).toEqual({ affectedRows: 1 });
    expectCode(() => empty.insert({ id: 2, status: "passive" }).updateBy("id", 1, { id: 2 }), "COLQL_DUPLICATE_KEY");

    empty.deleteMany({ id: { in: [1, 2] } });
    expect(empty.toArray()).toEqual([]);
    empty.insert({ id: 1, status: "passive" });
    expect(empty.findBy("id", 1)).toEqual({ id: 1, status: "passive" });

    const duplicates = table(schema).insertMany([
      { id: 1, status: "active" },
      { id: 2, status: "passive" },
    ]);
    duplicates.createUniqueIndex("id").dropUniqueIndex("id");
    duplicates.insert({ id: 1, status: "passive" });
    expect(duplicates.where("id", "=", 1).count()).toBe(2);
    expectCode(() => duplicates.createUniqueIndex("id"), "COLQL_DUPLICATE_KEY");
  });
});

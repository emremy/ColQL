import { describe, expect, it } from "vitest";
import { column, table, type QueryExplainPlan } from "../src";

function usersFixture(count = 100) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive"] as const),
    active: column.boolean(),
  });

  for (let id = 0; id < count; id += 1) {
    users.insert({
      id,
      age: id % 100,
      status: id < 70 ? "active" : "passive",
      active: id % 2 === 0,
    });
  }

  return users;
}

describe("query explain", () => {
  it("does not execute, materialize, scan, notify, or rebuild dirty indexes", () => {
    const events: unknown[] = [];
    const users = table(
      {
        id: column.uint32(),
        age: column.uint8(),
        status: column.dictionary(["active", "passive"] as const),
      },
      { onQuery: (info) => events.push(info) },
    );

    users
      .insertMany([
        { id: 1, age: 10, status: "active" },
        { id: 2, age: 20, status: "passive" },
        { id: 3, age: 30, status: "active" },
      ])
      .createIndex("id")
      .createSortedIndex("age");
    users.deleteMany({ id: 2 });
    users.resetScanCounter();
    users.resetMaterializationCounter();
    events.length = 0;

    const equalityStatsBefore = users.indexStats();
    const sortedStatsBefore = users.sortedIndexStats();
    const explain = users.where("id", "=", 3).select(["id"]).explain();

    expect(explain).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexesUsed: ["equality:id"],
        indexState: "dirty",
        reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
        projectionPushdown: true,
      }),
    );
    expect(explain).not.toHaveProperty("candidateRows");
    expect(users.scannedRowCount).toBe(0);
    expect(users.materializedRowCount).toBe(0);
    expect(events).toEqual([]);
    expect(users.indexStats()).toEqual(equalityStatsBefore);
    expect(users.sortedIndexStats()).toEqual(sortedStatsBefore);
  });

  it("reports a selective equality index plan", () => {
    const users = usersFixture();
    users.createIndex("id");

    const explain: QueryExplainPlan = users.where("id", "=", 42).explain();

    expect(explain).toEqual({
      scanType: "index",
      indexesUsed: ["equality:id"],
      predicates: 1,
      predicateOrder: ["id ="],
      projectionPushdown: false,
      candidateRows: 1,
      indexState: "fresh",
    });
  });

  it("reports a selective sorted range index plan", () => {
    const users = usersFixture();
    users.createSortedIndex("age");

    expect(users.where("age", ">=", 95).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexesUsed: ["sorted:age"],
        candidateRows: 5,
        indexState: "fresh",
      }),
    );
  });

  it("reports predicate ordering and projection pushdown", () => {
    const users = usersFixture();
    users.createIndex("status").createSortedIndex("age");

    expect(
      users
        .where("age", ">=", 25)
        .where("status", "=", "passive")
        .select(["id", "age"])
        .explain(),
    ).toEqual(
      expect.objectContaining({
        predicates: 2,
        predicateOrder: ["status =", "age >="],
        projectionPushdown: true,
      }),
    );
  });

  it("reports callback predicates as full scans", () => {
    const users = usersFixture();
    users.createIndex("id");

    expect(
      users
        .where("id", "=", 42)
        .filter((row) => row.active)
        .explain(),
    ).toEqual(
      expect.objectContaining({
        scanType: "full",
        indexesUsed: [],
        predicates: 2,
        predicateOrder: ["id ="],
        reasonCode: "CALLBACK_PREDICATE_REQUIRES_FULL_SCAN",
      }),
    );
  });

  it("reports no predicates as a full scan", () => {
    expect(usersFixture().query().explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        indexesUsed: [],
        predicates: 0,
        predicateOrder: [],
        reasonCode: "NO_PREDICATES",
      }),
    );
  });

  it("reports missing equality indexes", () => {
    expect(usersFixture().where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        reasonCode: "NO_INDEX_FOR_COLUMN",
      }),
    );
  });

  it("reports missing sorted indexes for range predicates", () => {
    expect(usersFixture().where("age", ">=", 90).explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        reasonCode: "RANGE_QUERY_WITHOUT_SORTED_INDEX",
      }),
    );
  });

  it("reports unsupported indexed operators", () => {
    const users = usersFixture();
    users.createIndex("id");

    expect(users.where("id", "!=", 42).explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        reasonCode: "UNSUPPORTED_INDEX_OPERATOR",
      }),
    );
  });

  it("reports broad index candidates as full scans", () => {
    const users = usersFixture();
    users.createIndex("status");

    expect(users.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        candidateRows: 70,
        reasonCode: "INDEX_CANDIDATE_SET_TOO_LARGE",
      }),
    );
  });

  it("reports dirty sorted indexes as index plans without candidate rows", () => {
    const users = usersFixture();
    users.createSortedIndex("age");
    users.insert({ id: 101, age: 99, status: "passive", active: true });

    const explain = users.where("age", ">=", 95).explain();

    expect(explain).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexesUsed: ["sorted:age"],
        indexState: "dirty",
        reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
      }),
    );
    expect(explain).not.toHaveProperty("candidateRows");
    expect(users.sortedIndexStats()[0]?.dirty).toBe(true);
  });

  it("matches dirty equality index execution behavior after lazy rebuild", () => {
    const users = usersFixture();
    users.createIndex("id");
    users.updateMany({ status: "passive" }, { active: true });

    const explain = users.where("id", "=", 42).explain();

    expect(explain).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexesUsed: ["equality:id"],
        indexState: "dirty",
        reasonCode: "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION",
      }),
    );
    expect(explain).not.toHaveProperty("candidateRows");

    users.resetScanCounter();
    expect(users.where("id", "=", 42).toArray()).toEqual([
      { id: 42, age: 42, status: "active", active: true },
    ]);
    expect(users.scannedRowCount).toBe(1);
    expect(users.where("id", "=", 42).explain()).toEqual(
      expect.objectContaining({
        scanType: "index",
        indexesUsed: ["equality:id"],
        candidateRows: 1,
        indexState: "fresh",
      }),
    );
  });
});

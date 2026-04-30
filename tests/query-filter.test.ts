import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function usersFixture(count = 100) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive"] as const),
  });

  for (let id = 0; id < count; id += 1) {
    users.insert({
      id,
      age: id,
      status: id % 2 === 0 ? "active" : "passive",
    });
  }

  return users;
}

describe("query filter", () => {
  it("runs callback filters after structured predicates", () => {
    const users = usersFixture(10);
    const seenIds: number[] = [];

    const rows = users
      .where({ age: { gte: 5 } })
      .filter((row) => {
        seenIds.push(row.id);
        return row.id % 2 === 0;
      })
      .toArray();

    expect(seenIds).toEqual([5, 6, 7, 8, 9]);
    expect(rows.map((row) => row.id)).toEqual([6, 8]);
  });

  it("forces a full scan and skips index planning when callback filters are present", () => {
    const indexed = usersFixture();
    indexed.createIndex("id");
    indexed.resetScanCounter();

    expect(indexed.where("id", "=", 42).count()).toBe(1);
    expect(indexed.scannedRowCount).toBe(1);

    indexed.resetScanCounter();
    const query = indexed.where("id", "=", 42).filter((row) => row.status === "active");

    expect(query.__debugPlan()).toEqual(expect.objectContaining({ mode: "scan" }));
    expect(query.count()).toBe(1);
    expect(indexed.scannedRowCount).toBe(indexed.rowCount);
  });

  it("supports table-level callback filters as a full-scan escape hatch", () => {
    const users = usersFixture();
    users.createIndex("status");
    users.resetScanCounter();

    const query = users.filter((row) => row.status === "active" && row.id < 4);

    expect(query.__debugPlan()).toEqual(expect.objectContaining({ mode: "scan" }));
    expect(query.toArray().map((row) => row.id)).toEqual([0, 2]);
    expect(users.scannedRowCount).toBe(users.rowCount);
  });
});

import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function usersTable() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive", "blocked"] as const),
    is_active: column.boolean(),
  });

  for (let i = 0; i < 20; i += 1) {
    users.insert({
      id: i,
      age: i % 10,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "blocked",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

describe("indexing API", () => {
  it("creates, lists, reports, and drops indexes", () => {
    const users = usersTable();

    expect(users.hasIndex("status")).toBe(false);
    users.createIndex("status");

    expect(users.hasIndex("status")).toBe(true);
    expect(users.indexes()).toEqual(["status"]);
    expect(users.indexStats()).toEqual([
      expect.objectContaining({
        column: "status",
        uniqueValues: 3,
        rowCount: 20,
      }),
    ]);
    expect(users.indexStats()[0].memoryBytesApprox).toBeGreaterThan(0);

    users.dropIndex("status");
    expect(users.hasIndex("status")).toBe(false);
    expect(users.indexes()).toEqual([]);
  });

  it("keeps query results identical with and without numeric and dictionary indexes", () => {
    const users = usersTable();
    const numericScan = users.where("id", "=", 7).toArray();
    const dictionaryScan = users.where("status", "=", "active").toArray();
    const inScan = users.whereIn("status", ["active", "blocked"]).toArray();

    users.createIndex("id").createIndex("status");

    expect(users.where("id", "=", 7).toArray()).toEqual(numericScan);
    expect(users.where("status", "=", "active").toArray()).toEqual(dictionaryScan);
    expect(users.whereIn("status", ["active", "blocked"]).toArray()).toEqual(inScan);
  });

  it("queries still work after dropIndex using scan fallback", () => {
    const users = usersTable();
    users.createIndex("status");
    const indexed = users.where("status", "=", "passive").toArray();

    users.dropIndex("status");

    expect(users.where("status", "=", "passive").toArray()).toEqual(indexed);
  });
});

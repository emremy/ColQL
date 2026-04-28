import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function makeUsers(count = 50) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive", "archived"] as const),
  });
  for (let i = 0; i < count; i += 1) {
    users.insert({ id: i, age: i % 100, status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived" });
  }
  return users;
}

describe("delete index maintenance", () => {
  it("rebuilds equality indexes lazily after delete", () => {
    const users = makeUsers();
    users.createIndex("id").createIndex("status");

    users.delete(10);
    expect(users.where("id", "=", 10).toArray()).toEqual([]);
    expect(users.where("id", "=", 11).toArray()).toEqual([{ id: 11, age: 11, status: "archived" }]);

    const scan = makeUsers().toArray();
    scan.splice(10, 1);
    expect(users.where("status", "=", "active").toArray()).toEqual(scan.filter((row) => row.status === "active"));
  });

  it("keeps indexed queries correct after many deletes and inserts", () => {
    const users = makeUsers();
    users.createIndex("id");
    const baseline = users.toArray();

    for (const rowIndex of [0, 5, 10]) {
      baseline.splice(rowIndex, 1);
      users.delete(rowIndex);
    }

    baseline.push({ id: 100, age: 10, status: "active" });
    users.insert({ id: 100, age: 10, status: "active" });

    expect(users.where("id", "in", [1, 11, 100]).toArray()).toEqual(baseline.filter((row) => [1, 11, 100].includes(row.id)));
  });
});

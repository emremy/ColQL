import { describe, expect, it } from "vitest";
import { column, table } from "../src";
import { Table } from "../src/table";
import type { QueryInfo } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
};

function seed(users: { insert: (row: { id: number; age: number; status: "active" | "passive" }) => unknown }) {
  for (let id = 0; id < 10; id += 1) {
    users.insert({
      id,
      age: id,
      status: id % 2 === 0 ? "active" : "passive",
    });
  }
}

describe("onQuery", () => {
  it("reports terminal query info from table options", () => {
    const events: QueryInfo[] = [];
    const users = table(schema, { onQuery: (info) => events.push(info) });
    seed(users);
    users.createIndex("id");

    expect(events).toEqual([]);

    expect(users.where("id", "=", 4).count()).toBe(1);
    expect(events).toHaveLength(1);
    expect(events[0]).toEqual(expect.objectContaining({ rowsScanned: 1, indexUsed: true }));
    expect(events[0].duration).toBeGreaterThanOrEqual(0);

    users.where("status", "=", "active").filter((row) => row.id < 4).toArray();
    expect(events).toHaveLength(2);
    expect(events[1]).toEqual(expect.objectContaining({ rowsScanned: users.rowCount, indexUsed: false }));
  });

  it("does not instrument non-terminal query construction or streams", () => {
    const events: QueryInfo[] = [];
    const users = table(schema, { onQuery: (info) => events.push(info) });
    seed(users);

    const query = users.where({ status: "active" }).select(["id"]).limit(1);
    query.stream();

    expect(events).toEqual([]);

    expect(query.first()).toEqual({ id: 0 });
    expect(events).toHaveLength(1);
  });

  it("keeps constructor compatibility while allowing onQuery options", () => {
    const events: QueryInfo[] = [];
    const users = new Table(schema, 2, { onQuery: (info) => events.push(info) });
    seed(users);

    expect(users.capacity).toBeGreaterThanOrEqual(10);
    expect(users.where({ age: { gt: 7 } }).count()).toBe(2);
    expect(events).toHaveLength(1);
    expect(events[0].rowsScanned).toBe(users.rowCount);
  });
});

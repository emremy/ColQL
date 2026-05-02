import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function overwriteHeaderVersion(
  buffer: ArrayBuffer,
  version: string,
): ArrayBuffer {
  const source = new Uint8Array(buffer);
  const headerLength = new DataView(buffer).getUint32(8, true);
  const headerStart = 12;
  const header = new TextDecoder().decode(
    source.subarray(headerStart, headerStart + headerLength),
  );
  const meta = JSON.parse(header) as { version: string };
  meta.version = version;

  const patchedHeader = new TextEncoder().encode(JSON.stringify(meta));
  const output = new ArrayBuffer(headerStart + patchedHeader.byteLength);
  const bytes = new Uint8Array(output);
  bytes.set(source.subarray(0, headerStart), 0);
  new DataView(output).setUint32(8, patchedHeader.byteLength, true);
  bytes.set(patchedHeader, headerStart);

  return output;
}

describe("serialization", () => {
  it("serializes and deserializes numeric, dictionary, and boolean columns", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      score: column.float64(),
      status: column.dictionary(["active", "passive"] as const),
      is_active: column.boolean(),
    });

    users.insert({
      id: 1,
      age: 25,
      score: 10.5,
      status: "active",
      is_active: true,
    });
    users.insert({
      id: 2,
      age: 40,
      score: 20.25,
      status: "passive",
      is_active: false,
    });

    const buffer = users.serialize();
    const restored = table.deserialize(buffer);

    expect(restored.rowCount).toBe(users.rowCount);
    expect(restored.capacity).toBe(users.capacity);
    expect(restored.toArray()).toEqual(users.toArray());
    expect(restored.where("status", "=", "active").first()).toEqual({
      id: 1,
      age: 25,
      score: 10.5,
      status: "active",
      is_active: true,
    });
  });

  it("preserves schema metadata", () => {
    const users = table({
      id: column.uint32(),
      status: column.dictionary(["active", "passive"] as const),
      is_active: column.boolean(),
    });

    const restored = table.deserialize(users.serialize());
    expect(restored.schema.id.kind).toBe("numeric");
    expect(restored.schema.status.kind).toBe("dictionary");
    expect(restored.schema.is_active.kind).toBe("boolean");
  });

  it("throws on corrupted input", () => {
    const users = table({ id: column.uint32() });
    const bytes = new Uint8Array(users.serialize());
    bytes[0] = 0;

    expect(() => table.deserialize(bytes.buffer)).toThrow(/magic header/);
    expect(() => table.deserialize(new ArrayBuffer(2))).toThrow(/too small/);
  });

  it("throws on version mismatch", () => {
    const users = table({ id: column.uint32() });
    const patched = overwriteHeaderVersion(
      users.serialize(),
      "@colql/colql@9.9.9",
    );

    expect(() => table.deserialize(patched)).toThrow(/Unsupported ColQL/);
  });

  it("keeps query parity before and after deserialization with recreated indexes", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      score: column.uint32(),
      status: column.dictionary(["active", "passive", "archived"] as const),
      active: column.boolean(),
    });

    for (let id = 0; id < 60; id += 1) {
      users.insert({
        id,
        age: (id * 5) % 80,
        score: id * 10,
        status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
        active: id % 2 === 0,
      });
    }

    users.createIndex("status").createSortedIndex("age");
    users.updateMany({ status: "passive" }, { score: 777 });
    users.deleteMany({ active: false, age: { lt: 20 } });
    users.insertMany([
      { id: 101, age: 33, score: 500, status: "active", active: true },
      { id: 102, age: 72, score: 800, status: "archived", active: false },
    ]);

    const expectedRows = users.toArray();
    const expectedQuery = users.where({ status: { in: ["active", "archived"] }, age: { gte: 30, lt: 75 } }).toArray();
    const expectedScoreQuery = users.where("score", "=", 777).toArray();
    const restored = table.deserialize(users.serialize());

    expect(restored.toArray()).toEqual(expectedRows);
    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
    expect(restored.where({ status: { in: ["active", "archived"] }, age: { gte: 30, lt: 75 } }).toArray()).toEqual(expectedQuery);

    restored.createIndex("status");
    expect(restored.where({ status: { in: ["active", "archived"] }, age: { gte: 30, lt: 75 } }).toArray()).toEqual(expectedQuery);

    restored.createSortedIndex("age");
    expect(restored.where({ status: { in: ["active", "archived"] }, age: { gte: 30, lt: 75 } }).toArray()).toEqual(expectedQuery);

    restored.createIndex("score");
    expect(restored.where("score", "=", 777).toArray()).toEqual(expectedScoreQuery);
  });
});

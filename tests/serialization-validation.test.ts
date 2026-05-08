import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

type SerializedMeta = {
  version: unknown;
  rowCount: unknown;
  capacity: unknown;
  columns: Array<Record<string, unknown>>;
  indexes?: unknown;
};

function expectCode(fn: () => unknown, code: string, message: RegExp): void {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
    expect((error as Error).message).toMatch(message);
  }
}

function serializedFixture() {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive", "archived"] as const),
    active: column.boolean(),
  });
  users.insertMany([
    { id: 1, age: 10, status: "active", active: true },
    { id: 2, age: 20, status: "passive", active: false },
    { id: 3, age: 30, status: "archived", active: true },
  ]);
  users.updateMany({ id: 2 }, { status: "active" });

  return users.serialize();
}

function readMeta(buffer: ArrayBuffer): {
  bytes: Uint8Array;
  headerLength: number;
  headerStart: number;
  meta: SerializedMeta;
} {
  const bytes = new Uint8Array(buffer.slice(0));
  const headerLength = new DataView(bytes.buffer).getUint32(8, true);
  const headerStart = 12;
  const meta = JSON.parse(
    new TextDecoder().decode(bytes.subarray(headerStart, headerStart + headerLength)),
  ) as SerializedMeta;
  return { bytes, headerLength, headerStart, meta };
}

function withPatchedMeta(
  buffer: ArrayBuffer,
  patch: (meta: SerializedMeta) => void,
): ArrayBuffer {
  const { bytes, headerStart, meta } = readMeta(buffer);
  patch(meta);
  const encoded = new TextEncoder().encode(JSON.stringify(meta));
  const patched = new Uint8Array(Math.max(bytes.byteLength, headerStart + encoded.byteLength));
  patched.set(bytes);
  new DataView(patched.buffer).setUint32(8, encoded.byteLength, true);
  patched.set(encoded, headerStart);
  return patched.buffer;
}

describe("serialization validation", () => {
  it("rejects invalid input shape and corrupted magic", () => {
    expectCode(() => table.deserialize({} as ArrayBuffer), "COLQL_INVALID_SERIALIZED_DATA", /expected ArrayBuffer or Uint8Array/);
    expectCode(() => table.deserialize(new ArrayBuffer(2)), "COLQL_INVALID_SERIALIZED_DATA", /too small/);

    const users = table({ id: column.uint32() });
    const bytes = new Uint8Array(users.serialize());
    bytes[0] = 0;
    expectCode(() => table.deserialize(bytes.buffer), "COLQL_INVALID_SERIALIZED_DATA", /magic header/);
  });

  it("rejects unsupported versions and truncated payloads", () => {
    const users = table({ id: column.uint32() });
    const buffer = users.serialize();
    const patched = withPatchedMeta(buffer, (meta) => {
      meta.version = "@colql/colql@0.0.1";
    });

    expectCode(() => table.deserialize(patched), "COLQL_INVALID_SERIALIZED_DATA", /Unsupported ColQL serialization version/);
    expectCode(() => table.deserialize(buffer.slice(0, buffer.byteLength - 1)), "COLQL_INVALID_SERIALIZED_DATA", /exceeds input size/);
  });

  it("rejects malformed table metadata", () => {
    const buffer = serializedFixture();

    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => { meta.rowCount = (meta.capacity as number) + 1; })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /rowCount exceeds capacity/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => { meta.capacity = -1; })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /capacity must be a positive integer/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => { meta.columns = []; })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /expected at least one column/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => { meta.indexes = [{ column: "id" }]; })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /serialized indexes are not supported/,
    );
  });

  it("rejects malformed column metadata", () => {
    const buffer = serializedFixture();

    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => {
        meta.columns.push({ ...meta.columns[0] });
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /duplicate column/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => {
        meta.columns[0].kind = "vector";
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /unknown column kind/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => {
        meta.columns[0].type = "uint64";
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /invalid numeric column type/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => {
        meta.columns[0].byteLength = -1;
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /byteLength must be a non-negative integer/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => {
        meta.columns[0].byteOffset = 13;
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /byte offset is invalid/,
    );
    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (meta) => {
        meta.columns[1].byteOffset = meta.columns[0].byteOffset;
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /overlaps column/,
    );
  });

  it("rejects corrupted dictionary metadata and payload codes", () => {
    const buffer = serializedFixture();
    const { bytes, meta } = readMeta(buffer);
    const statusMeta = meta.columns.find((columnMeta) => columnMeta.name === "status");
    if (statusMeta === undefined) {
      throw new Error("Expected status column metadata");
    }

    expectCode(
      () => table.deserialize(withPatchedMeta(buffer, (patchedMeta) => {
        const patchedStatus = patchedMeta.columns.find((columnMeta) => columnMeta.name === "status");
        if (patchedStatus !== undefined) {
          patchedStatus.values = ["active", "active"];
        }
      })),
      "COLQL_INVALID_SERIALIZED_DATA",
      /Duplicate dictionary value/,
    );

    bytes[statusMeta.byteOffset as number] = 99;
    expectCode(
      () => table.deserialize(bytes.buffer),
      "COLQL_INVALID_SERIALIZED_DATA",
      /contains invalid code/,
    );
  });

  for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    it(`maps corrupted numeric payload value ${String(value)} to serialized-data errors`, () => {
      const metrics = table({ score: column.float64() });
      metrics.insert({ score: 1.5 });
      const { bytes, meta } = readMeta(metrics.serialize());
      const scoreMeta = meta.columns.find((columnMeta) => columnMeta.name === "score");
      if (scoreMeta === undefined) {
        throw new Error("Expected score column metadata");
      }

      new DataView(bytes.buffer).setFloat64(scoreMeta.byteOffset as number, value, true);
      expectCode(
        () => table.deserialize(bytes.buffer),
        "COLQL_INVALID_SERIALIZED_DATA",
        /failed to restore column "score"/,
      );
    });
  }

  it("restores mutated snapshots without indexes and can be reindexed", () => {
    const buffer = serializedFixture();
    const restored = table.deserialize(buffer);

    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
    expect(restored.uniqueIndexes()).toEqual([]);
    expect(restored.where("status", "=", "active").explain()).toEqual(
      expect.objectContaining({
        scanType: "full",
        reasonCode: "NO_INDEX_FOR_COLUMN",
      }),
    );

    restored.createIndex("status").createSortedIndex("age").createUniqueIndex("id");
    expect(restored.where("status", "=", "archived").explain()).toEqual(
      expect.objectContaining({ scanType: "index", indexState: "fresh" }),
    );
    expect(restored.findBy("id", 1)).toEqual({
      id: 1,
      age: 10,
      status: "active",
      active: true,
    });
  });
});

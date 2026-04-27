import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function overwriteHeaderVersion(buffer: ArrayBuffer, version: string): ArrayBuffer {
  const bytes = new Uint8Array(buffer.slice(0));
  const headerLength = new DataView(bytes.buffer).getUint32(8, true);
  const headerStart = 12;
  const header = new TextDecoder().decode(bytes.subarray(headerStart, headerStart + headerLength));
  const patchedHeader = header.replace("memql@0.0.3", version);
  bytes.set(new TextEncoder().encode(patchedHeader), headerStart);
  return bytes.buffer;
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

    users.insert({ id: 1, age: 25, score: 10.5, status: "active", is_active: true });
    users.insert({ id: 2, age: 40, score: 20.25, status: "passive", is_active: false });

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
    const patched = overwriteHeaderVersion(users.serialize(), "memql@9.9.9");

    expect(() => table.deserialize(patched)).toThrow(/Unsupported memql/);
  });
});

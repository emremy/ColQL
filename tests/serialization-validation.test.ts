import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

function expectCode(fn: () => unknown, code: string, message: RegExp): void {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
    expect((error as Error).message).toMatch(message);
  }
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
    const bytes = new Uint8Array(buffer.slice(0));
    const headerLength = new DataView(bytes.buffer).getUint32(8, true);
    const headerStart = 12;
    const meta = JSON.parse(new TextDecoder().decode(bytes.subarray(headerStart, headerStart + headerLength))) as { version: string };
    meta.version = "@colql/colql@0.0.1";
    const encoded = new TextEncoder().encode(JSON.stringify(meta));
    const patched = new Uint8Array(headerStart + encoded.byteLength);
    patched.set(bytes.subarray(0, headerStart));
    new DataView(patched.buffer).setUint32(8, encoded.byteLength, true);
    patched.set(encoded, headerStart);

    expectCode(() => table.deserialize(patched.buffer), "COLQL_INVALID_SERIALIZED_DATA", /Unsupported ColQL serialization version/);
    expectCode(() => table.deserialize(buffer.slice(0, buffer.byteLength - 1)), "COLQL_INVALID_SERIALIZED_DATA", /exceeds input size/);
  });
});

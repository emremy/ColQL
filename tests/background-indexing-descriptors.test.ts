import { describe, expect, it } from "vitest";
import { BooleanColumnStorage } from "../src/storage/boolean-column";
import { backgroundIndexFeasibility } from "../src/storage/chunk-descriptor";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";
import { Table } from "../src/table";
import { column } from "../src";

describe("background indexing storage descriptors", () => {
  it("describes numeric chunks without copying or materializing rows", () => {
    const storage = new NumericColumnStorage("uint32", 1, undefined, 0, 2);
    for (const value of [10, 20, 30, 40, 50]) {
      storage.append(value);
    }

    const descriptor = storage.describeChunks();

    expect(descriptor).toEqual({
      columnKind: "numeric",
      valueType: "uint32",
      rowCount: 5,
      chunkSize: 2,
      chunks: [
        expect.objectContaining({
          chunkIndex: 0,
          rowStart: 0,
          logicalLength: 2,
          chunkSize: 2,
          bufferKind: "array-buffer",
          byteOffset: 0,
          byteLength: 8,
          arrayName: "Uint32Array",
          bytesPerElement: 4,
        }),
        expect.objectContaining({
          chunkIndex: 1,
          rowStart: 2,
          logicalLength: 2,
          chunkSize: 2,
          bufferKind: "array-buffer",
          byteOffset: 0,
          byteLength: 8,
          arrayName: "Uint32Array",
          bytesPerElement: 4,
        }),
        expect.objectContaining({
          chunkIndex: 2,
          rowStart: 4,
          logicalLength: 1,
          chunkSize: 2,
          bufferKind: "array-buffer",
          byteOffset: 0,
          byteLength: 4,
          arrayName: "Uint32Array",
          bytesPerElement: 4,
        }),
      ],
    });

    const firstChunk = descriptor.chunks[0];
    expect(firstChunk.zeroCopyEligible).toBe(false);
    expect(firstChunk).not.toHaveProperty("sharedBuffer");
    const view = new Uint32Array(
      firstChunk.buffer,
      firstChunk.byteOffset,
      firstChunk.logicalLength,
    );
    storage.set(1, 99);
    expect(view[1]).toBe(99);
    expect(backgroundIndexFeasibility(descriptor)).toEqual({
      eligible: false,
      reason: "non-shared-buffer",
      columnKind: "numeric",
      rowCount: 5,
      chunkCount: 3,
      zeroCopyInput: false,
    });
  });

  it("describes SAB-backed numeric chunks as zero-copy worker-readable", () => {
    const storage = NumericColumnStorage.withSharedBuffer("uint32", 1, undefined, 0, 2);
    for (const value of [10, 20, 30]) {
      storage.append(value);
    }

    const descriptor = storage.describeChunks();
    const firstChunk = descriptor.chunks[0];

    expect(firstChunk.buffer).toBeInstanceOf(SharedArrayBuffer);
    expect(firstChunk.sharedBuffer).toBe(firstChunk.buffer);
    expect(firstChunk.bufferKind).toBe("shared-array-buffer");
    expect(firstChunk.zeroCopyEligible).toBe(true);
    expect(firstChunk.byteOffset).toBe(0);
    expect(firstChunk.byteLength).toBe(8);
    expect(firstChunk.logicalLength).toBe(2);
    expect(backgroundIndexFeasibility(descriptor)).toEqual({
      eligible: true,
      reason: "shared-chunks",
      columnKind: "numeric",
      rowCount: 3,
      chunkCount: 2,
      zeroCopyInput: true,
    });

    const view = new Uint32Array(
      firstChunk.sharedBuffer,
      firstChunk.byteOffset,
      firstChunk.logicalLength,
    );
    storage.set(1, 99);
    expect(view[1]).toBe(99);
    view[0] = 77;
    expect(storage.get(0)).toBe(77);
  });

  it("describes dictionary code chunks without exposing dictionary strings", () => {
    const values = Array.from(
      { length: 300 },
      (_unused, index) => `value-${index}`,
    ) as [string, ...string[]];
    const storage = new DictionaryColumnStorage(values, 1, undefined, 0, 2);
    storage.append("value-1");
    storage.append("value-299");
    storage.append("value-42");

    const descriptor = storage.describeChunks();

    expect(descriptor.columnKind).toBe("dictionary-code");
    expect(descriptor.codeArrayName).toBe("Uint16Array");
    expect(descriptor.dictionarySize).toBe(300);
    expect(descriptor.rowCount).toBe(3);
    expect(Object.keys(descriptor)).not.toContain("values");
    expect(descriptor.chunks).toEqual([
      expect.objectContaining({
        chunkIndex: 0,
        rowStart: 0,
        logicalLength: 2,
        byteLength: 4,
        arrayName: "Uint16Array",
        bytesPerElement: 2,
      }),
      expect.objectContaining({
        chunkIndex: 1,
        rowStart: 2,
        logicalLength: 1,
        byteLength: 2,
        arrayName: "Uint16Array",
        bytesPerElement: 2,
      }),
    ]);

    const firstChunk = descriptor.chunks[0];
    expect(firstChunk.zeroCopyEligible).toBe(false);
    expect(firstChunk).not.toHaveProperty("sharedBuffer");
    const codes = new Uint16Array(
      firstChunk.buffer,
      firstChunk.byteOffset,
      firstChunk.logicalLength,
    );
    expect([...codes]).toEqual([1, 299]);
    expect(backgroundIndexFeasibility(descriptor)).toEqual({
      eligible: false,
      reason: "non-shared-buffer",
      columnKind: "dictionary-code",
      rowCount: 3,
      chunkCount: 2,
      zeroCopyInput: false,
    });
  });

  it("describes SAB-backed dictionary code chunks without copying or exposing values", () => {
    const values = Array.from(
      { length: 300 },
      (_unused, index) => `value-${index}`,
    ) as [string, ...string[]];
    const storage = DictionaryColumnStorage.withSharedBuffer(values, 1, undefined, 0, 2);
    storage.append("value-1");
    storage.append("value-299");
    storage.append("value-42");

    const descriptor = storage.describeChunks();
    const firstChunk = descriptor.chunks[0];

    expect(descriptor.columnKind).toBe("dictionary-code");
    expect(Object.keys(descriptor)).not.toContain("values");
    expect(firstChunk.buffer).toBeInstanceOf(SharedArrayBuffer);
    expect(firstChunk.sharedBuffer).toBe(firstChunk.buffer);
    expect(firstChunk.bufferKind).toBe("shared-array-buffer");
    expect(firstChunk.zeroCopyEligible).toBe(true);
    expect(backgroundIndexFeasibility(descriptor)).toEqual({
      eligible: true,
      reason: "shared-chunks",
      columnKind: "dictionary-code",
      rowCount: 3,
      chunkCount: 2,
      zeroCopyInput: true,
    });

    const codes = new Uint16Array(
      firstChunk.sharedBuffer,
      firstChunk.byteOffset,
      firstChunk.logicalLength,
    );
    expect([...codes]).toEqual([1, 299]);
    storage.set(0, "value-42");
    expect(codes[0]).toBe(42);
    codes[1] = 1;
    expect(storage.get(1)).toBe("value-1");
  });

  it("keeps SAB-backed numeric and dictionary storage behavior compatible with indexes and serialization", () => {
    const schema = {
      id: column.uint32(),
      age: column.uint8(),
      status: column.dictionary(["active", "passive"] as const),
    };
    const users = new Table(schema, 2, {
      storages: {
        id: NumericColumnStorage.withSharedBuffer("uint32", 2),
        age: NumericColumnStorage.withSharedBuffer("uint8", 2),
        status: DictionaryColumnStorage.withSharedBuffer(["active", "passive"] as const, 2),
      },
    });

    users
      .insert({ id: 1, age: 20, status: "active" })
      .insert({ id: 2, age: 30, status: "passive" })
      .insert({ id: 3, age: 40, status: "active" })
      .createIndex("status")
      .createSortedIndex("age");

    expect(users.where("status", "=", "active").toArray()).toEqual([
      { id: 1, age: 20, status: "active" },
      { id: 3, age: 40, status: "active" },
    ]);
    expect(users.where("age", ">=", 30).toArray()).toEqual([
      { id: 2, age: 30, status: "passive" },
      { id: 3, age: 40, status: "active" },
    ]);

    users.update(1, { age: 35, status: "active" });
    users.delete(0);

    expect(users.where("status", "=", "active").toArray()).toEqual([
      { id: 2, age: 35, status: "active" },
      { id: 3, age: 40, status: "active" },
    ]);
    expect(users.where("age", ">", 35).toArray()).toEqual([
      { id: 3, age: 40, status: "active" },
    ]);

    const restored = Table.deserialize(users.serialize());
    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
    expect(restored.toArray()).toEqual(users.toArray());
    restored.createIndex("status").createSortedIndex("age");
    expect(restored.where("status", "=", "active").toArray()).toEqual(users.toArray());
  });

  it("marks boolean chunks unsupported for Phase 1 background indexing", () => {
    const storage = new BooleanColumnStorage(1, undefined, 0, 2);
    storage.append(true);
    storage.append(false);

    const descriptor = storage.describeChunks();

    expect(descriptor).toEqual({
      columnKind: "boolean",
      rowCount: 2,
      chunkSize: 2,
      chunks: [],
      unsupportedReason: "boolean-bit-packed-deferred",
    });
    expect(backgroundIndexFeasibility(descriptor)).toEqual({
      eligible: false,
      reason: "unsupported-column-kind",
      columnKind: "boolean",
      rowCount: 2,
      chunkCount: 0,
      zeroCopyInput: false,
    });
  });
});

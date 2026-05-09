import { describe, expect, it } from "vitest";
import { BooleanColumnStorage } from "../src/storage/boolean-column";
import { backgroundIndexFeasibility } from "../src/storage/chunk-descriptor";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";
import { NumericColumnStorage } from "../src/storage/numeric-column";

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

import { describe, expect, it } from "vitest";
import { NumericColumnStorage } from "../src/storage/numeric-column";
import type { NumericColumnType } from "../src/types";

const cases: [NumericColumnType, number, string][] = [
  ["int16", -123, "Int16Array"],
  ["int32", -123_456, "Int32Array"],
  ["uint8", 255, "Uint8Array"],
  ["uint16", 65_535, "Uint16Array"],
  ["uint32", 4_000_000_000, "Uint32Array"],
  ["float32", 1.5, "Float32Array"],
  ["float64", Math.PI, "Float64Array"],
];

describe("NumericColumnStorage", () => {
  it.each(cases)("stores, reads, and resizes %s", (type, value, arrayName) => {
    const storage = new NumericColumnStorage(type, 2);
    storage.set(0, value);
    storage.set(1, 7);
    storage.resize(5);
    expect(storage.get(0)).toBeCloseTo(value);
    expect(storage.get(1)).toBeCloseTo(7);
    expect(storage.capacity).toBe(5);
    expect(storage.arrayName).toBe(arrayName);
  });
});

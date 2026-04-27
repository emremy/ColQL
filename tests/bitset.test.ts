import { describe, expect, it } from "vitest";
import { BitSet } from "../src/storage/bitset";

describe("BitSet", () => {
  it("defaults to false and can set/get true and false", () => {
    const bits = new BitSet(10);
    expect(bits.get(3)).toBe(false);
    bits.set(3, true);
    expect(bits.get(3)).toBe(true);
    bits.set(3, false);
    expect(bits.get(3)).toBe(false);
  });

  it.each([10, 17, 33])("resizes non-byte-aligned capacity %i without losing values", (capacity) => {
    const bits = new BitSet(capacity);
    bits.set(capacity - 1, true);
    bits.resize(capacity * 2 + 1);
    expect(bits.get(capacity - 1)).toBe(true);
    expect(bits.get(capacity)).toBe(false);
  });

  it("can resize multiple times", () => {
    const bits = new BitSet(2);
    bits.set(1, true);
    bits.resize(9);
    bits.resize(33);
    expect(bits.get(1)).toBe(true);
  });
});

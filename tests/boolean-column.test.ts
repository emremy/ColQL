import { describe, expect, it } from "vitest";
import { BooleanColumnStorage } from "../src/storage/boolean-column";

describe("BooleanColumnStorage", () => {
  it("stores booleans via BitSet and resizes without data loss", () => {
    const storage = new BooleanColumnStorage(2);
    storage.set(0, true);
    storage.set(1, false);
    storage.resize(10);

    expect(storage.get(0)).toBe(true);
    expect(storage.get(1)).toBe(false);
    expect(storage.get(9)).toBe(false);
  });
});

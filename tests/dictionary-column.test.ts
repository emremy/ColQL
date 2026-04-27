import { describe, expect, it } from "vitest";
import { column, table } from "../src";
import { DictionaryColumnStorage } from "../src/storage/dictionary-column";

describe("DictionaryColumnStorage", () => {
  it("stores and retrieves dictionary values", () => {
    const storage = new DictionaryColumnStorage(["active", "passive"] as const, 2);
    storage.set(0, "active");
    storage.set(1, "passive");
    storage.resize(4);
    expect(storage.get(0)).toBe("active");
    expect(storage.get(1)).toBe("passive");
  });

  it("rejects unknown values", () => {
    const storage = new DictionaryColumnStorage(["active", "passive"] as const, 1);
    expect(() => storage.set(0, "deleted" as "active")).toThrow(/Invalid dictionary value/);
  });

  it("uses the smallest practical code array", () => {
    const small = new DictionaryColumnStorage(["a", "b"] as const, 1);
    const mediumValues = Array.from({ length: 256 }, (_, index) => `v${index}`) as [string, ...string[]];
    const medium = new DictionaryColumnStorage(mediumValues, 1);
    const largeValues = Array.from({ length: 65_536 }, (_, index) => `v${index}`) as [string, ...string[]];
    const large = new DictionaryColumnStorage(largeValues, 1);

    expect(small.arrayName).toBe("Uint8Array");
    expect(medium.arrayName).toBe("Uint16Array");
    expect(large.arrayName).toBe("Uint32Array");
  });

  it("supports in and not in through queries", () => {
    const users = table({ status: column.dictionary(["active", "passive", "blocked"] as const) });
    users.insert({ status: "active" }).insert({ status: "passive" }).insert({ status: "blocked" });

    expect(users.where("status", "in", ["active", "passive"]).count()).toBe(2);
    expect(users.where("status", "not in", ["blocked"]).count()).toBe(2);
  });
});

import { describe, expect, it } from "vitest";
import { ColQLError } from "../src";
import { ExperimentalChunkedBooleanColumn } from "../src/experimental/chunked/chunked-boolean-column";
import { ExperimentalChunkedDictionaryColumn } from "../src/experimental/chunked/chunked-dictionary-column";
import { ExperimentalChunkedNumericColumn } from "../src/experimental/chunked/chunked-numeric-column";

describe("experimental chunked column storage", () => {
  it("appends and gets uint32, uint8, and float64 values", () => {
    const ids = new ExperimentalChunkedNumericColumn("uint32", 3);
    const ages = new ExperimentalChunkedNumericColumn("uint8", 3);
    const scores = new ExperimentalChunkedNumericColumn("float64", 3);

    for (let i = 0; i < 8; i += 1) {
      ids.append(i + 100);
      ages.append(i);
      scores.append(i + 0.5);
    }

    expect(ids.toArray()).toEqual([100, 101, 102, 103, 104, 105, 106, 107]);
    expect(ages.get(4)).toBe(4);
    expect(scores.get(7)).toBe(7.5);
  });

  it("physically deletes first, middle, and last rows across chunks", () => {
    const column = new ExperimentalChunkedNumericColumn("uint32", 4);
    const baseline = Array.from({ length: 11 }, (_, index) => index);
    baseline.forEach((value) => column.append(value));

    baseline.splice(0, 1);
    column.deleteAt(0);
    expect(column.toArray()).toEqual(baseline);

    baseline.splice(4, 1);
    column.deleteAt(4);
    expect(column.toArray()).toEqual(baseline);

    baseline.splice(baseline.length - 1, 1);
    column.deleteAt(column.rowCount - 1);
    expect(column.toArray()).toEqual(baseline);
  });

  it("deletes all rows one by one", () => {
    const column = new ExperimentalChunkedNumericColumn("uint8", 2);
    for (let i = 0; i < 9; i += 1) {
      column.append(i);
    }

    while (column.rowCount > 0) {
      column.deleteAt(0);
    }

    expect(column.toArray()).toEqual([]);
    expect(column.rowCount).toBe(0);
  });

  it("matches an array baseline for deterministic and random deletes", () => {
    const column = new ExperimentalChunkedNumericColumn("uint32", 5);
    const baseline = Array.from({ length: 50 }, (_, index) => index * 2);
    baseline.forEach((value) => column.append(value));

    for (const pickIndex of [0, 7, 12, 20, 49]) {
      const index = Math.min(pickIndex, baseline.length - 1);
      baseline.splice(index, 1);
      column.deleteAt(index);
      expect(column.toArray()).toEqual(baseline);
    }

    let seed = 17;
    while (baseline.length > 10) {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      const index = seed % baseline.length;
      baseline.splice(index, 1);
      column.deleteAt(index);
      expect(column.toArray()).toEqual(baseline);
    }
  });

  it("supports dictionary and boolean chunked deletes", () => {
    const status = new ExperimentalChunkedDictionaryColumn(["active", "passive", "archived"] as const, 3);
    const active = new ExperimentalChunkedBooleanColumn(3);
    const baselineStatus = ["active", "passive", "archived", "active", "passive"] as const;
    const baselineActive = [true, false, false, true, false];

    baselineStatus.forEach((value) => status.append(value));
    baselineActive.forEach((value) => active.append(value));

    status.deleteAt(2);
    active.deleteAt(2);

    expect(status.toArray()).toEqual(["active", "passive", "active", "passive"]);
    expect(active.toArray()).toEqual([true, false, true, false]);
  });

  it("throws ColQLError for invalid delete indexes", () => {
    const column = new ExperimentalChunkedNumericColumn("uint32", 4);
    column.append(1);

    expect(() => column.deleteAt(1)).toThrow(ColQLError);
    expect(() => column.get(-1)).toThrow(ColQLError);
  });
});

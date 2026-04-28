import { describe, expect, it } from "vitest";
import { column, table } from "../src";

describe("production chunked storage", () => {
  it("preserves logical order across chunk boundaries after deletes", () => {
    const users = table({
      id: column.uint32(),
      age: column.uint8(),
      score: column.float64(),
      status: column.dictionary(["active", "passive", "archived"] as const),
      is_active: column.boolean(),
    });
    const baseline: Array<{ id: number; age: number; score: number; status: "active" | "passive" | "archived"; is_active: boolean }> = [];

    for (let id = 0; id < 70_000; id += 1) {
      const row = {
        id,
        age: id % 100,
        score: id + 0.5,
        status: id % 3 === 0 ? "active" as const : id % 3 === 1 ? "passive" as const : "archived" as const,
        is_active: id % 2 === 0,
      };
      users.insert(row);
      baseline.push(row);
    }

    users.delete(65_535);
    baseline.splice(65_535, 1);
    users.delete(65_535);
    baseline.splice(65_535, 1);
    users.delete(1);
    baseline.splice(1, 1);

    expect(users.rowCount).toBe(baseline.length);
    expect(users.get(0)).toEqual(baseline[0]);
    expect(users.get(1)).toEqual(baseline[1]);
    expect(users.get(65_534)).toEqual(baseline[65_534]);
    expect(users.get(65_535)).toEqual(baseline[65_535]);
    expect(users.get(users.rowCount - 1)).toEqual(baseline[baseline.length - 1]);
    expect(users.where("id", "in", [65_535, 65_536, 65_537]).toArray()).toEqual(
      baseline.filter((row) => [65_535, 65_536, 65_537].includes(row.id)),
    );
  });
});

import { describe, expect, it } from "vitest";
import { column, table } from "../src";

function scoresFixture() {
  const scores = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    status: column.dictionary(["active", "passive"] as const),
  });

  scores.insert({ id: 1, age: 10, score: 50, status: "active" });
  scores.insert({ id: 2, age: 20, score: 75, status: "passive" });
  scores.insert({ id: 3, age: 30, score: 100, status: "active" });
  return scores;
}

describe("aggregation API", () => {
  it("computes sum, avg, min, and max without materializing rows", () => {
    const scores = scoresFixture();
    scores.resetMaterializationCounter();

    expect(scores.sum("age")).toBe(60);
    expect(scores.avg("age")).toBe(20);
    expect(scores.min("score")).toBe(50);
    expect(scores.max("score")).toBe(100);
    expect(scores.materializedRowCount).toBe(0);
  });

  it("works with where filters", () => {
    const scores = scoresFixture();

    expect(scores.where("status", "=", "active").sum("score")).toBe(150);
    expect(scores.where("status", "=", "active").avg("age")).toBe(20);
    expect(scores.where("status", "=", "active").min("age")).toBe(10);
    expect(scores.where("status", "=", "active").max("age")).toBe(30);
    expect(() => scores.where("status", "=", "missing" as "active")).toThrow(/Invalid dictionary value/);
  });

  it("returns undefined for avg/min/max on empty matches", () => {
    const scores = scoresFixture();

    expect(scores.where("age", ">", 99).sum("age")).toBe(0);
    expect(scores.where("age", ">", 99).avg("age")).toBeUndefined();
    expect(scores.where("age", ">", 99).min("age")).toBeUndefined();
    expect(scores.where("age", ">", 99).max("age")).toBeUndefined();
  });

  it("count matches filtered results and respects limit", () => {
    const scores = scoresFixture();

    expect(scores.where("age", ">=", 20).count()).toBe(2);
    expect(scores.where("age", ">=", 10).limit(2).count()).toBe(2);
  });
});

import { describe, expect, it } from "vitest";
import { table } from "../../../src";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex } from "../helpers/explain";
import { updateOracle } from "../helpers/oracle";

describe("serialization mutation after restore endpoint scenarios", () => {
  it("PATCH /users after restore and reindex mutates correctly and preserves find-by-id", () => {
    const { users, oracle } = buildUserAnalyticsFixture();
    const restored = table.deserialize(users.serialize()) as unknown as typeof users;
    restored.createIndex("segment").createSortedIndex("age").createUniqueIndex("id");

    const updated = updateOracle(
      oracle,
      (row) => row.segment === "free" && row.age < 25,
      { segment: "pro" },
    );
    expectMutationResult(
      restored.updateMany({ segment: "free", age: { lt: 25 } }, { segment: "pro" }),
      updated,
    );

    const query = restored.where("segment", "=", "pro");
    expectDirtyIndex(query, "equality:segment");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.segment === "pro"),
    );
    expect(restored.findBy("id", 42)).toEqual(oracle.find((row) => row.id === 42));
  });
});

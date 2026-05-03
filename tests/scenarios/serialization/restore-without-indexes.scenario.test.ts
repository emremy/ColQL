import { describe, expect, it } from "vitest";
import { table } from "../../../src";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectFullScanReason } from "../helpers/explain";

describe("serialization restore without indexes endpoint scenarios", () => {
  it("GET /users after restore is correct before reindex but explains no implicit indexes", () => {
    const { users, oracle } = buildUserAnalyticsFixture();
    const restored = table.deserialize(users.serialize()) as unknown as typeof users;

    expect(restored.indexes()).toEqual([]);
    expect(restored.sortedIndexes()).toEqual([]);
    expect(restored.uniqueIndexes()).toEqual([]);

    const query = restored.where("status", "=", "suspended");
    expectFullScanReason(query, "NO_INDEX_FOR_COLUMN");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.status === "suspended"),
    );
  });
});

import { describe, it } from "vitest";
import { table } from "../../../src";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectFreshIndex, expectFullScanReason } from "../helpers/explain";

describe("serialization restore and reindex endpoint scenarios", () => {
  it("GET /users transitions from full scan to index usage after explicit reindex", () => {
    const { users, oracle } = buildUserAnalyticsFixture();
    const restored = table.deserialize(users.serialize()) as unknown as typeof users;

    const before = restored.where("lastSeen", ">=", 1_700_110_000);
    expectFullScanReason(before, "RANGE_QUERY_WITHOUT_SORTED_INDEX");
    expectRowsEqual(
      before.toArray(),
      oracle.filter((row) => row.lastSeen >= 1_700_110_000),
    );

    restored.createIndex("status").createSortedIndex("lastSeen").createUniqueIndex("id");
    const after = restored.where("lastSeen", ">=", 1_700_110_000);
    expectFreshIndex(after, "sorted:lastSeen");
    expectRowsEqual(
      after.toArray(),
      oracle.filter((row) => row.lastSeen >= 1_700_110_000),
    );
  });
});

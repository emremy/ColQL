import { describe, it } from "vitest";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex } from "../helpers/explain";
import { deleteFromOracle } from "../helpers/oracle";

describe("user analytics delete endpoint scenarios", () => {
  it("DELETE /users?status=suspended&lastSeen<cutoff removes subset and preserves oracle parity", () => {
    const { users, oracle } = buildUserAnalyticsFixture();
    const cutoff = 1_700_040_000;

    const affectedRows = deleteFromOracle(
      oracle,
      (row) => row.status === "suspended" && row.lastSeen < cutoff,
    );
    const result = users.deleteMany({
      status: "suspended",
      lastSeen: { lt: cutoff },
    });

    expectMutationResult(result, affectedRows);
    const query = users.where("lastSeen", "<", cutoff);
    expectDirtyIndex(query, "sorted:lastSeen");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.lastSeen < cutoff),
    );
  });
});

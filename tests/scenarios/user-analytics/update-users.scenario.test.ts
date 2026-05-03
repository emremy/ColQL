import { describe, it } from "vitest";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex } from "../helpers/explain";
import { updateOracle } from "../helpers/oracle";

describe("user analytics update endpoint scenarios", () => {
  it("PATCH /users?status=inactive&age>=60 updates a subset and keeps indexed queries correct", () => {
    const { users, oracle } = buildUserAnalyticsFixture();

    const affectedRows = updateOracle(
      oracle,
      (row) => row.status === "inactive" && row.age >= 60,
      { status: "active", score: 9_999 },
    );
    const result = users.updateMany(
      { status: "inactive", age: { gte: 60 } },
      { status: "active", score: 9_999 },
    );

    expectMutationResult(result, affectedRows);
    const query = users.where("status", "=", "active");
    expectDirtyIndex(query, "equality:status");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.status === "active"),
    );
  });
});

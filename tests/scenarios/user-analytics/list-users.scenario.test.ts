import { describe, it } from "vitest";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectFreshIndex } from "../helpers/explain";

describe("user analytics list endpoint scenarios", () => {
  it("GET /users?status=suspended&segment=enterprise uses equality index and matches oracle", () => {
    const { users, oracle } = buildUserAnalyticsFixture();

    const query = users.where({ status: "suspended", segment: "enterprise" });
    const expected = oracle.filter(
      (row) => row.status === "suspended" && row.segment === "enterprise",
    );

    expectFreshIndex(query, "equality:segment");
    expectRowsEqual(query.toArray(), expected);
  });
});

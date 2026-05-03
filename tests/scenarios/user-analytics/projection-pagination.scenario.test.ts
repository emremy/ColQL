import { describe, it } from "vitest";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectProjectedRows } from "../helpers/assertions";
import { expectProjectionPushdown, expectUsesIndex } from "../helpers/explain";
import { projectRows } from "../helpers/oracle";

describe("user analytics projection and pagination endpoint scenarios", () => {
  it("GET /users?segment=enterprise&age>=45&limit=25&offset=5 projects only requested fields", () => {
    const { users, oracle } = buildUserAnalyticsFixture();

    const query = users
      .where({ segment: "enterprise", age: { gte: 45 } })
      .select(["id", "age", "score"])
      .offset(5)
      .limit(25);
    const expected = projectRows(
      oracle
        .filter((row) => row.segment === "enterprise" && row.age >= 45)
        .slice(5, 30),
      ["id", "age", "score"],
    );

    expectUsesIndex(query, "equality:segment");
    expectProjectionPushdown(query);
    expectProjectedRows(query.toArray(), expected);
  });
});

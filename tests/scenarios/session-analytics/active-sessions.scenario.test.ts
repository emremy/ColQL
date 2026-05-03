import { describe, expect, it } from "vitest";
import { buildSessionAnalyticsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectUsesIndex } from "../helpers/explain";

describe("session analytics active session endpoint scenarios", () => {
  it("GET /sessions?status=active&country=TR uses equality index and matches oracle", () => {
    const { sessions, oracle } = buildSessionAnalyticsFixture();

    const query = sessions.where({ status: "active", country: "TR" });
    const expected = oracle.filter(
      (row) => row.status === "active" && row.country === "TR",
    );

    expectUsesIndex(query, "equality:country");
    expectRowsEqual(query.toArray(), expected);
    expect(sessions.countWhere({ status: "active" })).toBe(
      oracle.filter((row) => row.status === "active").length,
    );
  });
});

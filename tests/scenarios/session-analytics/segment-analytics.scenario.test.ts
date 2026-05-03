import { describe, expect, it } from "vitest";
import { buildSessionAnalyticsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectUsesIndex } from "../helpers/explain";
import { avgBy, sumBy } from "../helpers/oracle";

describe("session analytics segment endpoint scenarios", () => {
  it("GET /sessions/segments?segment=enterprise&status=active reports aggregate usage and parity", () => {
    const { sessions, oracle } = buildSessionAnalyticsFixture();

    const query = sessions.where({ segment: "enterprise", status: "active" });
    const expected = oracle.filter(
      (row) => row.segment === "enterprise" && row.status === "active",
    );

    expectUsesIndex(query, "equality:segment");
    expectRowsEqual(query.toArray(), expected);
    expect(query.sum("durationMs")).toBe(sumBy(expected, "durationMs"));
    expect(query.avg("durationMs")).toBe(avgBy(expected, "durationMs"));
  });
});

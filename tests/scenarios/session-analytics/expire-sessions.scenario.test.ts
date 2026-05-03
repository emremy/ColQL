import { describe, expect, it } from "vitest";
import { buildSessionAnalyticsFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex } from "../helpers/explain";
import { updateOracle } from "../helpers/oracle";

describe("session analytics expire endpoint scenarios", () => {
  it("PATCH /sessions/expire marks stale sessions inactive and keeps indexed queries correct", () => {
    const { sessions, oracle } = buildSessionAnalyticsFixture();
    const cutoff = 1_720_060_000;

    const expired = updateOracle(
      oracle,
      (row) => row.status === "active" && row.startedAt < cutoff,
      { status: "expired" },
    );
    expectMutationResult(
      sessions.updateMany({ status: "active", startedAt: { lt: cutoff } }, { status: "expired" }),
      expired,
    );

    const query = sessions.where("status", "=", "expired");
    expectDirtyIndex(query, "equality:status");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.status === "expired"),
    );
    expect(sessions.avg("durationMs")).toBe(
      oracle.reduce((total, row) => total + row.durationMs, 0) / oracle.length,
    );
  });
});

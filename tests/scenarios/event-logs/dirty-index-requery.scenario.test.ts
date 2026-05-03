import { describe, it } from "vitest";
import { buildEventLogsFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { expectDirtyIndex, expectFreshIndex } from "../helpers/explain";
import { deleteFromOracle, updateOracle } from "../helpers/oracle";

describe("event log dirty index requery endpoint scenarios", () => {
  it("DELETE /events?severity=debug dirties indexes, then requery rebuilds and matches oracle", () => {
    const { events, oracle } = buildEventLogsFixture();

    const deleted = deleteFromOracle(oracle, (row) => row.severity === "debug");
    expectMutationResult(events.deleteMany({ severity: "debug" }), deleted);

    const query = events.where("timestamp", ">=", 1_710_145_000);
    expectDirtyIndex(query, "sorted:timestamp");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.timestamp >= 1_710_145_000),
    );
    expectFreshIndex(events.where("timestamp", ">=", 1_710_145_000), "sorted:timestamp");
  });

  it("PATCH /events?service=worker marks slow worker events as warnings and keeps equality queries correct", () => {
    const { events, oracle } = buildEventLogsFixture();

    const updated = updateOracle(
      oracle,
      (row) => row.service === "worker" && row.durationMs >= 1_500,
      { severity: "warn" },
    );
    expectMutationResult(
      events.updateMany({ service: "worker", durationMs: { gte: 1_500 } }, { severity: "warn" }),
      updated,
    );

    const query = events.where("severity", "=", "warn");
    expectDirtyIndex(query, "equality:severity");
    expectRowsEqual(
      query.toArray(),
      oracle.filter((row) => row.severity === "warn"),
    );
  });
});

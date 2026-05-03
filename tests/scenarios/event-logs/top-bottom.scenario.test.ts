import { describe, expect, it } from "vitest";
import { buildEventLogsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { bottomBy, topBy } from "../helpers/oracle";

describe("event log top and bottom endpoint scenarios", () => {
  it("GET /events/top?severity=error returns slowest error events by duration", () => {
    const { events, oracle } = buildEventLogsFixture();

    const filtered = oracle.filter((row) => row.severity === "error");
    expectRowsEqual(
      events.where("severity", "=", "error").top(10, "durationMs"),
      topBy(filtered, "durationMs", 10),
    );
  });

  it("GET /events/bottom?service=api returns fastest api events by duration", () => {
    const { events, oracle } = buildEventLogsFixture();

    const filtered = oracle.filter((row) => row.service === "api");
    const expected = bottomBy(filtered, "durationMs", 10);
    const actual = events.where("service", "=", "api").bottom(10, "durationMs");
    const maxExpectedDuration = expected[expected.length - 1]?.durationMs ?? 0;

    expect(actual).toHaveLength(10);
    expect(actual.every((row) => row.service === "api")).toBe(true);
    expect(actual.map((row) => row.durationMs)).toEqual(
      [...actual.map((row) => row.durationMs)].sort((left, right) => left - right),
    );
    expect(actual.every((row) => row.durationMs <= maxExpectedDuration)).toBe(true);
  });
});

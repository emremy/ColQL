import { describe, it } from "vitest";
import { buildEventLogsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectFreshIndex } from "../helpers/explain";

describe("event log service and severity endpoint scenarios", () => {
  it("GET /events?service=billing&severity=error uses equality index and matches oracle", () => {
    const { events, oracle } = buildEventLogsFixture();

    const query = events.where({ service: "billing", severity: "error" });
    const expected = oracle.filter(
      (row) => row.service === "billing" && row.severity === "error",
    );

    expectFreshIndex(query, "equality:severity");
    expectRowsEqual(query.toArray(), expected);
  });
});

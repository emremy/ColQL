import { describe, it } from "vitest";
import { buildEventLogsFixture } from "../helpers/api-fixtures";
import { expectRowsEqual } from "../helpers/assertions";
import { expectUsesIndex } from "../helpers/explain";

describe("event log time range endpoint scenarios", () => {
  it("GET /events?from=&to= uses sorted timestamp index and matches oracle", () => {
    const { events, oracle } = buildEventLogsFixture();
    const from = 1_710_120_000;
    const to = 1_710_126_000;

    const query = events.where({ timestamp: { gte: from, lte: to } });
    const expected = oracle.filter(
      (row) => row.timestamp >= from && row.timestamp <= to,
    );

    expectUsesIndex(query, "sorted:timestamp");
    expectRowsEqual(query.toArray(), expected);
  });
});

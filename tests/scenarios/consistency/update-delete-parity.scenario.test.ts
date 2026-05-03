import { describe, expect, it } from "vitest";
import { ColQLError } from "../../../src";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";
import { expectMutationResult, expectRowsEqual } from "../helpers/assertions";
import { deleteFromOracle, updateOracle } from "../helpers/oracle";

describe("consistency update/delete parity endpoint scenarios", () => {
  it("PATCH then DELETE /users maintains parity through repeated mutation/query sequence", () => {
    const { users, oracle } = buildUserAnalyticsFixture(25_000);

    const updated = updateOracle(
      oracle,
      (row) => row.segment === "enterprise" && row.score >= 7_500,
      { status: "suspended" },
    );
    expectMutationResult(
      users.updateMany({ segment: "enterprise", score: { gte: 7_500 } }, { status: "suspended" }),
      updated,
    );

    const deleted = deleteFromOracle(
      oracle,
      (row) => row.status === "inactive" && row.lastSeen < 1_700_030_000,
    );
    expectMutationResult(
      users.deleteMany({ status: "inactive", lastSeen: { lt: 1_700_030_000 } }),
      deleted,
    );

    expectRowsEqual(
      users.where({ status: "suspended", age: { gte: 40 } }).toArray(),
      oracle.filter((row) => row.status === "suspended" && row.age >= 40),
    );
    expectRowsEqual(
      users.where("segment", "=", "enterprise").limit(100).toArray(),
      oracle.filter((row) => row.segment === "enterprise").slice(0, 100),
    );
  });

  it("POST /users bulk insert with duplicate id fails all-or-nothing", () => {
    const { users, oracle } = buildUserAnalyticsFixture();
    const before = users.toArray();

    expect(() =>
      users.insertMany([
        { id: 99_001, status: "active", segment: "pro", age: 33, score: 100, lastSeen: 1_700_001_000 },
        { id: 42, status: "inactive", segment: "free", age: 22, score: 200, lastSeen: 1_700_002_000 },
      ]),
    ).toThrow(ColQLError);

    expectRowsEqual(users.toArray(), before);
    expectRowsEqual(users.toArray(), oracle);
  });
});

import { describe, expect, it } from "vitest";
import { buildUserAnalyticsFixture } from "../helpers/api-fixtures";

describe("consistency row index instability endpoint scenarios", () => {
  it("DELETE /users/:rowIndex demonstrates rowIndex is not a stable external ID", () => {
    const { users } = buildUserAnalyticsFixture(50);

    const firstRow = users.get(0);
    const secondRow = users.get(1);
    users.delete(0);

    expect(firstRow.id).not.toBe(users.get(0).id);
    expect(users.get(0).id).toBe(secondRow.id);
    expect(users.findBy("id", firstRow.id)).toBeUndefined();
    expect(users.findBy("id", secondRow.id)).toEqual(secondRow);
  });
});

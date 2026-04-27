import { describe, expect, it } from "vitest";
import { ColQLError } from "../src";
import { column, table } from "../src";

function expectColQLError(fn: () => unknown, code: string, message: RegExp): void {
  try {
    fn();
    throw new Error("Expected ColQLError");
  } catch (error) {
    expect(error).toBeInstanceOf(ColQLError);
    expect((error as ColQLError).code).toBe(code);
    expect((error as Error).message).toMatch(message);
  }
}

describe("ColQLError", () => {
  it("exposes code and details", () => {
    const error = new ColQLError("COLQL_TYPE_MISMATCH", "Bad value", { column: "age" });

    expect(error.name).toBe("ColQLError");
    expect(error.code).toBe("COLQL_TYPE_MISMATCH");
    expect(error.details).toEqual({ column: "age" });
  });

  it("is used for schema errors", () => {
    expectColQLError(
      () => table({ status: column.dictionary(["active", "active"] as unknown as [string, string]) }),
      "COLQL_DUPLICATE_COLUMN",
      /Duplicate dictionary value/,
    );
  });
});

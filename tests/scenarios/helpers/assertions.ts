import { expect } from "vitest";

export function expectRowsEqual<TRow>(
  actual: readonly TRow[],
  expected: readonly TRow[],
): void {
  expect(actual).toEqual(expected);
}

export function expectProjectedRows<TRow extends object>(
  actual: readonly TRow[],
  expected: readonly TRow[],
): void {
  expect(actual).toEqual(expected);
  for (const row of actual) {
    expect(Object.keys(row).sort()).toEqual(Object.keys(expected[0] ?? row).sort());
  }
}

export function expectMutationResult(
  actual: { readonly affectedRows: number },
  expectedAffectedRows: number,
): void {
  expect(actual).toEqual({ affectedRows: expectedAffectedRows });
}

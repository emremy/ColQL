import { ColQLError } from "./errors";
import type { ColumnDefinition, NumericColumnType, Operator, Schema } from "./types";

export const SUPPORTED_OPERATORS: readonly Operator[] = ["=", "!=", ">", ">=", "<", "<=", "in", "not in"];
const COMPARISON_OPERATORS: readonly Operator[] = [">", ">=", "<", "<="];

const NUMERIC_RANGES: Record<NumericColumnType, { min?: number; max?: number; integer: boolean; label: string }> = {
  uint8: { min: 0, max: 255, integer: true, label: "uint8 integer between 0 and 255" },
  uint16: { min: 0, max: 65_535, integer: true, label: "uint16 integer between 0 and 65535" },
  uint32: { min: 0, max: 4_294_967_295, integer: true, label: "uint32 integer between 0 and 4294967295" },
  int16: { min: -32_768, max: 32_767, integer: true, label: "int16 integer between -32768 and 32767" },
  int32: { min: -2_147_483_648, max: 2_147_483_647, integer: true, label: "int32 integer between -2147483648 and 2147483647" },
  float32: { integer: false, label: "finite float32 number" },
  float64: { integer: false, label: "finite float64 number" },
};

export function formatValue(value: unknown): string {
  if (typeof value === "string") {
    return `"${value}"`;
  }

  if (typeof value === "number" && Number.isNaN(value)) {
    return "NaN";
  }

  return String(value);
}

export function assertNonNegativeInteger(value: number, name: "limit" | "offset"): void {
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
    throw new ColQLError(
      name === "limit" ? "COLQL_INVALID_LIMIT" : "COLQL_INVALID_OFFSET",
      `Invalid ${name}: expected non-negative integer, received ${formatValue(value)}.`,
      { value },
    );
  }
}

export function assertPositiveInteger(value: number, name: "top" | "bottom"): void {
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value <= 0) {
    throw new ColQLError(
      "COLQL_INVALID_LIMIT",
      `Invalid ${name} count: expected positive integer, received ${formatValue(value)}.`,
      { value },
    );
  }
}

export function assertRowIndex(value: number, rowCount: number): void {
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value < 0 || value >= rowCount) {
    const max = Math.max(rowCount - 1, 0);
    throw new ColQLError(
      "COLQL_INVALID_ROW_INDEX",
      `Invalid row index: expected integer between 0 and ${max}, received ${formatValue(value)}.`,
      { value, rowCount },
    );
  }
}

export function assertNumericValue(columnName: string, type: NumericColumnType, value: unknown): void {
  const range = NUMERIC_RANGES[type];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ColQLError(
      "COLQL_TYPE_MISMATCH",
      `Invalid value for column "${columnName}": expected ${range.label}, received ${formatValue(value)}.`,
      { columnName, type, value },
    );
  }

  if (range.integer && !Number.isInteger(value)) {
    throw new ColQLError(
      "COLQL_TYPE_MISMATCH",
      `Invalid value for column "${columnName}": expected ${range.label}, received ${formatValue(value)}.`,
      { columnName, type, value },
    );
  }

  if ((range.min !== undefined && value < range.min) || (range.max !== undefined && value > range.max)) {
    throw new ColQLError(
      "COLQL_OUT_OF_RANGE",
      `Invalid value for column "${columnName}": expected ${range.label}, received ${formatValue(value)}.`,
      { columnName, type, value },
    );
  }
}

export function assertBooleanValue(columnName: string, value: unknown): void {
  if (typeof value !== "boolean") {
    throw new ColQLError(
      "COLQL_TYPE_MISMATCH",
      `Invalid value for column "${columnName}": expected boolean, received ${formatValue(value)}.`,
      { columnName, value },
    );
  }
}

export function assertDictionaryValues(values: readonly unknown[], columnName = "dictionary"): void {
  if (!Array.isArray(values) || values.length === 0) {
    throw new ColQLError("COLQL_INVALID_SCHEMA", `Invalid dictionary column "${columnName}": expected a non-empty array of values.`);
  }

  const seen = new Set<unknown>();
  for (const value of values) {
    if (typeof value !== "string") {
      throw new ColQLError(
        "COLQL_INVALID_SCHEMA",
        `Invalid dictionary column "${columnName}": values must be strings, received ${formatValue(value)}.`,
        { value },
      );
    }

    if (seen.has(value)) {
      throw new ColQLError("COLQL_DUPLICATE_COLUMN", `Duplicate dictionary value ${formatValue(value)} in column "${columnName}".`);
    }

    seen.add(value);
  }
}

export function assertDictionaryValue(columnName: string, values: readonly string[], value: unknown): void {
  if (typeof value !== "string" || !values.includes(value)) {
    throw new ColQLError(
      "COLQL_UNKNOWN_VALUE",
      `Invalid dictionary value for column "${columnName}": expected one of ${JSON.stringify(values)}, received ${formatValue(value)}.`,
      { columnName, value, values },
    );
  }
}

export function validateColumnValue(columnName: string, definition: ColumnDefinition, value: unknown): void {
  if (definition.kind === "numeric") {
    assertNumericValue(columnName, definition.type, value);
    return;
  }

  if (definition.kind === "boolean") {
    assertBooleanValue(columnName, value);
    return;
  }

  assertDictionaryValue(columnName, definition.values, value);
}

export function assertValidSchema(schema: unknown): asserts schema is Schema {
  if (typeof schema !== "object" || schema === null || Array.isArray(schema)) {
    throw new ColQLError("COLQL_INVALID_SCHEMA", "Invalid schema: expected a non-null object of column definitions.");
  }

  const keys = Object.keys(schema);
  if (keys.length === 0) {
    throw new ColQLError("COLQL_INVALID_SCHEMA", "Invalid schema: expected at least one column.");
  }

  for (const key of keys) {
    const definition = (schema as Record<string, unknown>)[key] as Partial<ColumnDefinition> | undefined;
    if (typeof definition !== "object" || definition === null || typeof definition.kind !== "string") {
      throw new ColQLError("COLQL_INVALID_COLUMN", `Invalid column "${key}": expected a ColQL column definition.`);
    }

    if (definition.kind === "numeric") {
      if (typeof definition.type !== "string" || !(definition.type in NUMERIC_RANGES)) {
        throw new ColQLError("COLQL_INVALID_COLUMN_TYPE", `Invalid column "${key}": unknown numeric type ${formatValue(definition.type)}.`);
      }
      continue;
    }

    if (definition.kind === "boolean") {
      continue;
    }

    if (definition.kind === "dictionary") {
      assertDictionaryValues((definition as { values?: readonly unknown[] }).values ?? [], key);
      continue;
    }

    throw new ColQLError("COLQL_INVALID_COLUMN_TYPE", `Invalid column "${key}": unsupported column kind ${formatValue(definition.kind)}.`);
  }
}

export function assertColumnExists(schema: Schema, columnName: PropertyKey, context = "column"): asserts columnName is string {
  if (typeof columnName !== "string" || !(columnName in schema)) {
    throw new ColQLError("COLQL_INVALID_COLUMN", `Unknown column "${String(columnName)}"${context ? ` in ${context}` : ""}.`, { columnName });
  }
}

export function assertOperator(operator: unknown): asserts operator is Operator {
  if (typeof operator !== "string" || !SUPPORTED_OPERATORS.includes(operator as Operator)) {
    throw new ColQLError(
      "COLQL_INVALID_OPERATOR",
      `Invalid operator "${String(operator)}". Supported operators: ${SUPPORTED_OPERATORS.join(", ")}.`,
      { operator },
    );
  }
}

export function assertOperatorAllowed(columnName: string, definition: ColumnDefinition, operator: Operator): void {
  if (COMPARISON_OPERATORS.includes(operator) && definition.kind !== "numeric") {
    throw new ColQLError(
      "COLQL_INVALID_OPERATOR",
      `Operator "${operator}" is not supported for ${definition.kind} column "${columnName}".`,
      { columnName, operator, kind: definition.kind },
    );
  }
}

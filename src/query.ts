import type { Table } from "./table";
import type { ColumnValue, Filter, Operator, RowForSchema, Schema, SelectedRow } from "./types";

type InternalFilter = ReturnType<Table<Schema>["createFilter"]>;

type ValueForOperator<TValue, TOperator extends Operator> = TOperator extends "in" | "not in"
  ? readonly TValue[]
  : TValue;

export class Query<TSchema extends Schema, TResult> implements Iterable<TResult> {
  private readonly filters: readonly InternalFilter[];
  private readonly selectedColumns?: readonly (keyof TSchema)[];
  private readonly limitValue?: number;
  private readonly offsetValue: number;

  constructor(
    private readonly source: Table<TSchema>,
    options: {
      filters?: readonly InternalFilter[];
      selectedColumns?: readonly (keyof TSchema)[];
      limitValue?: number;
      offsetValue?: number;
    } = {},
  ) {
    this.filters = options.filters ?? [];
    this.selectedColumns = options.selectedColumns;
    this.limitValue = options.limitValue;
    this.offsetValue = options.offsetValue ?? 0;
  }

  where<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): Query<TSchema, TResult> {
    const nextFilter = this.source.createFilter({ columnName, operator, value });
    return new Query(this.source, {
      filters: [...this.filters, nextFilter],
      selectedColumns: this.selectedColumns,
      limitValue: this.limitValue,
      offsetValue: this.offsetValue,
    });
  }

  select<const Keys extends readonly (keyof TSchema)[]>(
    columns: Keys,
  ): Query<TSchema, SelectedRow<TSchema, Keys>> {
    this.validateSelectedColumns(columns);
    return new Query(this.source, {
      filters: this.filters,
      selectedColumns: columns,
      limitValue: this.limitValue,
      offsetValue: this.offsetValue,
    });
  }

  limit(n: number): Query<TSchema, TResult> {
    this.assertNonNegativeInteger(n, "limit");
    return new Query(this.source, {
      filters: this.filters,
      selectedColumns: this.selectedColumns,
      limitValue: n,
      offsetValue: this.offsetValue,
    });
  }

  offset(n: number): Query<TSchema, TResult> {
    this.assertNonNegativeInteger(n, "offset");
    return new Query(this.source, {
      filters: this.filters,
      selectedColumns: this.selectedColumns,
      limitValue: this.limitValue,
      offsetValue: n,
    });
  }

  toArray(): TResult[] {
    const rows: TResult[] = [];
    this.forEach((row) => rows.push(row));
    return rows;
  }

  first(): TResult | undefined {
    const iterator = this[Symbol.iterator]();
    const next = iterator.next();
    return next.done ? undefined : next.value;
  }

  count(): number {
    let seen = 0;
    let produced = 0;

    for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
      if (!this.matches(rowIndex)) {
        continue;
      }

      if (seen < this.offsetValue) {
        seen += 1;
        continue;
      }

      if (this.limitValue !== undefined && produced >= this.limitValue) {
        break;
      }

      produced += 1;
    }

    return produced;
  }

  forEach(callback: (row: TResult, index: number) => void): void {
    let index = 0;
    for (const row of this) {
      callback(row, index);
      index += 1;
    }
  }

  *[Symbol.iterator](): Iterator<TResult> {
    let seen = 0;
    let produced = 0;

    for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
      if (!this.matches(rowIndex)) {
        continue;
      }

      if (seen < this.offsetValue) {
        seen += 1;
        continue;
      }

      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      produced += 1;
      yield this.source.materializeRow(rowIndex, this.selectedColumns) as TResult;
    }
  }

  private matches(rowIndex: number): boolean {
    for (const filter of this.filters) {
      if (!this.source.matchesFilter(rowIndex, filter)) {
        return false;
      }
    }

    return true;
  }

  private validateSelectedColumns(columns: readonly (keyof TSchema)[]): void {
    for (const key of columns) {
      if (!(key in this.source.schema)) {
        throw new Error(`Unknown selected column "${String(key)}".`);
      }
    }
  }

  private assertNonNegativeInteger(value: number, name: "limit" | "offset"): void {
    if (!Number.isInteger(value) || value < 0) {
      throw new Error(`${name} must be a non-negative integer. Received ${value}.`);
    }
  }
}

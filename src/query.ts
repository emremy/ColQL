import { BinaryHeap, type HeapItem } from "./heap";
import type { Table } from "./table";
import type { ColumnValue, Filter, NumericColumnKey, Operator, RowForSchema, Schema, SelectedRow } from "./types";
import { ColQLError } from "./errors";
import { assertColumnExists, assertNonNegativeInteger, assertPositiveInteger } from "./validation";

type InternalFilter = ReturnType<Table<Schema>["createFilter"]>;

type ValueForOperator<TValue, TOperator extends Operator> = TOperator extends "in" | "not in"
  ? readonly TValue[]
  : TValue;

export class Query<TSchema extends Schema, TResult> implements Iterable<TResult> {
  private readonly filters: readonly InternalFilter[];
  private readonly plannedFilters: readonly InternalFilter[];
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
    this.plannedFilters = this.orderFilters(this.filters);
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

  whereIn<Key extends keyof TSchema>(
    columnName: Key,
    values: readonly ColumnValue<TSchema[Key]>[],
  ): Query<TSchema, TResult> {
    return this.where(columnName, "in", values);
  }

  whereNotIn<Key extends keyof TSchema>(
    columnName: Key,
    values: readonly ColumnValue<TSchema[Key]>[],
  ): Query<TSchema, TResult> {
    return this.where(columnName, "not in", values);
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
    assertNonNegativeInteger(n, "limit");
    return new Query(this.source, {
      filters: this.filters,
      selectedColumns: this.selectedColumns,
      limitValue: n,
      offsetValue: this.offsetValue,
    });
  }

  offset(n: number): Query<TSchema, TResult> {
    assertNonNegativeInteger(n, "offset");
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

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        break;
      }

      this.source.recordRowScan();
      if (!this.matches(rowIndex)) {
        continue;
      }

      if (seen < this.offsetValue) {
        seen += 1;
        continue;
      }

      produced += 1;
    }

    return produced;
  }

  size(): number {
    return this.count();
  }

  isEmpty(): boolean {
    for (const _rowIndex of this.matchingRowIndexes()) {
      return false;
    }

    return true;
  }

  sum<Key extends NumericColumnKey<TSchema>>(columnName: Key): number {
    this.assertNumericColumn(columnName);
    let total = 0;

    for (const rowIndex of this.matchingRowIndexes()) {
      total += this.source.getNumericValue(rowIndex, columnName);
    }

    return total;
  }

  avg<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
    this.assertNumericColumn(columnName);
    let total = 0;
    let count = 0;

    for (const rowIndex of this.matchingRowIndexes()) {
      total += this.source.getNumericValue(rowIndex, columnName);
      count += 1;
    }

    return count === 0 ? undefined : total / count;
  }

  min<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
    this.assertNumericColumn(columnName);
    let result: number | undefined;

    for (const rowIndex of this.matchingRowIndexes()) {
      const value = this.source.getNumericValue(rowIndex, columnName);
      result = result === undefined || value < result ? value : result;
    }

    return result;
  }

  max<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
    this.assertNumericColumn(columnName);
    let result: number | undefined;

    for (const rowIndex of this.matchingRowIndexes()) {
      const value = this.source.getNumericValue(rowIndex, columnName);
      result = result === undefined || value > result ? value : result;
    }

    return result;
  }

  top<Key extends NumericColumnKey<TSchema>>(n: number, columnName: Key): TResult[] {
    assertPositiveInteger(n, "top");
    this.assertNumericColumn(columnName);
    return this.topOrBottom(n, columnName, "top");
  }

  bottom<Key extends NumericColumnKey<TSchema>>(n: number, columnName: Key): TResult[] {
    assertPositiveInteger(n, "bottom");
    this.assertNumericColumn(columnName);
    return this.topOrBottom(n, columnName, "bottom");
  }

  forEach(callback: (row: TResult, index: number) => void): void {
    let index = 0;
    for (const row of this) {
      callback(row, index);
      index += 1;
    }
  }

  stream(): Iterable<TResult> {
    return this;
  }

  __debugPlan(): ReturnType<Table<TSchema>["getIndexDebugPlan"]> {
    return this.source.getIndexDebugPlan(this.filters);
  }

  *[Symbol.iterator](): Iterator<TResult> {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      this.source.recordRowScan();
      if (!this.matches(rowIndex)) {
        continue;
      }

      if (seen < this.offsetValue) {
        seen += 1;
        continue;
      }

      produced += 1;
      yield this.source.materializeRow(rowIndex, this.selectedColumns) as TResult;
    }
  }

  private *matchingRowIndexes(): IterableIterator<number> {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      this.source.recordRowScan();
      if (!this.matches(rowIndex)) {
        continue;
      }

      if (seen < this.offsetValue) {
        seen += 1;
        continue;
      }

      produced += 1;
      yield rowIndex;
    }
  }

  private *rowIndexesToScan(): IterableIterator<number> {
    const plan = this.source.getIndexedCandidatePlan(this.filters);
    if (plan !== undefined) {
      for (const rowIndex of plan.rowIndexes) {
        yield rowIndex;
      }
      return;
    }

    for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
      yield rowIndex;
    }
  }

  private topOrBottom<Key extends NumericColumnKey<TSchema>>(
    n: number,
    columnName: Key,
    direction: "top" | "bottom",
  ): TResult[] {
    if (n === 0) {
      return [];
    }

    const heap = new BinaryHeap((left, right) =>
      direction === "top" ? left.value - right.value : right.value - left.value,
    );

    for (const rowIndex of this.matchingRowIndexes()) {
      const value = this.source.getNumericValue(rowIndex, columnName);
      const item: HeapItem = { rowIndex, value };

      if (heap.size < n) {
        heap.push(item);
        continue;
      }

      const root = heap.peek();
      if (root === undefined) {
        heap.push(item);
        continue;
      }

      const shouldReplace = direction === "top" ? value > root.value : value < root.value;
      if (shouldReplace) {
        heap.replaceRoot(item);
      }
    }

    return heap
      .toArray()
      .sort((left, right) => (direction === "top" ? right.value - left.value : left.value - right.value))
      .map((item) => this.source.materializeRow(item.rowIndex, this.selectedColumns) as TResult);
  }

  private matches(rowIndex: number): boolean {
    for (const filter of this.plannedFilters) {
      if (!this.source.matchesFilter(rowIndex, filter)) {
        return false;
      }
    }

    return true;
  }

  private orderFilters(filters: readonly InternalFilter[]): readonly InternalFilter[] {
    if (filters.length < 2) {
      return filters;
    }

    return [...filters].sort((left, right) => this.filterCost(left) - this.filterCost(right));
  }

  private filterCost(filter: InternalFilter): number {
    switch (filter.operator) {
      case "=":
        return 0;
      case "!=":
        return 1;
      case ">":
      case ">=":
      case "<":
      case "<=":
        return 2;
      case "in":
        return 3;
      case "not in":
        return 4;
    }
  }

  private validateSelectedColumns(columns: readonly (keyof TSchema)[]): void {
    if (!Array.isArray(columns) || columns.length === 0) {
      throw new ColQLError("COLQL_INVALID_COLUMN", "Invalid select: expected a non-empty array of column names.");
    }

    const seen = new Set<keyof TSchema>();
    for (const key of columns) {
      assertColumnExists(this.source.schema, key, "select()");
      if (seen.has(key)) {
        throw new ColQLError("COLQL_DUPLICATE_COLUMN", `Duplicate column "${String(key)}" in select().`);
      }
      seen.add(key);
    }
  }

  private assertNumericColumn(columnName: keyof TSchema): void {
    assertColumnExists(this.source.schema, columnName, "aggregation");

    if (this.source.schema[columnName].kind !== "numeric") {
      throw new ColQLError(
        "COLQL_INVALID_COLUMN_TYPE",
        `Aggregation requires a numeric column, received ${this.source.schema[columnName].kind} column "${String(columnName)}".`,
      );
    }
  }
}

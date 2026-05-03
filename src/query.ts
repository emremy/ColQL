import { BinaryHeap, type HeapItem } from "./heap";
import type { Table } from "./table";
import type { ColumnValue, Filter, MutationResult, NumericColumnKey, ObjectWherePredicate, Operator, QueryExplainPlan, QueryExplainReasonCode, RowForSchema, RowPredicate, Schema, SelectedRow } from "./types";
import type { IndexExplainPlan } from "./indexing/index-manager";
import { ColQLError } from "./errors";
import { assertColumnExists, assertNonNegativeInteger, assertPositiveInteger } from "./validation";

type InternalFilter = ReturnType<Table<Schema>["createFilter"]>;

type ValueForOperator<TValue, TOperator extends Operator> = TOperator extends "in" | "not in"
  ? readonly TValue[]
  : TValue;

type MutationSource<TSchema extends Schema> = {
  updateRows(rowIndexes: readonly number[], partialRow: Partial<RowForSchema<TSchema>>): MutationResult;
  deleteRows(rowIndexes: readonly number[]): MutationResult;
};

type ExplainPlanSource = {
  getIndexExplainPlan(filters: readonly InternalFilter[]): IndexExplainPlan;
};

export class Query<TSchema extends Schema, TResult> implements Iterable<TResult> {
  private readonly filters: readonly InternalFilter[];
  private readonly plannedFilters: readonly InternalFilter[];
  private readonly rowPredicates: readonly RowPredicate<TSchema>[];
  private readonly selectedColumns?: readonly (keyof TSchema)[];
  private readonly limitValue?: number;
  private readonly offsetValue: number;

  constructor(
    private readonly source: Table<TSchema>,
    options: {
      filters?: readonly InternalFilter[];
      rowPredicates?: readonly RowPredicate<TSchema>[];
      selectedColumns?: readonly (keyof TSchema)[];
      limitValue?: number;
      offsetValue?: number;
    } = {},
  ) {
    this.filters = options.filters ?? [];
    this.plannedFilters = this.orderFilters(this.filters);
    this.rowPredicates = options.rowPredicates ?? [];
    this.selectedColumns = options.selectedColumns;
    this.limitValue = options.limitValue;
    this.offsetValue = options.offsetValue ?? 0;
  }

  where(predicate: ObjectWherePredicate<TSchema>): Query<TSchema, TResult>;
  where<Key extends keyof TSchema, TOperator extends Operator>(
    columnName: Key,
    operator: TOperator,
    value: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): Query<TSchema, TResult>;
  where<Key extends keyof TSchema, TOperator extends Operator>(
    columnNameOrPredicate: Key | ObjectWherePredicate<TSchema>,
    operator?: TOperator,
    value?: ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
  ): Query<TSchema, TResult> {
    if (arguments.length === 1) {
      return this.whereObject(columnNameOrPredicate as ObjectWherePredicate<TSchema>);
    }

    const columnName = columnNameOrPredicate as Key;
    const nextFilter = this.source.createFilter({
      columnName,
      operator: operator as TOperator,
      value: value as ValueForOperator<ColumnValue<TSchema[Key]>, TOperator>,
    });
    return new Query(this.source, {
      filters: [...this.filters, nextFilter],
      rowPredicates: this.rowPredicates,
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

  filter(predicate: RowPredicate<TSchema>): Query<TSchema, TResult> {
    return new Query(this.source, {
      filters: this.filters,
      rowPredicates: [...this.rowPredicates, predicate],
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
      rowPredicates: this.rowPredicates,
      selectedColumns: columns,
      limitValue: this.limitValue,
      offsetValue: this.offsetValue,
    });
  }

  limit(n: number): Query<TSchema, TResult> {
    assertNonNegativeInteger(n, "limit");
    return new Query(this.source, {
      filters: this.filters,
      rowPredicates: this.rowPredicates,
      selectedColumns: this.selectedColumns,
      limitValue: n,
      offsetValue: this.offsetValue,
    });
  }

  offset(n: number): Query<TSchema, TResult> {
    assertNonNegativeInteger(n, "offset");
    return new Query(this.source, {
      filters: this.filters,
      rowPredicates: this.rowPredicates,
      selectedColumns: this.selectedColumns,
      limitValue: this.limitValue,
      offsetValue: n,
    });
  }

  toArray(): TResult[] {
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.collectArray());
    }

    return this.collectArray();
  }

  private collectArray(): TResult[] {
    const rows: TResult[] = [];
    this.forEachUninstrumented((row) => rows.push(row));
    return rows;
  }

  first(): TResult | undefined {
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.firstUninstrumented());
    }

    return this.firstUninstrumented();
  }

  private firstUninstrumented(): TResult | undefined {
    if (this.rowPredicates.length === 0) {
      const rowIndex = this.firstStructuredRowIndex();
      return rowIndex === undefined
        ? undefined
        : this.source.materializeRow(rowIndex, this.selectedColumns) as TResult;
    }

    const iterator = this[Symbol.iterator]();
    const next = iterator.next();
    return next.done ? undefined : next.value;
  }

  private firstStructuredRowIndex(): number | undefined {
    let seen = 0;
    let scanned = 0;

    try {
      const plan = this.source.getIndexedCandidatePlan(this.filters);
      if (plan !== undefined) {
        for (const rowIndex of plan.rowIndexes) {
          scanned += 1;
          if (!this.matchesStructuredFilters(rowIndex)) {
            continue;
          }

          if (seen < this.offsetValue) {
            seen += 1;
            continue;
          }

          return rowIndex;
        }

        return undefined;
      }

      for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
        scanned += 1;
        if (!this.matchesStructuredFilters(rowIndex)) {
          continue;
        }

        if (seen < this.offsetValue) {
          seen += 1;
          continue;
        }

        return rowIndex;
      }

      return undefined;
    } finally {
      this.source.recordRowScans(scanned);
    }
  }

  count(): number {
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.countUninstrumented());
    }

    return this.countUninstrumented();
  }

  private countUninstrumented(): number {
    if (this.rowPredicates.length === 0) {
      return this.countStructuredOnly();
    }

    return this.countWithRowPredicates();
  }

  private countStructuredOnly(): number {
    let seen = 0;
    let produced = 0;
    let scanned = 0;

    try {
      const plan = this.source.getIndexedCandidatePlan(this.filters);
      if (plan !== undefined) {
        for (const rowIndex of plan.rowIndexes) {
          if (this.limitValue !== undefined && produced >= this.limitValue) {
            break;
          }

          scanned += 1;
          if (!this.matchesStructuredFilters(rowIndex)) {
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

      for (let rowIndex = 0; rowIndex < this.source.rowCount; rowIndex += 1) {
        if (this.limitValue !== undefined && produced >= this.limitValue) {
          break;
        }

        scanned += 1;
        if (!this.matchesStructuredFilters(rowIndex)) {
          continue;
        }

        if (seen < this.offsetValue) {
          seen += 1;
          continue;
        }

        produced += 1;
      }

      return produced;
    } finally {
      this.source.recordRowScans(scanned);
    }
  }

  private countWithRowPredicates(): number {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        break;
      }

      this.source.recordRowScan();
      if (!this.matchesWithRowPredicates(rowIndex)) {
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
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.isEmptyUninstrumented());
    }

    return this.isEmptyUninstrumented();
  }

  private isEmptyUninstrumented(): boolean {
    if (this.rowPredicates.length === 0) {
      return this.firstStructuredRowIndex() === undefined;
    }

    for (const _rowIndex of this.matchingRowIndexes()) {
      return false;
    }

    return true;
  }

  sum<Key extends NumericColumnKey<TSchema>>(columnName: Key): number {
    this.assertNumericColumn(columnName);
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.sumUninstrumented(columnName));
    }

    return this.sumUninstrumented(columnName);
  }

  private sumUninstrumented<Key extends NumericColumnKey<TSchema>>(columnName: Key): number {
    let total = 0;

    for (const rowIndex of this.matchingRowIndexes()) {
      total += this.source.getNumericValue(rowIndex, columnName);
    }

    return total;
  }

  avg<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
    this.assertNumericColumn(columnName);
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.avgUninstrumented(columnName));
    }

    return this.avgUninstrumented(columnName);
  }

  private avgUninstrumented<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
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
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.minUninstrumented(columnName));
    }

    return this.minUninstrumented(columnName);
  }

  private minUninstrumented<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
    let result: number | undefined;

    for (const rowIndex of this.matchingRowIndexes()) {
      const value = this.source.getNumericValue(rowIndex, columnName);
      result = result === undefined || value < result ? value : result;
    }

    return result;
  }

  max<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
    this.assertNumericColumn(columnName);
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.maxUninstrumented(columnName));
    }

    return this.maxUninstrumented(columnName);
  }

  private maxUninstrumented<Key extends NumericColumnKey<TSchema>>(columnName: Key): number | undefined {
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
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.topOrBottom(n, columnName, "top"));
    }

    return this.topOrBottom(n, columnName, "top");
  }

  bottom<Key extends NumericColumnKey<TSchema>>(n: number, columnName: Key): TResult[] {
    assertPositiveInteger(n, "bottom");
    this.assertNumericColumn(columnName);
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.topOrBottom(n, columnName, "bottom"));
    }

    return this.topOrBottom(n, columnName, "bottom");
  }

  update(partialRow: Partial<RowForSchema<TSchema>>): MutationResult {
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.updateUninstrumented(partialRow));
    }

    return this.updateUninstrumented(partialRow);
  }

  delete(): MutationResult {
    if (this.source.hasQueryHook()) {
      return this.runTerminal(() => this.deleteUninstrumented());
    }

    return this.deleteUninstrumented();
  }

  forEach(callback: (row: TResult, index: number) => void): void {
    if (this.source.hasQueryHook()) {
      this.runTerminal(() => this.forEachUninstrumented(callback));
      return;
    }

    this.forEachUninstrumented(callback);
  }

  private forEachUninstrumented(callback: (row: TResult, index: number) => void): void {
    let index = 0;
    for (const row of this) {
      callback(row, index);
      index += 1;
    }
  }

  stream(): Iterable<TResult> {
    return this;
  }

  explain(): QueryExplainPlan {
    const predicates = this.filters.length + this.rowPredicates.length;
    const predicateOrder = this.plannedFilters.map((filter) =>
      `${filter.columnName} ${filter.operator}`,
    );

    if (this.rowPredicates.length > 0) {
      return {
        scanType: "full",
        indexesUsed: [],
        predicates,
        predicateOrder,
        projectionPushdown: this.selectedColumns !== undefined,
        reasonCode: "CALLBACK_PREDICATE_REQUIRES_FULL_SCAN",
        reason: this.reasonFor("CALLBACK_PREDICATE_REQUIRES_FULL_SCAN"),
      };
    }

    const plan = (this.source as unknown as ExplainPlanSource).getIndexExplainPlan(this.filters);
    const reasonCode = plan.reasonCode;
    return {
      scanType: plan.mode === "index" ? "index" : "full",
      indexesUsed:
        plan.mode === "index" ? [`${plan.source}:${plan.column}`] : [],
      predicates,
      predicateOrder,
      projectionPushdown: this.selectedColumns !== undefined,
      ...(plan.candidateCount !== undefined
        ? { candidateRows: plan.candidateCount }
        : {}),
      ...(plan.mode === "index" ? { indexState: plan.indexState } : {}),
      ...(reasonCode !== undefined ? { reasonCode } : {}),
      ...(reasonCode !== undefined ? { reason: this.reasonFor(reasonCode) } : {}),
    };
  }

  __debugPlan(): ReturnType<Table<TSchema>["getIndexDebugPlan"]> {
    // Internal diagnostics retained for existing tests/debugging. Application
    // code should use the public explain() contract instead.
    if (this.rowPredicates.length > 0) {
      return this.source.getIndexDebugPlan([]);
    }

    return this.source.getIndexDebugPlan(this.filters);
  }

  *[Symbol.iterator](): Iterator<TResult> {
    if (this.rowPredicates.length === 0) {
      yield* this.iterateStructuredOnly();
      return;
    }

    yield* this.iterateWithRowPredicates();
  }

  private *iterateStructuredOnly(): IterableIterator<TResult> {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      this.source.recordRowScan();
      if (!this.matchesStructuredFilters(rowIndex)) {
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

  private *iterateWithRowPredicates(): IterableIterator<TResult> {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      this.source.recordRowScan();
      if (!this.matchesWithRowPredicates(rowIndex)) {
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
    if (this.rowPredicates.length === 0) {
      yield* this.matchingStructuredRowIndexes();
      return;
    }

    yield* this.matchingRowIndexesWithPredicates();
  }

  private *matchingStructuredRowIndexes(): IterableIterator<number> {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      this.source.recordRowScan();
      if (!this.matchesStructuredFilters(rowIndex)) {
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

  private *matchingRowIndexesWithPredicates(): IterableIterator<number> {
    let seen = 0;
    let produced = 0;

    for (const rowIndex of this.rowIndexesToScan()) {
      if (this.limitValue !== undefined && produced >= this.limitValue) {
        return;
      }

      this.source.recordRowScan();
      if (!this.matchesWithRowPredicates(rowIndex)) {
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

  private snapshotMatchingRowIndexes(): number[] {
    return [...this.matchingRowIndexes()];
  }

  private updateUninstrumented(partialRow: Partial<RowForSchema<TSchema>>): MutationResult {
    return (this.source as unknown as MutationSource<TSchema>).updateRows(this.snapshotMatchingRowIndexes(), partialRow);
  }

  private deleteUninstrumented(): MutationResult {
    return (this.source as unknown as MutationSource<TSchema>).deleteRows(this.snapshotMatchingRowIndexes());
  }

  private runTerminal<T>(operation: () => T): T {
    const startScannedRows = this.source.scannedRowCount;
    const start = Date.now();
    const indexUsed = this.usesIndexPlan();
    const result = operation();
    this.source.notifyQuery({
      duration: Date.now() - start,
      rowsScanned: this.source.scannedRowCount - startScannedRows,
      indexUsed,
    });
    return result;
  }

  private usesIndexPlan(): boolean {
    return this.rowPredicates.length === 0 && this.source.getIndexDebugPlan(this.filters).mode === "index";
  }

  private reasonFor(reasonCode: QueryExplainReasonCode): string {
    switch (reasonCode) {
      case "NO_PREDICATES":
        return "Query has no structured predicates, so ColQL will scan rows in order.";
      case "NO_INDEX_FOR_COLUMN":
        return "No usable equality index exists for the indexed predicate column.";
      case "RANGE_QUERY_WITHOUT_SORTED_INDEX":
        return "Range predicates require a sorted index to avoid a full scan.";
      case "INDEX_CANDIDATE_SET_TOO_LARGE":
        return "The best index candidate set is too large, so a scan is expected to be cheaper.";
      case "CALLBACK_PREDICATE_REQUIRES_FULL_SCAN":
        return "Callback predicates are not index-aware and require a full scan.";
      case "INDEX_DIRTY_WOULD_REBUILD_ON_EXECUTION":
        return "The selected index is dirty; executing the query would lazily rebuild it before use.";
      case "UNSUPPORTED_INDEX_OPERATOR":
        return "The predicate operator is not supported by equality or sorted indexes.";
    }
  }

  private whereObject(predicate: ObjectWherePredicate<TSchema>): Query<TSchema, TResult> {
    this.assertObjectPredicate(predicate);

    let next: Query<TSchema, TResult> = this;
    for (const [columnName, condition] of Object.entries(predicate)) {
      if (condition === undefined) {
        continue;
      }

      assertColumnExists(this.source.schema, columnName, "where()");
      next = this.applyObjectCondition(next, columnName as keyof TSchema, condition);
    }

    if (next === this) {
      throw new ColQLError(
        "COLQL_INVALID_PREDICATE",
        "Invalid where predicate: expected at least one column condition.",
      );
    }

    return next;
  }

  private assertObjectPredicate(predicate: unknown): asserts predicate is ObjectWherePredicate<TSchema> {
    if (typeof predicate !== "object" || predicate === null || Array.isArray(predicate)) {
      throw new ColQLError(
        "COLQL_INVALID_PREDICATE",
        "Invalid where predicate: expected a non-null object.",
      );
    }

    if (Object.keys(predicate).length === 0) {
      throw new ColQLError(
        "COLQL_INVALID_PREDICATE",
        "Invalid where predicate: expected at least one column condition.",
      );
    }
  }

  private applyObjectCondition(
    query: Query<TSchema, TResult>,
    columnName: keyof TSchema,
    condition: unknown,
  ): Query<TSchema, TResult> {
    if (typeof condition !== "object" || condition === null || Array.isArray(condition)) {
      return query.where(columnName, "=", condition as ColumnValue<TSchema[typeof columnName]>);
    }

    const operatorEntries = Object.entries(condition);
    if (operatorEntries.length === 0) {
      throw new ColQLError(
        "COLQL_INVALID_PREDICATE",
        `Invalid where predicate for column "${String(columnName)}": expected at least one operator.`,
      );
    }

    let next = query;
    for (const [operatorName, operatorValue] of operatorEntries) {
      const operator = this.objectOperator(operatorName, columnName);
      next = next.where(
        columnName,
        operator,
        operatorValue as ValueForOperator<ColumnValue<TSchema[typeof columnName]>, typeof operator>,
      );
    }

    return next;
  }

  private objectOperator(operatorName: string, columnName: keyof TSchema): Extract<Operator, "=" | ">" | ">=" | "<" | "<=" | "in"> {
    const isRangeOperator = operatorName === "gt" || operatorName === "gte" || operatorName === "lt" || operatorName === "lte";
    if (isRangeOperator && this.source.schema[columnName].kind !== "numeric") {
      throw new ColQLError(
        "COLQL_INVALID_PREDICATE",
        `Invalid where predicate operator "${operatorName}" for ${this.source.schema[columnName].kind} column "${String(columnName)}".`,
        { columnName: String(columnName), operator: operatorName, kind: this.source.schema[columnName].kind },
      );
    }

    switch (operatorName) {
      case "eq":
        return "=";
      case "gt":
        return ">";
      case "gte":
        return ">=";
      case "lt":
        return "<";
      case "lte":
        return "<=";
      case "in":
        return "in";
      default:
        throw new ColQLError(
          "COLQL_INVALID_PREDICATE",
          `Invalid where predicate operator "${operatorName}" for column "${String(columnName)}".`,
          { columnName: String(columnName), operator: operatorName },
        );
    }
  }

  private *rowIndexesToScan(): IterableIterator<number> {
    if (this.rowPredicates.length > 0) {
      yield* this.fullScanRowIndexes();
      return;
    }

    const plan = this.source.getIndexedCandidatePlan(this.filters);
    if (plan !== undefined) {
      for (const rowIndex of plan.rowIndexes) {
        yield rowIndex;
      }
      return;
    }

    yield* this.fullScanRowIndexes();
  }

  private *fullScanRowIndexes(): IterableIterator<number> {
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

  private matchesStructuredFilters(rowIndex: number): boolean {
    for (const filter of this.plannedFilters) {
      if (!this.source.matchesFilter(rowIndex, filter)) {
        return false;
      }
    }

    return true;
  }

  private matchesWithRowPredicates(rowIndex: number): boolean {
    if (!this.matchesStructuredFilters(rowIndex)) {
      return false;
    }

    for (const predicate of this.rowPredicates) {
      if (!predicate(this.source.materializeRow(rowIndex))) {
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

import { ColQLError } from "../errors";
import type { ColumnDefinition } from "../types";
import { EqualityIndex, type EqualityIndexStats, type IndexableValue } from "./equality-index";
import { SortedIndex, type RangeOperator, type SortedIndexStats } from "./sorted-index";
import { UniqueIndex, type UniqueIndexStats, type UniqueIndexValue } from "./unique-index";

const DEFAULT_INDEX_SELECTIVITY_THRESHOLD = 0.4;

export type IndexFilter = {
  readonly columnName: string;
  readonly operator: string;
  readonly value: number | boolean | readonly (number | boolean)[];
};

export type IndexCandidatePlan = {
  readonly source: "equality" | "sorted";
  readonly column: string;
  readonly operator: "=" | "in" | RangeOperator;
  readonly candidateCount: number;
  readonly rowCount: number;
  readonly threshold: number;
  readonly rowIndexes: Iterable<number>;
};

export type IndexDebugPlan =
  | {
      readonly mode: "index";
      readonly source: "equality" | "sorted";
      readonly column: string;
      readonly operator: "=" | "in" | RangeOperator;
      readonly candidateCount: number;
      readonly rowCount: number;
      readonly threshold: number;
    }
  | {
      readonly mode: "scan";
      readonly source?: "equality" | "sorted";
      readonly column?: string;
      readonly operator?: "=" | "in" | RangeOperator;
      readonly candidateCount?: number;
      readonly rowCount: number;
      readonly threshold: number;
    };

type EqualityCandidateEstimate = {
  readonly source: "equality";
  readonly column: string;
  readonly operator: "=" | "in";
  readonly index: EqualityIndex;
  readonly value: IndexableValue | readonly IndexableValue[];
  readonly candidateCount: number;
};

type SortedCandidateEstimate = {
  readonly source: "sorted";
  readonly column: string;
  readonly operator: RangeOperator;
  readonly index: SortedIndex;
  readonly value: number;
  readonly candidateCount: number;
  readonly bounds: ReturnType<SortedIndex["bounds"]>;
};

type CandidateEstimate = EqualityCandidateEstimate | SortedCandidateEstimate;

export class IndexManager {
  private readonly indexesByColumn = new Map<string, EqualityIndex>();
  private readonly sortedIndexesByColumn = new Map<string, SortedIndex>();
  private readonly uniqueIndexesByColumn = new Map<string, UniqueIndex>();
  private equalityDirty = false;

  create(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (this.indexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_INDEX_EXISTS", `Index already exists for column "${columnName}".`);
    }

    this.assertEqualitySupported(columnName, definition);
    const index = new EqualityIndex(columnName);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      index.add(readComparableValue(rowIndex, columnName) as IndexableValue, rowIndex);
    }

    this.indexesByColumn.set(columnName, index);
  }

  drop(columnName: string): void {
    if (!this.indexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_INDEX_NOT_FOUND", `Index not found for column "${columnName}".`);
    }
  }

  has(columnName: string): boolean {
    return this.indexesByColumn.has(columnName);
  }

  list(): string[] {
    return [...this.indexesByColumn.keys()];
  }

  stats(): EqualityIndexStats[] {
    return this.list().map((columnName) => this.indexesByColumn.get(columnName)?.stats()).filter((stats): stats is EqualityIndexStats => stats !== undefined);
  }

  rebuild(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (!this.indexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_INDEX_NOT_FOUND", `Index not found for column "${columnName}".`);
    }

    if (this.equalityDirty) {
      this.rebuildEqualityIndexes(rowCount, readComparableValue);
      return;
    }

    this.indexesByColumn.set(columnName, this.buildEqualityIndex(columnName, rowCount, readComparableValue));
  }

  rebuildAll(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    this.rebuildEqualityIndexes(rowCount, readComparableValue);
    this.rebuildSortedIndexes(rowCount, readNumericValue);
  }

  addRow(columnName: string, value: number | boolean, rowIndex: number): void {
    if (this.equalityDirty) {
      return;
    }

    const index = this.indexesByColumn.get(columnName);
    if (index === undefined) {
      return;
    }

    index.add(value as IndexableValue, rowIndex);
  }

  createSorted(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    if (this.sortedIndexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_SORTED_INDEX_EXISTS", `Sorted index already exists for column "${columnName}".`);
    }

    this.assertSortedSupported(columnName, definition);
    const index = new SortedIndex(columnName);
    index.ensureFresh(rowCount, (rowIndex) => readNumericValue(rowIndex, columnName));
    this.sortedIndexesByColumn.set(columnName, index);
  }

  dropSorted(columnName: string): void {
    if (!this.sortedIndexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_SORTED_INDEX_NOT_FOUND", `Sorted index not found for column "${columnName}".`);
    }
  }

  hasSorted(columnName: string): boolean {
    return this.sortedIndexesByColumn.has(columnName);
  }

  listSorted(): string[] {
    return [...this.sortedIndexesByColumn.keys()];
  }

  sortedStats(): SortedIndexStats[] {
    return this.listSorted()
      .map((columnName) => this.sortedIndexesByColumn.get(columnName)?.stats())
      .filter((stats): stats is SortedIndexStats => stats !== undefined);
  }

  createUnique(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (this.uniqueIndexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_EXISTS", `Unique index already exists for column "${columnName}".`);
    }

    this.assertUniqueSupported(columnName, definition);
    this.uniqueIndexesByColumn.set(columnName, this.buildUniqueIndex(columnName, rowCount, readComparableValue));
  }

  dropUnique(columnName: string): void {
    if (!this.uniqueIndexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_NOT_FOUND", `Unique index not found for column "${columnName}".`);
    }
  }

  hasUnique(columnName: string): boolean {
    return this.uniqueIndexesByColumn.has(columnName);
  }

  listUnique(): string[] {
    return [...this.uniqueIndexesByColumn.keys()];
  }

  uniqueStats(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): UniqueIndexStats[] {
    this.rebuildUniqueIfDirty(rowCount, readComparableValue);
    return this.listUnique()
      .map((columnName) => this.uniqueIndexesByColumn.get(columnName)?.stats())
      .filter((stats): stats is UniqueIndexStats => stats !== undefined);
  }

  rebuildUnique(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (!this.uniqueIndexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_NOT_FOUND", `Unique index not found for column "${columnName}".`);
    }

    this.uniqueIndexesByColumn.set(columnName, this.buildUniqueIndex(columnName, rowCount, readComparableValue));
  }

  rebuildUniqueIndexes(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    const columns = this.listUnique();
    const next = new Map<string, UniqueIndex>();
    for (const column of columns) {
      next.set(column, this.buildUniqueIndex(column, rowCount, readComparableValue));
    }

    this.uniqueIndexesByColumn.clear();
    for (const [column, index] of next) {
      this.uniqueIndexesByColumn.set(column, index);
    }
  }

  addUniqueRow(columnName: string, value: number | boolean, rowIndex: number): void {
    const index = this.uniqueIndexesByColumn.get(columnName);
    if (index === undefined || index.isDirty()) {
      return;
    }

    index.add(value as UniqueIndexValue, rowIndex);
  }

  uniqueLookup(
    columnName: string,
    value: number,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): number | undefined {
    const index = this.uniqueIndexesByColumn.get(columnName);
    if (index === undefined) {
      throw new ColQLError("COLQL_UNIQUE_INDEX_NOT_FOUND", `Unique index not found for column "${columnName}".`);
    }

    if (index.isDirty()) {
      this.rebuildUnique(columnName, rowCount, readComparableValue);
    }

    return this.uniqueIndexesByColumn.get(columnName)?.get(value);
  }

  rebuildSorted(
    columnName: string,
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    const index = this.sortedIndexesByColumn.get(columnName);
    if (index === undefined) {
      throw new ColQLError("COLQL_SORTED_INDEX_NOT_FOUND", `Sorted index not found for column "${columnName}".`);
    }

    index.markDirty();
    index.ensureFresh(rowCount, (rowIndex) => readNumericValue(rowIndex, columnName));
  }

  markSortedDirty(): void {
    for (const index of this.sortedIndexesByColumn.values()) {
      index.markDirty();
    }
  }

  markPerformanceDirty(): void {
    if (this.indexesByColumn.size > 0) {
      this.equalityDirty = true;
    }

    this.markSortedDirty();
  }

  markUniqueDirty(columns?: readonly string[]): void {
    const indexes = columns === undefined
      ? [...this.uniqueIndexesByColumn.values()]
      : columns
          .map((column) => this.uniqueIndexesByColumn.get(column))
          .filter((index): index is UniqueIndex => index !== undefined);

    for (const index of indexes) {
      index.markDirty();
    }
  }

  markDeletedRow(rowIndex: number): void {
    this.markPerformanceDirty();
    for (const index of this.uniqueIndexesByColumn.values()) {
      index.deleteRow(rowIndex);
    }
  }

  markDirty(): void {
    this.markPerformanceDirty();
    this.markUniqueDirty();
  }

  bestCandidate(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): IndexCandidatePlan | undefined {
    this.rebuildEqualityIfDirty(rowCount, readComparableValue);
    const best = this.bestCandidateEstimate(filters, rowCount, readNumericValue);
    if (best === undefined) {
      return undefined;
    }

    const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
    if (best.candidateCount > threshold) {
      return undefined;
    }

    const rowIndexes = this.rowIndexesFor(best);

    return {
      source: best.source,
      column: best.column,
      operator: best.operator,
      candidateCount: best.candidateCount,
      rowCount,
      threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
      rowIndexes,
    };
  }

  debugPlan(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): IndexDebugPlan {
    this.rebuildEqualityIfDirty(rowCount, readComparableValue);
    const best = this.bestCandidateEstimate(filters, rowCount, readNumericValue);
    if (best === undefined) {
      return { mode: "scan", rowCount, threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD };
    }

    const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
    if (best.candidateCount > threshold) {
      return {
        mode: "scan",
        source: best.source,
        column: best.column,
        operator: best.operator,
        candidateCount: best.candidateCount,
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
      };
    }

    return {
      mode: "index",
      source: best.source,
      column: best.column,
      operator: best.operator,
      candidateCount: best.candidateCount,
      rowCount,
      threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
    };
  }

  private bestCandidateEstimate(
    filters: readonly IndexFilter[],
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): CandidateEstimate | undefined {
    let best: CandidateEstimate | undefined;

    for (const filter of filters) {
      const equality = this.equalityEstimate(filter);
      if (equality !== undefined && (best === undefined || equality.candidateCount < best.candidateCount)) {
        best = equality;
      }

      const sorted = this.sortedEstimate(filter, rowCount, readNumericValue);
      if (sorted !== undefined && (best === undefined || sorted.candidateCount < best.candidateCount)) {
        best = sorted;
      }
    }

    return best;
  }

  private equalityEstimate(filter: IndexFilter): EqualityCandidateEstimate | undefined {
    const index = this.indexesByColumn.get(filter.columnName);
    if (index === undefined || (filter.operator !== "=" && filter.operator !== "in")) {
      return undefined;
    }

    const candidateCount = filter.operator === "="
      ? index.count(filter.value as IndexableValue)
      : index.countIn(filter.value as readonly IndexableValue[]);

    return {
      source: "equality",
      column: filter.columnName,
      operator: filter.operator,
      index,
      value: filter.value as IndexableValue | readonly IndexableValue[],
      candidateCount,
    };
  }

  private sortedEstimate(
    filter: IndexFilter,
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): SortedCandidateEstimate | undefined {
    const index = this.sortedIndexesByColumn.get(filter.columnName);
    if (index === undefined || !this.isRangeOperator(filter.operator) || typeof filter.value !== "number") {
      return undefined;
    }

    index.ensureFresh(rowCount, (rowIndex) => readNumericValue(rowIndex, filter.columnName));
    const bounds = index.bounds(filter.operator, filter.value, (rowIndex) => readNumericValue(rowIndex, filter.columnName));

    return {
      source: "sorted",
      column: filter.columnName,
      operator: filter.operator,
      index,
      value: filter.value,
      candidateCount: bounds.count,
      bounds,
    };
  }

  private rowIndexesFor(best: CandidateEstimate): Iterable<number> {
    if (best.source === "equality") {
      return best.operator === "="
        ? best.index.get(best.value as IndexableValue)
        : best.index.getIn(best.value as readonly IndexableValue[]);
    }

    return best.index.rows(best.bounds);
  }

  private rebuildEqualityIfDirty(
    rowCount: number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (!this.equalityDirty || readComparableValue === undefined) {
      return;
    }

    this.rebuildEqualityIndexes(rowCount, readComparableValue);
    this.equalityDirty = false;
  }

  private rebuildEqualityIndexes(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    const columns = this.list();
    this.indexesByColumn.clear();
    for (const column of columns) {
      this.indexesByColumn.set(column, this.buildEqualityIndex(column, rowCount, readComparableValue));
    }

    this.equalityDirty = false;
  }

  private rebuildUniqueIfDirty(
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (![...this.uniqueIndexesByColumn.values()].some((index) => index.isDirty())) {
      return;
    }

    this.rebuildUniqueIndexes(rowCount, readComparableValue);
  }

  private buildUniqueIndex(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): UniqueIndex {
    const index = new UniqueIndex(columnName);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      try {
        index.add(readComparableValue(rowIndex, columnName) as UniqueIndexValue, rowIndex);
      } catch (error) {
        if (error instanceof ColQLError && error.code === "COLQL_DUPLICATE_KEY") {
          throw new ColQLError(
            "COLQL_DUPLICATE_KEY",
            `Duplicate key found while building unique index for column "${columnName}".`,
            { ...(error.details as object), columnName, operation: "rebuildUniqueIndex" },
          );
        }

        throw error;
      }
    }

    index.markFresh();
    return index;
  }

  private rebuildSortedIndexes(
    rowCount: number,
    readNumericValue: (rowIndex: number, columnName: string) => number,
  ): void {
    for (const column of this.listSorted()) {
      this.rebuildSorted(column, rowCount, readNumericValue);
    }
  }

  private buildEqualityIndex(
    columnName: string,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): EqualityIndex {
    const index = new EqualityIndex(columnName);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      index.add(readComparableValue(rowIndex, columnName) as IndexableValue, rowIndex);
    }

    return index;
  }

  private isRangeOperator(operator: string): operator is RangeOperator {
    return operator === ">" || operator === ">=" || operator === "<" || operator === "<=";
  }

  private assertEqualitySupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind === "boolean") {
      throw new ColQLError(
        "COLQL_INDEX_UNSUPPORTED_COLUMN",
        `Indexing is not supported for boolean column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }

  private assertSortedSupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind !== "numeric") {
      throw new ColQLError(
        "COLQL_SORTED_INDEX_UNSUPPORTED_COLUMN",
        `Sorted indexing is not supported for ${definition.kind} column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }

  private assertUniqueSupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind === "boolean") {
      throw new ColQLError(
        "COLQL_UNIQUE_INDEX_UNSUPPORTED",
        `Unique indexing is not supported for boolean column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }
}

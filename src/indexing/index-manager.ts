import { ColQLError } from "../errors";
import type { ColumnDefinition } from "../types";
import { EqualityIndex, type EqualityIndexStats, type IndexableValue } from "./equality-index";

const DEFAULT_INDEX_SELECTIVITY_THRESHOLD = 0.4;

export type IndexFilter = {
  readonly columnName: string;
  readonly operator: string;
  readonly value: number | boolean | readonly (number | boolean)[];
};

export type IndexCandidatePlan = {
  readonly column: string;
  readonly operator: "=" | "in";
  readonly candidateCount: number;
  readonly rowCount: number;
  readonly threshold: number;
  readonly rowIndexes: readonly number[];
};

export type IndexDebugPlan =
  | {
      readonly mode: "index";
      readonly column: string;
      readonly operator: "=" | "in";
      readonly candidateCount: number;
      readonly rowCount: number;
      readonly threshold: number;
    }
  | {
      readonly mode: "scan";
      readonly candidateCount?: number;
      readonly rowCount: number;
      readonly threshold: number;
    };

type CandidateEstimate = {
  readonly column: string;
  readonly operator: "=" | "in";
  readonly index: EqualityIndex;
  readonly value: IndexableValue | readonly IndexableValue[];
  readonly candidateCount: number;
};

export class IndexManager {
  private readonly indexesByColumn = new Map<string, EqualityIndex>();
  private readonly indexedDefinitions = new Map<string, ColumnDefinition>();
  private dirty = false;

  create(
    columnName: string,
    definition: ColumnDefinition,
    rowCount: number,
    readComparableValue: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (this.indexesByColumn.has(columnName)) {
      throw new ColQLError("COLQL_INDEX_EXISTS", `Index already exists for column "${columnName}".`);
    }

    this.assertSupported(columnName, definition);
    const index = new EqualityIndex(columnName);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      index.add(readComparableValue(rowIndex, columnName) as IndexableValue, rowIndex);
    }

    this.indexesByColumn.set(columnName, index);
    this.indexedDefinitions.set(columnName, definition);
  }

  drop(columnName: string): void {
    if (!this.indexesByColumn.delete(columnName)) {
      throw new ColQLError("COLQL_INDEX_NOT_FOUND", `Index not found for column "${columnName}".`);
    }
    this.indexedDefinitions.delete(columnName);
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

  addRow(columnName: string, value: number | boolean, rowIndex: number): void {
    if (this.dirty) {
      return;
    }

    const index = this.indexesByColumn.get(columnName);
    if (index === undefined) {
      return;
    }

    index.add(value as IndexableValue, rowIndex);
  }

  markDirty(): void {
    if (this.indexesByColumn.size > 0) {
      this.dirty = true;
    }
  }

  bestCandidate(
    filters: readonly IndexFilter[],
    rowCount: number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): IndexCandidatePlan | undefined {
    this.rebuildIfDirty(rowCount, readComparableValue);
    const best = this.bestCandidateEstimate(filters);
    if (best === undefined) {
      return undefined;
    }

    const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
    if (best.candidateCount > threshold) {
      return undefined;
    }

    const rowIndexes = best.operator === "="
      ? best.index.get(best.value as IndexableValue)
      : best.index.getIn(best.value as readonly IndexableValue[]);

    return {
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
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): IndexDebugPlan {
    this.rebuildIfDirty(rowCount, readComparableValue);
    const best = this.bestCandidateEstimate(filters);
    if (best === undefined) {
      return { mode: "scan", rowCount, threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD };
    }

    const threshold = rowCount * DEFAULT_INDEX_SELECTIVITY_THRESHOLD;
    if (best.candidateCount > threshold) {
      return {
        mode: "scan",
        candidateCount: best.candidateCount,
        rowCount,
        threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
      };
    }

    return {
      mode: "index",
      column: best.column,
      operator: best.operator,
      candidateCount: best.candidateCount,
      rowCount,
      threshold: DEFAULT_INDEX_SELECTIVITY_THRESHOLD,
    };
  }

  private bestCandidateEstimate(filters: readonly IndexFilter[]): CandidateEstimate | undefined {
    let best: CandidateEstimate | undefined;

    for (const filter of filters) {
      const index = this.indexesByColumn.get(filter.columnName);
      if (index === undefined || (filter.operator !== "=" && filter.operator !== "in")) {
        continue;
      }

      const candidateCount = filter.operator === "="
        ? index.count(filter.value as IndexableValue)
        : index.countIn(filter.value as readonly IndexableValue[]);

      if (best === undefined || candidateCount < best.candidateCount) {
        best = {
          column: filter.columnName,
          operator: filter.operator,
          index,
          value: filter.value as IndexableValue | readonly IndexableValue[],
          candidateCount,
        };
      }
    }

    return best;
  }

  private rebuildIfDirty(
    rowCount: number,
    readComparableValue?: (rowIndex: number, columnName: string) => number | boolean,
  ): void {
    if (!this.dirty) {
      return;
    }

    if (readComparableValue === undefined) {
      return;
    }

    const columns = [...this.indexesByColumn.keys()];
    this.indexesByColumn.clear();
    for (const columnName of columns) {
      const index = new EqualityIndex(columnName);
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
        index.add(readComparableValue(rowIndex, columnName) as IndexableValue, rowIndex);
      }
      this.indexesByColumn.set(columnName, index);
    }
    this.dirty = false;
  }

  private assertSupported(columnName: string, definition: ColumnDefinition): void {
    if (definition.kind === "boolean") {
      throw new ColQLError(
        "COLQL_INDEX_UNSUPPORTED_COLUMN",
        `Indexing is not supported for boolean column "${columnName}".`,
        { columnName, kind: definition.kind },
      );
    }
  }
}

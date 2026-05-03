export type SortedIndexStats = {
  column: string;
  rowCount: number;
  memoryBytesApprox: number;
  dirty: boolean;
};

export type RangeOperator = ">" | ">=" | "<" | "<=";

export type RangeBounds = {
  start: number;
  end: number;
  count: number;
};

export class SortedIndex {
  private rowIdsSortedByValue = new Uint32Array(0);
  private dirty = true;

  constructor(readonly column: string) {}

  markDirty(): void {
    this.dirty = true;
  }

  isDirty(): boolean {
    return this.dirty;
  }

  ensureFresh(rowCount: number, readValue: (rowIndex: number) => number): void {
    if (!this.dirty && this.rowIdsSortedByValue.length === rowCount) {
      return;
    }

    const rowIds: number[] = new Array(rowCount);
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      rowIds[rowIndex] = rowIndex;
    }

    rowIds.sort((left, right) => {
      const leftValue = readValue(left);
      const rightValue = readValue(right);
      if (leftValue === rightValue) {
        return left - right;
      }
      return leftValue - rightValue;
    });

    this.rowIdsSortedByValue = Uint32Array.from(rowIds);
    this.dirty = false;
  }

  bounds(operator: RangeOperator, value: number, readValue: (rowIndex: number) => number): RangeBounds {
    const length = this.rowIdsSortedByValue.length;
    let start = 0;
    let end = length;

    switch (operator) {
      case ">":
        start = this.upperBound(value, readValue);
        break;
      case ">=":
        start = this.lowerBound(value, readValue);
        break;
      case "<":
        end = this.lowerBound(value, readValue);
        break;
      case "<=":
        end = this.upperBound(value, readValue);
        break;
    }

    return { start, end, count: Math.max(0, end - start) };
  }

  rows(bounds: RangeBounds): readonly number[] {
    const rows: number[] = [];
    for (let index = bounds.start; index < bounds.end; index += 1) {
      rows.push(this.rowIdsSortedByValue[index]);
    }

    // Preserve scan-order query semantics. This allocation only happens after
    // the planner has accepted the candidate set as selective.
    rows.sort((left, right) => left - right);
    return rows;
  }

  stats(): SortedIndexStats {
    return {
      column: this.column,
      rowCount: this.rowIdsSortedByValue.length,
      memoryBytesApprox: this.rowIdsSortedByValue.byteLength,
      dirty: this.dirty,
    };
  }

  private lowerBound(value: number, readValue: (rowIndex: number) => number): number {
    let low = 0;
    let high = this.rowIdsSortedByValue.length;

    while (low < high) {
      const mid = low + Math.floor((high - low) / 2);
      const midValue = readValue(this.rowIdsSortedByValue[mid]);
      if (midValue < value) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }

  private upperBound(value: number, readValue: (rowIndex: number) => number): number {
    let low = 0;
    let high = this.rowIdsSortedByValue.length;

    while (low < high) {
      const mid = low + Math.floor((high - low) / 2);
      const midValue = readValue(this.rowIdsSortedByValue[mid]);
      if (midValue <= value) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }
}

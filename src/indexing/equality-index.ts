export type IndexableValue = number;

export type EqualityIndexStats = {
  column: string;
  uniqueValues: number;
  rowCount: number;
  memoryBytesApprox: number;
};

export class EqualityIndex {
  private readonly buckets = new Map<IndexableValue, number[]>();
  private indexedRows = 0;

  constructor(readonly column: string) {}

  add(value: IndexableValue, rowIndex: number): void {
    const bucket = this.buckets.get(value);
    if (bucket === undefined) {
      this.buckets.set(value, [rowIndex]);
    } else {
      bucket.push(rowIndex);
    }

    this.indexedRows += 1;
  }

  get(value: IndexableValue): readonly number[] {
    return this.buckets.get(value) ?? [];
  }

  count(value: IndexableValue): number {
    return this.get(value).length;
  }

  getIn(values: readonly IndexableValue[]): readonly number[] {
    if (values.length === 1) {
      return this.get(values[0]);
    }

    const seenValues = new Set<IndexableValue>();
    let total = 0;
    for (const value of values) {
      if (seenValues.has(value)) {
        continue;
      }
      seenValues.add(value);
      total += this.get(value).length;
    }

    const rows: number[] = [];
    if (total > 0) {
      rows.length = 0;
    }

    for (const value of seenValues) {
      for (const rowIndex of this.get(value)) {
        rows.push(rowIndex);
      }
    }

    rows.sort((left, right) => left - right);
    return rows;
  }

  countIn(values: readonly IndexableValue[]): number {
    const seenValues = new Set<IndexableValue>();
    let total = 0;

    for (const value of values) {
      if (seenValues.has(value)) {
        continue;
      }

      seenValues.add(value);
      total += this.count(value);
    }

    return total;
  }

  stats(): EqualityIndexStats {
    return {
      column: this.column,
      uniqueValues: this.buckets.size,
      rowCount: this.indexedRows,
      memoryBytesApprox: this.memoryBytesApprox(),
    };
  }

  private memoryBytesApprox(): number {
    // Honest approximation: JS Map/bucket overhead is runtime-dependent, so count the row ids plus a small per-bucket estimate.
    return this.indexedRows * Uint32Array.BYTES_PER_ELEMENT + this.buckets.size * 32;
  }
}

import { ColQLError } from "../errors";
import { IndexLifecycle, type IndexDirtyReason, type IndexLifecycleSnapshot } from "./index-lifecycle";

export type UniqueIndexValue = number;

export type UniqueIndexStats = {
  readonly column: string;
  readonly uniqueValues: number;
  readonly rowCount: number;
  readonly memoryBytesApprox: number;
  readonly dirty: boolean;
};

export class UniqueIndex {
  private readonly rowsByValue = new Map<UniqueIndexValue, number>();
  private indexedRows = 0;
  private readonly lifecycle: IndexLifecycle;

  constructor(readonly column: string, generation = 0) {
    this.lifecycle = new IndexLifecycle("fresh", generation);
  }

  add(value: UniqueIndexValue, rowIndex: number): void {
    const existingRowIndex = this.rowsByValue.get(value);
    if (existingRowIndex !== undefined) {
      throw new ColQLError(
        "COLQL_DUPLICATE_KEY",
        `Duplicate key for unique index "${this.column}".`,
        {
          columnName: this.column,
          encodedValue: value,
          existingRowIndex,
          rowIndex,
        },
      );
    }

    this.rowsByValue.set(value, rowIndex);
    this.indexedRows += 1;
  }

  get(value: UniqueIndexValue): number | undefined {
    return this.rowsByValue.get(value);
  }

  deleteRow(rowIndex: number): void {
    if (this.isDirty()) {
      return;
    }

    for (const [value, indexedRow] of this.rowsByValue) {
      if (indexedRow === rowIndex) {
        this.rowsByValue.delete(value);
        this.indexedRows -= 1;
        continue;
      }

      if (indexedRow > rowIndex) {
        this.rowsByValue.set(value, indexedRow - 1);
      }
    }
  }

  markDirty(reason: IndexDirtyReason = "update:indexed-column", incrementGeneration = true): void {
    this.lifecycle.markDirty(reason, incrementGeneration);
  }

  markFresh(): void {
    this.lifecycle.markFresh();
  }

  isDirty(): boolean {
    return this.lifecycle.state !== "fresh";
  }

  lifecycleSnapshot(): IndexLifecycleSnapshot {
    return this.lifecycle.snapshot();
  }

  markFailed(failureReason?: string): void {
    this.lifecycle.markFailed(failureReason);
  }

  bumpGeneration(): void {
    this.lifecycle.bumpGeneration();
  }

  stats(): UniqueIndexStats {
    return {
      column: this.column,
      uniqueValues: this.rowsByValue.size,
      rowCount: this.indexedRows,
      memoryBytesApprox: this.memoryBytesApprox(),
      dirty: this.isDirty(),
    };
  }

  private memoryBytesApprox(): number {
    return this.indexedRows * Uint32Array.BYTES_PER_ELEMENT + this.rowsByValue.size * 40;
  }
}

import { ColQLError } from "../errors";

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
  private dirty = false;

  constructor(readonly column: string) {}

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
    if (this.dirty) {
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

  markDirty(): void {
    this.dirty = true;
  }

  markFresh(): void {
    this.dirty = false;
  }

  isDirty(): boolean {
    return this.dirty;
  }

  stats(): UniqueIndexStats {
    return {
      column: this.column,
      uniqueValues: this.rowsByValue.size,
      rowCount: this.indexedRows,
      memoryBytesApprox: this.memoryBytesApprox(),
      dirty: this.dirty,
    };
  }

  private memoryBytesApprox(): number {
    return this.indexedRows * Uint32Array.BYTES_PER_ELEMENT + this.rowsByValue.size * 40;
  }
}

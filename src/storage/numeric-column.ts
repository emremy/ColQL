import type { ColumnStorage, NumericColumnType } from "../types";
import { ColQLError } from "../errors";
import { assertNonNegativeInteger, assertNumericValue } from "../validation";

type NumericArray =
  | Int16Array
  | Int32Array
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Float32Array
  | Float64Array;

type NumericArrayConstructor = new (capacity: number) => NumericArray;

const NUMERIC_ARRAYS: Record<NumericColumnType, NumericArrayConstructor> = {
  int16: Int16Array,
  int32: Int32Array,
  uint8: Uint8Array,
  uint16: Uint16Array,
  uint32: Uint32Array,
  float32: Float32Array,
  float64: Float64Array,
};

export class NumericColumnStorage implements ColumnStorage<number> {
  private data: NumericArray;
  private readonly ArrayType: NumericArrayConstructor;

  constructor(
    private readonly columnType: NumericColumnType,
    capacity: number,
    data?: NumericArray,
  ) {
    this.ArrayType = NUMERIC_ARRAYS[columnType];
    this.data = data ?? new this.ArrayType(capacity);
    if (this.data.length !== capacity) {
      throw new ColQLError("COLQL_INVALID_SERIALIZED_DATA", `Numeric column ${columnType} data length ${this.data.length} does not match capacity ${capacity}.`);
    }
  }

  get capacity(): number {
    return this.data.length;
  }

  get arrayName(): string {
    return this.data.constructor.name;
  }

  get(rowIndex: number): number {
    this.assertIndex(rowIndex);
    return this.data[rowIndex];
  }

  set(rowIndex: number, value: number): void {
    this.assertIndex(rowIndex);
    assertNumericValue(this.columnType, this.columnType, value);
    this.data[rowIndex] = value;
  }

  resize(capacity: number): void {
    assertNonNegativeInteger(capacity, "limit");

    const next = new this.ArrayType(capacity);
    next.set(this.data.subarray(0, Math.min(this.data.length, capacity)));
    this.data = next;
  }

  toBytes(): Uint8Array {
    return new Uint8Array(this.data.buffer, this.data.byteOffset, this.data.byteLength);
  }

  private assertIndex(rowIndex: number): void {
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.data.length) {
      throw new ColQLError("COLQL_INVALID_ROW_INDEX", `Invalid row index: expected integer between 0 and ${Math.max(this.data.length - 1, 0)}, received ${String(rowIndex)}.`);
    }
  }
}

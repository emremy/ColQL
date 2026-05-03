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

const DEFAULT_CHUNK_SIZE = 65_536;

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
  private readonly chunks: NumericArray[] = [];
  private readonly lengths: number[] = [];
  private readonly ArrayType: NumericArrayConstructor;
  private currentRowCount = 0;
  private logicalCapacity = 0;
  private packedChunks = true;

  constructor(
    private readonly columnType: NumericColumnType,
    capacity: number,
    data?: NumericArray,
    rowCount = data?.length ?? 0,
    private readonly chunkSize = DEFAULT_CHUNK_SIZE,
  ) {
    this.ArrayType = NUMERIC_ARRAYS[columnType];
    this.assertChunkSize(chunkSize);
    this.resize(capacity);

    if (data !== undefined) {
      if (data.length !== capacity) {
        throw new ColQLError("COLQL_INVALID_SERIALIZED_DATA", `Numeric column ${columnType} data length ${data.length} does not match capacity ${capacity}.`);
      }
      for (let index = 0; index < Math.min(rowCount, data.length); index += 1) this.append(data[index]);
    }
  }

  get capacity(): number { return this.logicalCapacity; }
  get rowCount(): number { return this.currentRowCount; }
  get arrayName(): string { return this.ArrayType.name; }

  append(value: number): void {
    assertNumericValue(this.columnType, this.columnType, value);
    this.ensureAppendCapacity();
    const chunkIndex = this.ensureWritableChunk();
    const offset = this.lengths[chunkIndex];
    this.chunks[chunkIndex][offset] = value;
    this.lengths[chunkIndex] += 1;
    this.currentRowCount += 1;
  }

  get(rowIndex: number): number {
    if (rowIndex >= this.currentRowCount && rowIndex < this.logicalCapacity) return 0;
    const { chunkIndex, offset } = this.locate(rowIndex);
    return this.chunks[chunkIndex][offset];
  }

  set(rowIndex: number, value: number): void {
    assertNumericValue(this.columnType, this.columnType, value);
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.logicalCapacity) this.assertIndex(rowIndex);
    while (rowIndex > this.currentRowCount) this.append(0);
    if (rowIndex === this.currentRowCount) return this.append(value);
    const { chunkIndex, offset } = this.locate(rowIndex);
    this.chunks[chunkIndex][offset] = value;
  }

  deleteAt(rowIndex: number): void {
    const { chunkIndex, offset } = this.locate(rowIndex);
    const chunk = this.chunks[chunkIndex];
    const length = this.lengths[chunkIndex];
    if (offset < length - 1) chunk.copyWithin(offset, offset + 1, length);
    this.lengths[chunkIndex] -= 1;
    this.currentRowCount -= 1;
    if (chunkIndex < this.chunks.length - 1) this.packedChunks = false;
    this.removeEmptyChunk(chunkIndex);
  }

  deleteMany(rowIndexes: readonly number[]): void {
    if (rowIndexes.length === 0) return;

    let deleteOffset = 0;
    let nextDelete = rowIndexes[deleteOffset];
    const nextChunks: NumericArray[] = [];
    const nextLengths: number[] = [];
    let nextRowCount = 0;
    let sourceRowIndex = 0;

    const appendRaw = (value: number): void => {
      let chunk = nextChunks[nextChunks.length - 1];
      if (chunk === undefined || nextLengths[nextLengths.length - 1] >= this.chunkSize) {
        chunk = new this.ArrayType(this.chunkSize);
        nextChunks.push(chunk);
        nextLengths.push(0);
      }

      const chunkIndex = nextChunks.length - 1;
      chunk[nextLengths[chunkIndex]] = value;
      nextLengths[chunkIndex] += 1;
      nextRowCount += 1;
    };

    for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) {
      const chunk = this.chunks[chunkIndex];
      const length = this.lengths[chunkIndex];
      for (let offset = 0; offset < length; offset += 1) {
        if (sourceRowIndex === nextDelete) {
          deleteOffset += 1;
          nextDelete = rowIndexes[deleteOffset];
        } else {
          appendRaw(chunk[offset]);
        }
        sourceRowIndex += 1;
      }
    }

    this.chunks.length = 0;
    this.chunks.push(...nextChunks);
    this.lengths.length = 0;
    this.lengths.push(...nextLengths);
    this.currentRowCount = nextRowCount;
    this.packedChunks = true;
  }

  resize(capacity: number): void {
    assertNonNegativeInteger(capacity, "limit");
    this.logicalCapacity = capacity;
    while (this.chunks.length * this.chunkSize < capacity) {
      this.chunks.push(new this.ArrayType(this.chunkSize));
      this.lengths.push(0);
    }
  }

  toBytes(): Uint8Array {
    const output = new this.ArrayType(this.logicalCapacity);
    let targetOffset = 0;
    for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) {
      const length = this.lengths[chunkIndex];
      output.set(this.chunks[chunkIndex].subarray(0, length), targetOffset);
      targetOffset += length;
    }
    return new Uint8Array(output.buffer, output.byteOffset, output.byteLength);
  }

  private locate(rowIndex: number): { chunkIndex: number; offset: number } {
    this.assertIndex(rowIndex);
    if (this.packedChunks) {
      return {
        chunkIndex: Math.floor(rowIndex / this.chunkSize),
        offset: rowIndex % this.chunkSize,
      };
    }

    let remaining = rowIndex;
    for (let chunkIndex = 0; chunkIndex < this.lengths.length; chunkIndex += 1) {
      const length = this.lengths[chunkIndex];
      if (remaining < length) return { chunkIndex, offset: remaining };
      remaining -= length;
    }
    throw new ColQLError("COLQL_INVALID_ROW_INDEX", `Invalid row index: could not locate row ${String(rowIndex)}.`);
  }

  private ensureWritableChunk(): number {
    if (this.packedChunks) {
      const packedChunkIndex = Math.floor(this.currentRowCount / this.chunkSize);
      while (this.chunks.length <= packedChunkIndex) {
        this.chunks.push(new this.ArrayType(this.chunkSize));
        this.lengths.push(0);
      }
      return packedChunkIndex;
    }

    const lastIndex = this.chunks.length - 1;
    if (lastIndex >= 0 && this.lengths[lastIndex] < this.chunkSize) return lastIndex;
    this.chunks.push(new this.ArrayType(this.chunkSize));
    this.lengths.push(0);
    return this.chunks.length - 1;
  }

  private ensureAppendCapacity(): void {
    if (this.currentRowCount < this.logicalCapacity) return;
    this.resize(Math.max(1, this.logicalCapacity * 2, this.currentRowCount + 1));
  }

  private removeEmptyChunk(chunkIndex: number): void {
    if (this.lengths[chunkIndex] !== 0) return;
    this.chunks.splice(chunkIndex, 1);
    this.lengths.splice(chunkIndex, 1);
  }

  private assertIndex(rowIndex: number): void {
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.currentRowCount) {
      throw new ColQLError("COLQL_INVALID_ROW_INDEX", `Invalid row index: expected integer between 0 and ${Math.max(this.currentRowCount - 1, 0)}, received ${String(rowIndex)}.`);
    }
  }

  private assertChunkSize(chunkSize: number): void {
    if (!Number.isInteger(chunkSize) || chunkSize < 1) throw new ColQLError("COLQL_INVALID_LIMIT", `Invalid chunk size: expected positive integer, received ${String(chunkSize)}.`);
  }
}

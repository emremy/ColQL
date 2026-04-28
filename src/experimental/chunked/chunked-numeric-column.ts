import { assertNumericValue } from "../../validation";
import type { ChunkedNumericType, ExperimentalChunkedColumn } from "./chunked-types";
import { assertChunkSize, locateByChunkLengths } from "./chunked-column";

type NumericArray = Uint8Array | Uint32Array | Float64Array;
type NumericArrayConstructor = new (length: number) => NumericArray;

const NUMERIC_ARRAYS: Record<ChunkedNumericType, NumericArrayConstructor> = {
  uint8: Uint8Array,
  uint32: Uint32Array,
  float64: Float64Array,
};

export class ExperimentalChunkedNumericColumn implements ExperimentalChunkedColumn<number> {
  readonly chunkSize: number;
  private readonly ArrayType: NumericArrayConstructor;
  private readonly chunks: NumericArray[] = [];
  private readonly lengths: number[] = [];
  private currentRowCount = 0;

  constructor(private readonly type: ChunkedNumericType, chunkSize = 65_536) {
    assertChunkSize(chunkSize);
    this.chunkSize = chunkSize;
    this.ArrayType = NUMERIC_ARRAYS[type];
  }

  get rowCount(): number {
    return this.currentRowCount;
  }

  append(value: number): void {
    assertNumericValue(this.type, this.type, value);
    const chunkIndex = this.ensureWritableChunk();
    const offset = this.lengths[chunkIndex];
    this.chunks[chunkIndex][offset] = value;
    this.lengths[chunkIndex] += 1;
    this.currentRowCount += 1;
  }

  get(rowIndex: number): number {
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    return this.chunks[chunkIndex][offset];
  }

  set(rowIndex: number, value: number): void {
    assertNumericValue(this.type, this.type, value);
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    this.chunks[chunkIndex][offset] = value;
  }

  deleteAt(rowIndex: number): void {
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    const chunk = this.chunks[chunkIndex];
    const length = this.lengths[chunkIndex];

    if (offset < length - 1) {
      chunk.copyWithin(offset, offset + 1, length);
    }

    this.lengths[chunkIndex] -= 1;
    this.currentRowCount -= 1;
    this.removeEmptyChunk(chunkIndex);
  }

  toArray(): number[] {
    const values: number[] = [];
    for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) {
      const chunk = this.chunks[chunkIndex];
      const length = this.lengths[chunkIndex];
      for (let offset = 0; offset < length; offset += 1) {
        values.push(chunk[offset]);
      }
    }
    return values;
  }

  memoryBytesApprox(): number {
    return this.chunks.reduce((total, chunk) => total + chunk.byteLength, 0) + this.lengths.length * 8;
  }

  private ensureWritableChunk(): number {
    const lastIndex = this.chunks.length - 1;
    if (lastIndex >= 0 && this.lengths[lastIndex] < this.chunkSize) {
      return lastIndex;
    }

    this.chunks.push(new this.ArrayType(this.chunkSize));
    this.lengths.push(0);
    return this.chunks.length - 1;
  }

  private removeEmptyChunk(chunkIndex: number): void {
    if (this.lengths[chunkIndex] !== 0) {
      return;
    }

    this.chunks.splice(chunkIndex, 1);
    this.lengths.splice(chunkIndex, 1);
  }
}

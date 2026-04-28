import { assertBooleanValue } from "../../validation";
import type { ExperimentalChunkedColumn } from "./chunked-types";
import { assertChunkSize, locateByChunkLengths } from "./chunked-column";

const BITS_PER_BYTE = 8;

class BooleanChunk {
  private readonly bytes: Uint8Array;

  constructor(readonly capacity: number) {
    this.bytes = new Uint8Array(Math.ceil(capacity / BITS_PER_BYTE));
  }

  get(index: number): boolean {
    const byteIndex = Math.floor(index / BITS_PER_BYTE);
    const bitOffset = index % BITS_PER_BYTE;
    return (this.bytes[byteIndex] & (1 << bitOffset)) !== 0;
  }

  set(index: number, value: boolean): void {
    const byteIndex = Math.floor(index / BITS_PER_BYTE);
    const bitOffset = index % BITS_PER_BYTE;
    const mask = 1 << bitOffset;
    if (value) {
      this.bytes[byteIndex] |= mask;
    } else {
      this.bytes[byteIndex] &= ~mask;
    }
  }

  deleteAt(offset: number, length: number): void {
    for (let index = offset; index < length - 1; index += 1) {
      this.set(index, this.get(index + 1));
    }
    if (length > 0) {
      this.set(length - 1, false);
    }
  }

  memoryBytes(): number {
    return this.bytes.byteLength;
  }
}

export class ExperimentalChunkedBooleanColumn implements ExperimentalChunkedColumn<boolean> {
  readonly chunkSize: number;
  private readonly chunks: BooleanChunk[] = [];
  private readonly lengths: number[] = [];
  private currentRowCount = 0;

  constructor(chunkSize = 65_536) {
    assertChunkSize(chunkSize);
    this.chunkSize = chunkSize;
  }

  get rowCount(): number {
    return this.currentRowCount;
  }

  append(value: boolean): void {
    assertBooleanValue("boolean", value);
    const chunkIndex = this.ensureWritableChunk();
    const offset = this.lengths[chunkIndex];
    this.chunks[chunkIndex].set(offset, value);
    this.lengths[chunkIndex] += 1;
    this.currentRowCount += 1;
  }

  get(rowIndex: number): boolean {
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    return this.chunks[chunkIndex].get(offset);
  }

  set(rowIndex: number, value: boolean): void {
    assertBooleanValue("boolean", value);
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    this.chunks[chunkIndex].set(offset, value);
  }

  deleteAt(rowIndex: number): void {
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    this.chunks[chunkIndex].deleteAt(offset, this.lengths[chunkIndex]);
    this.lengths[chunkIndex] -= 1;
    this.currentRowCount -= 1;
    this.removeEmptyChunk(chunkIndex);
  }

  toArray(): boolean[] {
    const values: boolean[] = [];
    for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) {
      for (let offset = 0; offset < this.lengths[chunkIndex]; offset += 1) {
        values.push(this.chunks[chunkIndex].get(offset));
      }
    }
    return values;
  }

  memoryBytesApprox(): number {
    return this.chunks.reduce((total, chunk) => total + chunk.memoryBytes(), 0) + this.lengths.length * 8;
  }

  private ensureWritableChunk(): number {
    const lastIndex = this.chunks.length - 1;
    if (lastIndex >= 0 && this.lengths[lastIndex] < this.chunkSize) {
      return lastIndex;
    }
    this.chunks.push(new BooleanChunk(this.chunkSize));
    this.lengths.push(0);
    return this.chunks.length - 1;
  }

  private removeEmptyChunk(chunkIndex: number): void {
    if (this.lengths[chunkIndex] !== 0) return;
    this.chunks.splice(chunkIndex, 1);
    this.lengths.splice(chunkIndex, 1);
  }
}

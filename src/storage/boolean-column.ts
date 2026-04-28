import type { ColumnStorage } from "../types";
import { ColQLError } from "../errors";
import { assertBooleanValue, assertNonNegativeInteger } from "../validation";

const DEFAULT_CHUNK_SIZE = 65_536;
const BITS_PER_BYTE = 8;

class BooleanChunk {
  private readonly bytes: Uint8Array;
  constructor(readonly capacity: number) { this.bytes = new Uint8Array(Math.ceil(capacity / BITS_PER_BYTE)); }
  get(index: number): boolean { return (this.bytes[Math.floor(index / BITS_PER_BYTE)] & (1 << (index % BITS_PER_BYTE))) !== 0; }
  set(index: number, value: boolean): void { const byteIndex = Math.floor(index / BITS_PER_BYTE); const mask = 1 << (index % BITS_PER_BYTE); if (value) this.bytes[byteIndex] |= mask; else this.bytes[byteIndex] &= ~mask; }
  deleteAt(offset: number, length: number): void { for (let index = offset; index < length - 1; index += 1) this.set(index, this.get(index + 1)); if (length > 0) this.set(length - 1, false); }
  copyInto(target: Uint8Array, targetBitOffset: number, length: number): void { for (let index = 0; index < length; index += 1) { const bitIndex = targetBitOffset + index; const byteIndex = Math.floor(bitIndex / BITS_PER_BYTE); const mask = 1 << (bitIndex % BITS_PER_BYTE); if (this.get(index)) target[byteIndex] |= mask; else target[byteIndex] &= ~mask; } }
}

export class BooleanColumnStorage implements ColumnStorage<boolean> {
  private readonly chunks: BooleanChunk[] = [];
  private readonly lengths: number[] = [];
  private currentRowCount = 0;
  private logicalCapacity = 0;

  constructor(capacity: number, bytes?: Uint8Array, rowCount = bytes === undefined ? 0 : capacity, private readonly chunkSize = DEFAULT_CHUNK_SIZE) {
    this.assertChunkSize(chunkSize);
    this.resize(capacity);
    if (bytes !== undefined) {
      const expectedBytes = Math.ceil(capacity / BITS_PER_BYTE);
      if (bytes.length !== expectedBytes) throw new ColQLError("COLQL_INVALID_SERIALIZED_DATA", `Boolean column byte length ${bytes.length} does not match capacity ${capacity}.`);
      for (let index = 0; index < Math.min(rowCount, capacity); index += 1) this.append((bytes[Math.floor(index / BITS_PER_BYTE)] & (1 << (index % BITS_PER_BYTE))) !== 0);
    }
  }

  get capacity(): number { return this.logicalCapacity; }
  get rowCount(): number { return this.currentRowCount; }

  append(value: boolean): void { assertBooleanValue("boolean", value); this.ensureAppendCapacity(); const chunkIndex = this.ensureWritableChunk(); const offset = this.lengths[chunkIndex]; this.chunks[chunkIndex].set(offset, value); this.lengths[chunkIndex] += 1; this.currentRowCount += 1; }
  get(rowIndex: number): boolean { if (rowIndex >= this.currentRowCount && rowIndex < this.logicalCapacity) return false; const { chunkIndex, offset } = this.locate(rowIndex); return this.chunks[chunkIndex].get(offset); }
  set(rowIndex: number, value: boolean): void { assertBooleanValue("boolean", value); if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.logicalCapacity) this.assertIndex(rowIndex); while (rowIndex > this.currentRowCount) this.append(false); if (rowIndex === this.currentRowCount) return this.append(value); const { chunkIndex, offset } = this.locate(rowIndex); this.chunks[chunkIndex].set(offset, value); }
  deleteAt(rowIndex: number): void { const { chunkIndex, offset } = this.locate(rowIndex); this.chunks[chunkIndex].deleteAt(offset, this.lengths[chunkIndex]); this.lengths[chunkIndex] -= 1; this.currentRowCount -= 1; this.removeEmptyChunk(chunkIndex); }

  resize(capacity: number): void { assertNonNegativeInteger(capacity, "limit"); this.logicalCapacity = capacity; while (this.chunks.length * this.chunkSize < capacity) { this.chunks.push(new BooleanChunk(this.chunkSize)); this.lengths.push(0); } }
  toBytes(): Uint8Array { const output = new Uint8Array(Math.ceil(this.logicalCapacity / BITS_PER_BYTE)); let targetBitOffset = 0; for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) { const length = this.lengths[chunkIndex]; this.chunks[chunkIndex].copyInto(output, targetBitOffset, length); targetBitOffset += length; } return output; }

  private locate(rowIndex: number): { chunkIndex: number; offset: number } { this.assertIndex(rowIndex); let remaining = rowIndex; for (let chunkIndex = 0; chunkIndex < this.lengths.length; chunkIndex += 1) { const length = this.lengths[chunkIndex]; if (remaining < length) return { chunkIndex, offset: remaining }; remaining -= length; } throw new ColQLError("COLQL_INVALID_ROW_INDEX", `Invalid row index: could not locate row ${String(rowIndex)}.`); }
  private ensureWritableChunk(): number { const lastIndex = this.chunks.length - 1; if (lastIndex >= 0 && this.lengths[lastIndex] < this.chunkSize) return lastIndex; this.chunks.push(new BooleanChunk(this.chunkSize)); this.lengths.push(0); return this.chunks.length - 1; }
  private ensureAppendCapacity(): void { if (this.currentRowCount >= this.logicalCapacity) this.resize(Math.max(1, this.logicalCapacity * 2, this.currentRowCount + 1)); }
  private removeEmptyChunk(chunkIndex: number): void { if (this.lengths[chunkIndex] === 0) { this.chunks.splice(chunkIndex, 1); this.lengths.splice(chunkIndex, 1); } }
  private assertIndex(rowIndex: number): void { if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.currentRowCount) throw new ColQLError("COLQL_INVALID_ROW_INDEX", `Invalid row index: expected integer between 0 and ${Math.max(this.currentRowCount - 1, 0)}, received ${String(rowIndex)}.`); }
  private assertChunkSize(chunkSize: number): void { if (!Number.isInteger(chunkSize) || chunkSize < 1) throw new ColQLError("COLQL_INVALID_LIMIT", `Invalid chunk size: expected positive integer, received ${String(chunkSize)}.`); }
}

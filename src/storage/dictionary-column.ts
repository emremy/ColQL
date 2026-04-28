import type { ColumnStorage } from "../types";
import { ColQLError } from "../errors";
import { assertDictionaryValue, assertDictionaryValues, assertNonNegativeInteger } from "../validation";

type DictionaryCodeArray = Uint8Array | Uint16Array | Uint32Array;
type DictionaryCodeArrayConstructor = new (capacity: number) => DictionaryCodeArray;

const DEFAULT_CHUNK_SIZE = 65_536;

function codeArrayForSize(size: number): DictionaryCodeArrayConstructor {
  if (size <= 255) return Uint8Array;
  if (size <= 65_535) return Uint16Array;
  return Uint32Array;
}

export class DictionaryColumnStorage<Values extends readonly string[]>
  implements ColumnStorage<Values[number]>
{
  private readonly codeByValue = new Map<Values[number], number>();
  private readonly chunks: DictionaryCodeArray[] = [];
  private readonly lengths: number[] = [];
  private readonly ArrayType: DictionaryCodeArrayConstructor;
  private currentRowCount = 0;
  private logicalCapacity = 0;

  constructor(
    private readonly values: Values,
    capacity: number,
    data?: DictionaryCodeArray,
    rowCount = data?.length ?? 0,
    private readonly chunkSize = DEFAULT_CHUNK_SIZE,
  ) {
    assertDictionaryValues(values);
    this.assertChunkSize(chunkSize);
    this.ArrayType = codeArrayForSize(values.length);
    values.forEach((value, index) => this.codeByValue.set(value, index));
    this.resize(capacity);

    if (data !== undefined) {
      if (data.length !== capacity) {
        throw new ColQLError("COLQL_INVALID_SERIALIZED_DATA", `Dictionary column data length ${data.length} does not match capacity ${capacity}.`);
      }
      const logicalLength = Math.min(rowCount, data.length);
      for (let index = 0; index < logicalLength; index += 1) {
        this.appendCode(data[index]);
      }
    }
  }

  get capacity(): number {
    return this.logicalCapacity;
  }

  get rowCount(): number {
    return this.currentRowCount;
  }

  get arrayName(): string {
    return this.ArrayType.name;
  }

  append(value: Values[number]): void {
    this.appendCode(this.encode(value));
  }

  get(rowIndex: number): Values[number] {
    return this.values[this.getCode(rowIndex)];
  }

  getCode(rowIndex: number): number {
    if (rowIndex >= this.currentRowCount && rowIndex < this.logicalCapacity) {
      return 0;
    }
    const { chunkIndex, offset } = this.locate(rowIndex);
    return this.chunks[chunkIndex][offset];
  }

  encode(value: Values[number]): number {
    assertDictionaryValue("dictionary", this.values, value);
    const code = this.codeByValue.get(value);
    if (code === undefined) {
      throw new ColQLError("COLQL_UNKNOWN_VALUE", `Invalid value for dictionary column "dictionary": expected one of ${JSON.stringify(this.values)}, received ${String(value)}.`);
    }
    return code;
  }

  set(rowIndex: number, value: Values[number]): void {
    const code = this.encode(value);
    if (rowIndex < 0 || !Number.isInteger(rowIndex) || rowIndex >= this.logicalCapacity) {
      this.assertIndex(rowIndex);
    }
    while (rowIndex > this.currentRowCount) {
      this.appendCode(0);
    }
    if (rowIndex === this.currentRowCount) {
      this.appendCode(code);
      return;
    }
    const { chunkIndex, offset } = this.locate(rowIndex);
    this.chunks[chunkIndex][offset] = code;
  }

  deleteAt(rowIndex: number): void {
    const { chunkIndex, offset } = this.locate(rowIndex);
    const chunk = this.chunks[chunkIndex];
    const length = this.lengths[chunkIndex];
    if (offset < length - 1) {
      chunk.copyWithin(offset, offset + 1, length);
    }
    this.lengths[chunkIndex] -= 1;
    this.currentRowCount -= 1;
    this.removeEmptyChunk(chunkIndex);
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

  private appendCode(code: number): void {
    const chunkIndex = this.ensureWritableChunk();
    const offset = this.lengths[chunkIndex];
    this.chunks[chunkIndex][offset] = code;
    this.lengths[chunkIndex] += 1;
    this.currentRowCount += 1;
  }

  private locate(rowIndex: number): { chunkIndex: number; offset: number } {
    this.assertIndex(rowIndex);
    let remaining = rowIndex;
    for (let chunkIndex = 0; chunkIndex < this.lengths.length; chunkIndex += 1) {
      const length = this.lengths[chunkIndex];
      if (remaining < length) return { chunkIndex, offset: remaining };
      remaining -= length;
    }
    throw new ColQLError("COLQL_INVALID_ROW_INDEX", `Invalid row index: could not locate row ${String(rowIndex)}.`);
  }

  private ensureWritableChunk(): number {
    const lastIndex = this.chunks.length - 1;
    if (lastIndex >= 0 && this.lengths[lastIndex] < this.chunkSize) return lastIndex;
    this.chunks.push(new this.ArrayType(this.chunkSize));
    this.lengths.push(0);
    return this.chunks.length - 1;
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
    if (!Number.isInteger(chunkSize) || chunkSize < 1) {
      throw new ColQLError("COLQL_INVALID_LIMIT", `Invalid chunk size: expected positive integer, received ${String(chunkSize)}.`);
    }
  }
}

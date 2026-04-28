import { ColQLError } from "../../errors";
import { assertDictionaryValue, assertDictionaryValues } from "../../validation";
import type { ExperimentalChunkedColumn } from "./chunked-types";
import { assertChunkSize, locateByChunkLengths } from "./chunked-column";

type DictionaryCodeArray = Uint8Array | Uint16Array | Uint32Array;
type DictionaryCodeArrayConstructor = new (length: number) => DictionaryCodeArray;

function codeArrayForSize(size: number): DictionaryCodeArrayConstructor {
  if (size <= 255) return Uint8Array;
  if (size <= 65_535) return Uint16Array;
  return Uint32Array;
}

export class ExperimentalChunkedDictionaryColumn<Values extends readonly string[]>
  implements ExperimentalChunkedColumn<Values[number]>
{
  readonly chunkSize: number;
  private readonly ArrayType: DictionaryCodeArrayConstructor;
  private readonly codeByValue = new Map<Values[number], number>();
  private readonly chunks: DictionaryCodeArray[] = [];
  private readonly lengths: number[] = [];
  private currentRowCount = 0;

  constructor(private readonly values: Values, chunkSize = 65_536) {
    assertChunkSize(chunkSize);
    assertDictionaryValues(values);
    this.chunkSize = chunkSize;
    this.ArrayType = codeArrayForSize(values.length);
    values.forEach((value, index) => this.codeByValue.set(value, index));
  }

  get rowCount(): number {
    return this.currentRowCount;
  }

  append(value: Values[number]): void {
    const code = this.encode(value);
    const chunkIndex = this.ensureWritableChunk();
    const offset = this.lengths[chunkIndex];
    this.chunks[chunkIndex][offset] = code;
    this.lengths[chunkIndex] += 1;
    this.currentRowCount += 1;
  }

  get(rowIndex: number): Values[number] {
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    return this.values[this.chunks[chunkIndex][offset]];
  }

  set(rowIndex: number, value: Values[number]): void {
    const { chunkIndex, offset } = locateByChunkLengths(rowIndex, this.currentRowCount, this.lengths);
    this.chunks[chunkIndex][offset] = this.encode(value);
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

  toArray(): Values[number][] {
    const values: Values[number][] = [];
    for (let chunkIndex = 0; chunkIndex < this.chunks.length; chunkIndex += 1) {
      const chunk = this.chunks[chunkIndex];
      const length = this.lengths[chunkIndex];
      for (let offset = 0; offset < length; offset += 1) {
        values.push(this.values[chunk[offset]]);
      }
    }
    return values;
  }

  memoryBytesApprox(): number {
    return this.chunks.reduce((total, chunk) => total + chunk.byteLength, 0) + this.lengths.length * 8;
  }

  private encode(value: Values[number]): number {
    assertDictionaryValue("dictionary", this.values, value);
    const code = this.codeByValue.get(value);
    if (code === undefined) {
      throw new ColQLError("COLQL_UNKNOWN_VALUE", `Invalid dictionary value ${String(value)}.`);
    }
    return code;
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
    if (this.lengths[chunkIndex] !== 0) return;
    this.chunks.splice(chunkIndex, 1);
    this.lengths.splice(chunkIndex, 1);
  }
}

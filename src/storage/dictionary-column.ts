import type { ColumnStorage } from "../types";
import { ColQLError } from "../errors";
import { assertDictionaryValue, assertDictionaryValues, assertNonNegativeInteger } from "../validation";

type DictionaryCodeArray = Uint8Array | Uint16Array | Uint32Array;
type DictionaryCodeArrayConstructor = new (capacity: number) => DictionaryCodeArray;

function codeArrayForSize(size: number): DictionaryCodeArrayConstructor {
  if (size <= 255) {
    return Uint8Array;
  }

  if (size <= 65_535) {
    return Uint16Array;
  }

  return Uint32Array;
}

export class DictionaryColumnStorage<Values extends readonly string[]>
  implements ColumnStorage<Values[number]>
{
  private readonly codeByValue = new Map<Values[number], number>();
  private data: DictionaryCodeArray;
  private readonly ArrayType: DictionaryCodeArrayConstructor;

  constructor(
    private readonly values: Values,
    capacity: number,
    data?: DictionaryCodeArray,
  ) {
    assertDictionaryValues(values);

    this.ArrayType = codeArrayForSize(values.length);
    this.data = data ?? new this.ArrayType(capacity);
    if (this.data.length !== capacity) {
      throw new ColQLError("COLQL_INVALID_SERIALIZED_DATA", `Dictionary column data length ${this.data.length} does not match capacity ${capacity}.`);
    }

    values.forEach((value, index) => {
      this.codeByValue.set(value, index);
    });
  }

  get capacity(): number {
    return this.data.length;
  }

  get arrayName(): string {
    return this.data.constructor.name;
  }

  get(rowIndex: number): Values[number] {
    this.assertIndex(rowIndex);
    return this.values[this.data[rowIndex]];
  }

  getCode(rowIndex: number): number {
    this.assertIndex(rowIndex);
    return this.data[rowIndex];
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
    this.assertIndex(rowIndex);
    this.data[rowIndex] = this.encode(value);
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

import type { ColumnStorage } from "../types";

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
  ) {
    if (values.length === 0) {
      throw new Error("Dictionary columns require at least one value.");
    }

    this.ArrayType = codeArrayForSize(values.length);
    this.data = new this.ArrayType(capacity);

    values.forEach((value, index) => {
      if (this.codeByValue.has(value)) {
        throw new Error(`Dictionary column contains duplicate value "${value}".`);
      }

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
    const code = this.codeByValue.get(value);
    if (code === undefined) {
      throw new Error(
        `Invalid dictionary value "${String(value)}". Expected one of: ${this.values.join(", ")}.`,
      );
    }

    return code;
  }

  set(rowIndex: number, value: Values[number]): void {
    this.assertIndex(rowIndex);
    this.data[rowIndex] = this.encode(value);
  }

  resize(capacity: number): void {
    if (!Number.isInteger(capacity) || capacity < 0) {
      throw new Error(`Dictionary column capacity must be a non-negative integer. Received ${capacity}.`);
    }

    const next = new this.ArrayType(capacity);
    next.set(this.data.subarray(0, Math.min(this.data.length, capacity)));
    this.data = next;
  }

  private assertIndex(rowIndex: number): void {
    if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= this.data.length) {
      throw new Error(`Row index ${rowIndex} is outside dictionary capacity ${this.data.length}.`);
    }
  }
}

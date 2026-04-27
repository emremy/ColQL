import type { ColumnStorage } from "../types";
import { BitSet } from "./bitset";

export class BooleanColumnStorage implements ColumnStorage<boolean> {
  private readonly bits: BitSet;

  constructor(capacity: number, bytes?: Uint8Array) {
    this.bits = new BitSet(capacity, bytes);
  }

  get capacity(): number {
    return this.bits.capacity;
  }

  get(rowIndex: number): boolean {
    return this.bits.get(rowIndex);
  }

  set(rowIndex: number, value: boolean): void {
    if (typeof value !== "boolean") {
      throw new Error(`Boolean column expects true or false. Received ${String(value)}.`);
    }

    this.bits.set(rowIndex, value);
  }

  resize(capacity: number): void {
    this.bits.resize(capacity);
  }

  toBytes(): Uint8Array {
    return this.bits.toBytes();
  }
}

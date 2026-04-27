const BITS_PER_BYTE = 8;

export class BitSet {
  private bytes: Uint8Array;
  private bitCapacity: number;

  constructor(capacity: number) {
    this.assertCapacity(capacity);
    this.bitCapacity = capacity;
    this.bytes = new Uint8Array(this.bytesForCapacity(capacity));
  }

  get capacity(): number {
    return this.bitCapacity;
  }

  get(index: number): boolean {
    this.assertIndex(index);
    const byteIndex = Math.floor(index / BITS_PER_BYTE);
    const bitOffset = index % BITS_PER_BYTE;
    return (this.bytes[byteIndex] & (1 << bitOffset)) !== 0;
  }

  set(index: number, value: boolean): void {
    this.assertIndex(index);
    const byteIndex = Math.floor(index / BITS_PER_BYTE);
    const bitOffset = index % BITS_PER_BYTE;
    const mask = 1 << bitOffset;

    if (value) {
      this.bytes[byteIndex] |= mask;
      return;
    }

    this.bytes[byteIndex] &= ~mask;
  }

  resize(newCapacity: number): void {
    this.assertCapacity(newCapacity);
    const nextBytes = new Uint8Array(this.bytesForCapacity(newCapacity));
    nextBytes.set(this.bytes.subarray(0, nextBytes.length));
    this.bytes = nextBytes;
    this.bitCapacity = newCapacity;
    this.clearUnusedBits();
  }

  private bytesForCapacity(capacity: number): number {
    return Math.ceil(capacity / BITS_PER_BYTE);
  }

  private assertCapacity(capacity: number): void {
    if (!Number.isInteger(capacity) || capacity < 0) {
      throw new Error(`BitSet capacity must be a non-negative integer. Received ${capacity}.`);
    }
  }

  private assertIndex(index: number): void {
    if (!Number.isInteger(index) || index < 0 || index >= this.bitCapacity) {
      throw new Error(`BitSet index ${index} is outside capacity ${this.bitCapacity}.`);
    }
  }

  private clearUnusedBits(): void {
    const remainder = this.bitCapacity % BITS_PER_BYTE;
    if (remainder === 0 || this.bytes.length === 0) {
      return;
    }

    const keepMask = (1 << remainder) - 1;
    this.bytes[this.bytes.length - 1] &= keepMask;
  }
}

export type StorageBackingKind = "array-buffer" | "shared-array-buffer";

export type TypedArrayWithBuffer =
  | Int16Array
  | Int32Array
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Float32Array
  | Float64Array;

export type TypedArrayConstructor<TArray extends TypedArrayWithBuffer> = {
  readonly BYTES_PER_ELEMENT: number;
  readonly name: string;
  new (length: number): TArray;
  new (buffer: ArrayBufferLike, byteOffset?: number, length?: number): TArray;
};

export function createTypedChunk<TArray extends TypedArrayWithBuffer>(
  ArrayType: TypedArrayConstructor<TArray>,
  length: number,
  backingKind: StorageBackingKind,
): TArray {
  if (backingKind === "shared-array-buffer") {
    if (typeof SharedArrayBuffer === "undefined") {
      throw new Error("SharedArrayBuffer is not available in this runtime.");
    }

    return new ArrayType(
      new SharedArrayBuffer(length * ArrayType.BYTES_PER_ELEMENT),
    );
  }

  return new ArrayType(length);
}

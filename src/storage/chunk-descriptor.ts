import type { NumericColumnType } from "../types";

export type BackgroundIndexColumnKind = "numeric" | "dictionary-code" | "boolean";

export type ChunkBufferKind = "array-buffer" | "shared-array-buffer";

export type TypedChunkArrayName =
  | "Int16Array"
  | "Int32Array"
  | "Uint8Array"
  | "Uint16Array"
  | "Uint32Array"
  | "Float32Array"
  | "Float64Array";

export type ColumnChunkDescriptor = {
  readonly chunkIndex: number;
  readonly rowStart: number;
  readonly logicalLength: number;
  readonly chunkSize: number;
  readonly buffer: ArrayBufferLike;
  readonly bufferKind: ChunkBufferKind;
  readonly byteOffset: number;
  readonly byteLength: number;
  readonly arrayName: TypedChunkArrayName;
  readonly bytesPerElement: number;
};

export type NumericColumnChunkDescriptorSet = {
  readonly columnKind: "numeric";
  readonly valueType: NumericColumnType;
  readonly rowCount: number;
  readonly chunkSize: number;
  readonly chunks: readonly ColumnChunkDescriptor[];
};

export type DictionaryCodeColumnChunkDescriptorSet = {
  readonly columnKind: "dictionary-code";
  readonly codeArrayName: Extract<TypedChunkArrayName, "Uint8Array" | "Uint16Array" | "Uint32Array">;
  readonly dictionarySize: number;
  readonly rowCount: number;
  readonly chunkSize: number;
  readonly chunks: readonly ColumnChunkDescriptor[];
};

export type UnsupportedColumnChunkDescriptorSet = {
  readonly columnKind: "boolean";
  readonly rowCount: number;
  readonly chunkSize: number;
  readonly chunks: readonly [];
  readonly unsupportedReason: "boolean-bit-packed-deferred";
};

export type BackgroundIndexColumnDescriptorSet =
  | NumericColumnChunkDescriptorSet
  | DictionaryCodeColumnChunkDescriptorSet
  | UnsupportedColumnChunkDescriptorSet;

export type BackgroundIndexFeasibility =
  | {
      readonly eligible: true;
      readonly reason: "shared-chunks";
      readonly columnKind: Exclude<BackgroundIndexColumnKind, "boolean">;
      readonly rowCount: number;
      readonly chunkCount: number;
      readonly zeroCopyInput: true;
    }
  | {
      readonly eligible: false;
      readonly reason:
        | "unsupported-column-kind"
        | "non-shared-buffer"
        | "no-readable-chunks";
      readonly columnKind: BackgroundIndexColumnKind;
      readonly rowCount: number;
      readonly chunkCount: number;
      readonly zeroCopyInput: false;
    };

export function describeChunk(
  chunkIndex: number,
  rowStart: number,
  logicalLength: number,
  chunkSize: number,
  array: {
    readonly buffer: ArrayBufferLike;
    readonly byteOffset: number;
    readonly byteLength: number;
    readonly BYTES_PER_ELEMENT: number;
    readonly constructor: { readonly name: string };
  },
): ColumnChunkDescriptor {
  return {
    chunkIndex,
    rowStart,
    logicalLength,
    chunkSize,
    buffer: array.buffer,
    bufferKind: bufferKind(array.buffer),
    byteOffset: array.byteOffset,
    byteLength: logicalLength * array.BYTES_PER_ELEMENT,
    arrayName: array.constructor.name as TypedChunkArrayName,
    bytesPerElement: array.BYTES_PER_ELEMENT,
  };
}

export function backgroundIndexFeasibility(
  descriptor: BackgroundIndexColumnDescriptorSet,
): BackgroundIndexFeasibility {
  if (descriptor.columnKind === "boolean") {
    return {
      eligible: false,
      reason: "unsupported-column-kind",
      columnKind: descriptor.columnKind,
      rowCount: descriptor.rowCount,
      chunkCount: 0,
      zeroCopyInput: false,
    };
  }

  if (descriptor.rowCount > 0 && descriptor.chunks.length === 0) {
    return {
      eligible: false,
      reason: "no-readable-chunks",
      columnKind: descriptor.columnKind,
      rowCount: descriptor.rowCount,
      chunkCount: 0,
      zeroCopyInput: false,
    };
  }

  const allShared = descriptor.chunks.every(
    (chunk) => chunk.bufferKind === "shared-array-buffer",
  );
  if (!allShared) {
    return {
      eligible: false,
      reason: "non-shared-buffer",
      columnKind: descriptor.columnKind,
      rowCount: descriptor.rowCount,
      chunkCount: descriptor.chunks.length,
      zeroCopyInput: false,
    };
  }

  return {
    eligible: true,
    reason: "shared-chunks",
    columnKind: descriptor.columnKind,
    rowCount: descriptor.rowCount,
    chunkCount: descriptor.chunks.length,
    zeroCopyInput: true,
  };
}

function bufferKind(buffer: ArrayBufferLike): ChunkBufferKind {
  return typeof SharedArrayBuffer !== "undefined" &&
    buffer instanceof SharedArrayBuffer
    ? "shared-array-buffer"
    : "array-buffer";
}

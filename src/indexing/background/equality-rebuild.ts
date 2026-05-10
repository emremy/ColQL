import { ColQLError } from "../../errors";
import {
  backgroundIndexFeasibility,
  type BackgroundIndexColumnDescriptorSet,
  type ColumnChunkDescriptor,
  type DictionaryCodeColumnChunkDescriptorSet,
  type NumericColumnChunkDescriptorSet,
  type TypedChunkArrayName,
} from "../../storage/chunk-descriptor";
import { EqualityIndex } from "../equality-index";
import type {
  BackgroundChunkJob,
  BackgroundChunkTask,
  BackgroundJobId,
} from "./types";

type EqualityTypedArray =
  | Int16Array
  | Int32Array
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Float32Array
  | Float64Array;

type EqualityTypedArrayConstructor = {
  readonly BYTES_PER_ELEMENT: number;
  new (length: number): EqualityTypedArray;
  new (
    buffer: ArrayBufferLike,
    byteOffset?: number,
    length?: number,
  ): EqualityTypedArray;
};

export type EqualityBackgroundRebuildJobMetadata = {
  readonly jobId: BackgroundJobId;
  readonly indexId: string;
  readonly indexKind: "equality";
  readonly columnName: string;
  readonly generation: number;
  readonly columnEpoch: number;
};

export type EqualityBackgroundRebuildTaskPayload = {
  readonly descriptor: ColumnChunkDescriptor;
  readonly arrayName: TypedChunkArrayName;
};

export type EqualityEncodedChunkResult = {
  readonly columnName: string;
  readonly chunkIndex: number;
  readonly rowStart: number;
  readonly keyArrayName: TypedChunkArrayName;
  readonly keyCount: number;
  readonly rowIdCount: number;
  readonly keyBuffer: ArrayBuffer;
  readonly offsetsBuffer: ArrayBuffer;
  readonly rowIdsBuffer: ArrayBuffer;
  readonly byteLength: number;
};

export type EqualityBackgroundEligibility =
  | {
      readonly eligible: true;
      readonly reason: "shared-chunks";
      readonly rowCount: number;
      readonly chunkCount: number;
      readonly estimatedOutputBytes: number;
    }
  | {
      readonly eligible: false;
      readonly reason:
        | "unsupported-column-kind"
        | "non-shared-buffer"
        | "no-readable-chunks"
        | "memory-budget";
      readonly rowCount: number;
      readonly chunkCount: number;
      readonly estimatedOutputBytes: number;
    };

const UINT32_BYTES = Uint32Array.BYTES_PER_ELEMENT;

export function equalityBackgroundRebuildEligibility(
  descriptor: BackgroundIndexColumnDescriptorSet,
  memoryBudgetBytes = Number.POSITIVE_INFINITY,
): EqualityBackgroundEligibility {
  const feasibility = backgroundIndexFeasibility(descriptor);
  const estimatedOutputBytes = estimateEqualityOutputBytes(descriptor);
  if (!feasibility.eligible) {
    return {
      eligible: false,
      reason: feasibility.reason,
      rowCount: feasibility.rowCount,
      chunkCount: feasibility.chunkCount,
      estimatedOutputBytes,
    };
  }

  if (estimatedOutputBytes > memoryBudgetBytes) {
    return {
      eligible: false,
      reason: "memory-budget",
      rowCount: descriptor.rowCount,
      chunkCount: descriptor.chunks.length,
      estimatedOutputBytes,
    };
  }

  return {
    eligible: true,
    reason: "shared-chunks",
    rowCount: descriptor.rowCount,
    chunkCount: descriptor.chunks.length,
    estimatedOutputBytes,
  };
}

export function createEqualityBackgroundJob(
  metadata: EqualityBackgroundRebuildJobMetadata,
  descriptor:
    | NumericColumnChunkDescriptorSet
    | DictionaryCodeColumnChunkDescriptorSet,
): BackgroundChunkJob<EqualityBackgroundRebuildTaskPayload> {
  return {
    ...metadata,
    tasks: descriptor.chunks.map((chunk) => ({
      taskId: `${metadata.jobId}:${chunk.chunkIndex}`,
      chunkIndex: chunk.chunkIndex,
      payload: {
        descriptor: chunk,
        arrayName: chunk.arrayName,
      },
    })),
  };
}

export function executeEqualityChunkRebuild(
  task: BackgroundChunkTask<EqualityBackgroundRebuildTaskPayload>,
): EqualityEncodedChunkResult {
  const { descriptor, arrayName } = task.payload;
  if (!descriptor.zeroCopyEligible || descriptor.sharedBuffer === undefined) {
    throw new ColQLError(
      "COLQL_UNSUPPORTED_OPERATION",
      "Equality background rebuild requires SharedArrayBuffer-backed chunk input.",
    );
  }

  const values = typedArrayView(
    arrayName,
    descriptor.sharedBuffer,
    descriptor.byteOffset,
    descriptor.logicalLength,
  );
  const groups = new Map<number, number[]>();

  // This Map is intentionally scoped to one chunk task. It is bounded by the
  // configured chunk size and is encoded before crossing the worker boundary.
  for (let offset = 0; offset < values.length; offset += 1) {
    const key = values[offset];
    const rowIndex = descriptor.rowStart + offset;
    const rows = groups.get(key);
    if (rows === undefined) {
      groups.set(key, [rowIndex]);
    } else {
      rows.push(rowIndex);
    }
  }

  const keys = [...groups.keys()].sort((left, right) => left - right);
  const keyArray = createKeyArray(arrayName, keys.length);
  const offsets = new Uint32Array(keys.length + 1);
  const rowIds = new Uint32Array(descriptor.logicalLength);
  let rowOffset = 0;

  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const rows = groups.get(key) ?? [];
    keyArray[index] = key;
    offsets[index] = rowOffset;
    rowIds.set(rows, rowOffset);
    rowOffset += rows.length;
  }
  offsets[keys.length] = rowOffset;

  return {
    columnName: task.columnName,
    chunkIndex: descriptor.chunkIndex,
    rowStart: descriptor.rowStart,
    keyArrayName: arrayName,
    keyCount: keys.length,
    rowIdCount: rowOffset,
    keyBuffer: keyArray.buffer as ArrayBuffer,
    offsetsBuffer: offsets.buffer,
    rowIdsBuffer: rowIds.buffer,
    byteLength: keyArray.byteLength + offsets.byteLength + rowIds.byteLength,
  };
}

export function mergeEqualityEncodedResults(
  columnName: string,
  results: readonly EqualityEncodedChunkResult[],
): EqualityIndex {
  const index = new EqualityIndex(columnName);
  const orderedResults = [...results].sort(
    (left, right) => left.chunkIndex - right.chunkIndex,
  );

  for (const result of orderedResults) {
    validateEqualityEncodedResult(result);
    const keys = typedArrayView(
      result.keyArrayName,
      result.keyBuffer,
      0,
      result.keyCount,
    );
    const offsets = new Uint32Array(result.offsetsBuffer);
    const rowIds = new Uint32Array(result.rowIdsBuffer);

    for (let keyIndex = 0; keyIndex < keys.length; keyIndex += 1) {
      const key = keys[keyIndex];
      const start = offsets[keyIndex];
      const end = offsets[keyIndex + 1];
      for (let offset = start; offset < end; offset += 1) {
        index.add(key, rowIds[offset]);
      }
    }
  }

  return index;
}

export function validateEqualityEncodedResult(
  result: EqualityEncodedChunkResult,
): void {
  if (!Number.isInteger(result.keyCount) || result.keyCount < 0) {
    throw invalidOutput("keyCount");
  }
  if (!Number.isInteger(result.rowIdCount) || result.rowIdCount < 0) {
    throw invalidOutput("rowIdCount");
  }

  const KeyArray = arrayConstructor(result.keyArrayName);
  if (result.keyBuffer.byteLength !== result.keyCount * KeyArray.BYTES_PER_ELEMENT) {
    throw invalidOutput("keyBuffer");
  }
  if (result.offsetsBuffer.byteLength !== (result.keyCount + 1) * UINT32_BYTES) {
    throw invalidOutput("offsetsBuffer");
  }
  if (result.rowIdsBuffer.byteLength !== result.rowIdCount * UINT32_BYTES) {
    throw invalidOutput("rowIdsBuffer");
  }

  const offsets = new Uint32Array(result.offsetsBuffer);
  if (offsets[result.keyCount] !== result.rowIdCount) {
    throw invalidOutput("offsetsBuffer");
  }
  for (let index = 1; index < offsets.length; index += 1) {
    if (offsets[index] < offsets[index - 1]) {
      throw invalidOutput("offsetsBuffer");
    }
  }
}

function estimateEqualityOutputBytes(
  descriptor: BackgroundIndexColumnDescriptorSet,
): number {
  if (descriptor.columnKind === "boolean") {
    return 0;
  }

  const keyBytes = descriptor.chunks.reduce(
    (total, chunk) => total + chunk.logicalLength * chunk.bytesPerElement,
    0,
  );
  const offsetsBytes = (descriptor.rowCount + descriptor.chunks.length) * UINT32_BYTES;
  const rowIdBytes = descriptor.rowCount * UINT32_BYTES;
  return keyBytes + offsetsBytes + rowIdBytes;
}

function typedArrayView(
  arrayName: TypedChunkArrayName,
  buffer: ArrayBufferLike,
  byteOffset: number,
  length: number,
): EqualityTypedArray {
  return new (arrayConstructor(arrayName))(buffer, byteOffset, length);
}

function createKeyArray(
  arrayName: TypedChunkArrayName,
  length: number,
): EqualityTypedArray {
  return new (arrayConstructor(arrayName))(length);
}

function arrayConstructor(
  arrayName: TypedChunkArrayName,
): EqualityTypedArrayConstructor {
  switch (arrayName) {
    case "Int16Array":
      return Int16Array;
    case "Int32Array":
      return Int32Array;
    case "Uint8Array":
      return Uint8Array;
    case "Uint16Array":
      return Uint16Array;
    case "Uint32Array":
      return Uint32Array;
    case "Float32Array":
      return Float32Array;
    case "Float64Array":
      return Float64Array;
  }
}

function invalidOutput(field: string): ColQLError {
  return new ColQLError(
    "COLQL_INVALID_INDEX_OPERATION",
    `Invalid equality background rebuild output: ${field}.`,
  );
}

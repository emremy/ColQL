import { ColQLError } from "../../errors";
import {
  backgroundIndexFeasibility,
  type BackgroundIndexColumnDescriptorSet,
  type ColumnChunkDescriptor,
  type NumericColumnChunkDescriptorSet,
  type TypedChunkArrayName,
} from "../../storage/chunk-descriptor";
import type {
  BackgroundChunkJob,
  BackgroundChunkTask,
  BackgroundJobId,
} from "./types";

type SortedTypedArray =
  | Int16Array
  | Int32Array
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Float32Array
  | Float64Array;

type SortedTypedArrayConstructor = {
  readonly BYTES_PER_ELEMENT: number;
  new (length: number): SortedTypedArray;
  new (
    buffer: ArrayBufferLike,
    byteOffset?: number,
    length?: number,
  ): SortedTypedArray;
};

export type SortedBackgroundRebuildJobMetadata = {
  readonly jobId: BackgroundJobId;
  readonly indexId: string;
  readonly indexKind: "sorted";
  readonly columnName: string;
  readonly generation: number;
  readonly columnEpoch: number;
  readonly rowCount: number;
};

export type SortedBackgroundRebuildTaskPayload = {
  readonly descriptor: ColumnChunkDescriptor;
  readonly arrayName: TypedChunkArrayName;
};

export type SortedEncodedChunkResult = {
  readonly columnName: string;
  readonly chunkIndex: number;
  readonly rowStart: number;
  readonly rowCount: number;
  readonly valueArrayName: TypedChunkArrayName;
  readonly valuesBuffer: ArrayBuffer;
  readonly rowIdsBuffer: ArrayBuffer;
  readonly byteLength: number;
  readonly minValue?: number;
  readonly maxValue?: number;
};

export type SortedBackgroundEligibility =
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

export function sortedBackgroundRebuildEligibility(
  descriptor: BackgroundIndexColumnDescriptorSet,
  memoryBudgetBytes = Number.POSITIVE_INFINITY,
): SortedBackgroundEligibility {
  const estimatedOutputBytes = estimateSortedOutputBytes(descriptor);
  if (descriptor.columnKind !== "numeric") {
    return {
      eligible: false,
      reason: "unsupported-column-kind",
      rowCount: descriptor.rowCount,
      chunkCount: descriptor.chunks.length,
      estimatedOutputBytes,
    };
  }

  const feasibility = backgroundIndexFeasibility(descriptor);
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

export function createSortedBackgroundJob(
  metadata: SortedBackgroundRebuildJobMetadata,
  descriptor: NumericColumnChunkDescriptorSet,
): BackgroundChunkJob<SortedBackgroundRebuildTaskPayload> {
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

export function executeSortedChunkRebuild(
  task: BackgroundChunkTask<SortedBackgroundRebuildTaskPayload>,
): SortedEncodedChunkResult {
  const { descriptor, arrayName } = task.payload;
  if (!descriptor.zeroCopyEligible || descriptor.sharedBuffer === undefined) {
    throw new ColQLError(
      "COLQL_UNSUPPORTED_OPERATION",
      "Sorted background rebuild requires SharedArrayBuffer-backed numeric chunk input.",
    );
  }

  const sourceValues = typedArrayView(
    arrayName,
    descriptor.sharedBuffer,
    descriptor.byteOffset,
    descriptor.logicalLength,
  );
  const sortedRowIds = new Uint32Array(descriptor.logicalLength);
  for (let offset = 0; offset < sortedRowIds.length; offset += 1) {
    sortedRowIds[offset] = descriptor.rowStart + offset;
  }

  sortedRowIds.sort((leftRowId, rightRowId) => {
    const leftValue = sourceValues[leftRowId - descriptor.rowStart];
    const rightValue = sourceValues[rightRowId - descriptor.rowStart];
    if (leftValue === rightValue) {
      return leftRowId - rightRowId;
    }
    return leftValue - rightValue;
  });

  const sortedValues = createValueArray(arrayName, sortedRowIds.length);
  for (let index = 0; index < sortedRowIds.length; index += 1) {
    sortedValues[index] = sourceValues[sortedRowIds[index] - descriptor.rowStart];
  }

  return {
    columnName: task.columnName,
    chunkIndex: descriptor.chunkIndex,
    rowStart: descriptor.rowStart,
    rowCount: sortedRowIds.length,
    valueArrayName: arrayName,
    valuesBuffer: sortedValues.buffer as ArrayBuffer,
    rowIdsBuffer: sortedRowIds.buffer,
    byteLength: sortedValues.byteLength + sortedRowIds.byteLength,
    ...(sortedValues.length > 0
      ? {
          minValue: sortedValues[0],
          maxValue: sortedValues[sortedValues.length - 1],
        }
      : {}),
  };
}

export function mergeSortedEncodedResults(
  results: readonly SortedEncodedChunkResult[],
  expectedRowCount: number,
): Uint32Array {
  const orderedResults = [...results].sort(
    (left, right) => left.chunkIndex - right.chunkIndex,
  );
  const valueViews: SortedTypedArray[] = [];
  const rowIdViews: Uint32Array[] = [];
  let totalRows = 0;

  for (const result of orderedResults) {
    validateSortedEncodedResult(result);
    totalRows += result.rowCount;
    valueViews.push(typedArrayView(
      result.valueArrayName,
      result.valuesBuffer,
      0,
      result.rowCount,
    ));
    rowIdViews.push(new Uint32Array(result.rowIdsBuffer));
  }

  if (totalRows !== expectedRowCount) {
    throw invalidOutput("rowCount");
  }

  const positions = new Uint32Array(orderedResults.length);
  const output = new Uint32Array(totalRows);
  const seenRowIds = new Uint8Array(expectedRowCount);
  for (let outputOffset = 0; outputOffset < output.length; outputOffset += 1) {
    let selected = -1;
    let selectedValue = 0;
    let selectedRowId = 0;

    for (let chunkIndex = 0; chunkIndex < orderedResults.length; chunkIndex += 1) {
      const position = positions[chunkIndex];
      if (position >= rowIdViews[chunkIndex].length) {
        continue;
      }

      const value = valueViews[chunkIndex][position];
      const rowId = rowIdViews[chunkIndex][position];
      if (
        selected === -1 ||
        value < selectedValue ||
        (value === selectedValue && rowId < selectedRowId)
      ) {
        selected = chunkIndex;
        selectedValue = value;
        selectedRowId = rowId;
      }
    }

    if (selected === -1) {
      throw invalidOutput("rowCount");
    }

    if (selectedRowId >= expectedRowCount || seenRowIds[selectedRowId] !== 0) {
      throw invalidOutput("rowIdsBuffer");
    }

    output[outputOffset] = selectedRowId;
    seenRowIds[selectedRowId] = 1;
    positions[selected] += 1;
  }

  return output;
}

export function validateSortedEncodedResult(
  result: SortedEncodedChunkResult,
): void {
  if (!Number.isInteger(result.rowCount) || result.rowCount < 0) {
    throw invalidOutput("rowCount");
  }

  const ValueArray = arrayConstructor(result.valueArrayName);
  if (result.valuesBuffer.byteLength !== result.rowCount * ValueArray.BYTES_PER_ELEMENT) {
    throw invalidOutput("valuesBuffer");
  }
  if (result.rowIdsBuffer.byteLength !== result.rowCount * UINT32_BYTES) {
    throw invalidOutput("rowIdsBuffer");
  }

  const values = typedArrayView(
    result.valueArrayName,
    result.valuesBuffer,
    0,
    result.rowCount,
  );
  const rowIds = new Uint32Array(result.rowIdsBuffer);
  for (let index = 1; index < result.rowCount; index += 1) {
    const previousValue = values[index - 1];
    const currentValue = values[index];
    const previousRowId = rowIds[index - 1];
    const currentRowId = rowIds[index];
    if (
      currentValue < previousValue ||
      (currentValue === previousValue && currentRowId < previousRowId)
    ) {
      throw invalidOutput("sort-order");
    }
  }
}

function estimateSortedOutputBytes(
  descriptor: BackgroundIndexColumnDescriptorSet,
): number {
  if (descriptor.columnKind !== "numeric") {
    return 0;
  }

  const valueBytes = descriptor.chunks.reduce(
    (total, chunk) => total + chunk.logicalLength * chunk.bytesPerElement,
    0,
  );
  const rowIdBytes = descriptor.rowCount * UINT32_BYTES;
  return valueBytes + rowIdBytes;
}

function typedArrayView(
  arrayName: TypedChunkArrayName,
  buffer: ArrayBufferLike,
  byteOffset: number,
  length: number,
): SortedTypedArray {
  return new (arrayConstructor(arrayName))(buffer, byteOffset, length);
}

function createValueArray(
  arrayName: TypedChunkArrayName,
  length: number,
): SortedTypedArray {
  return new (arrayConstructor(arrayName))(length);
}

function arrayConstructor(
  arrayName: TypedChunkArrayName,
): SortedTypedArrayConstructor {
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
    `Invalid sorted background rebuild output: ${field}.`,
  );
}

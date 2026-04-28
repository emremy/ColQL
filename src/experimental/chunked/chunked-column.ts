import { ColQLError } from "../../errors";

export function assertChunkSize(chunkSize: number): void {
  if (!Number.isInteger(chunkSize) || chunkSize < 1) {
    throw new ColQLError(
      "COLQL_INVALID_LIMIT",
      `Invalid chunk size: expected positive integer, received ${String(chunkSize)}.`,
      { chunkSize },
    );
  }
}

export function assertRowIndex(rowIndex: number, rowCount: number): void {
  if (!Number.isInteger(rowIndex) || rowIndex < 0 || rowIndex >= rowCount) {
    throw new ColQLError(
      "COLQL_INVALID_ROW_INDEX",
      `Invalid row index: expected integer between 0 and ${Math.max(rowCount - 1, 0)}, received ${String(rowIndex)}.`,
      { rowIndex, rowCount },
    );
  }
}

export function locateByChunkLengths(
  rowIndex: number,
  rowCount: number,
  lengths: readonly number[],
): { chunkIndex: number; offset: number } {
  assertRowIndex(rowIndex, rowCount);

  let remaining = rowIndex;
  for (let chunkIndex = 0; chunkIndex < lengths.length; chunkIndex += 1) {
    const length = lengths[chunkIndex];
    if (remaining < length) {
      return { chunkIndex, offset: remaining };
    }
    remaining -= length;
  }

  throw new ColQLError(
    "COLQL_INVALID_ROW_INDEX",
    `Invalid row index: could not locate row ${String(rowIndex)} in chunked storage.`,
    { rowIndex, rowCount },
  );
}

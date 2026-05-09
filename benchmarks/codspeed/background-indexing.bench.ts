import { bench, describe } from "vitest";
import { NumericColumnStorage } from "../../src/storage/numeric-column";
import { DictionaryColumnStorage } from "../../src/storage/dictionary-column";
import {
  createEqualityBackgroundJob,
  executeEqualityChunkRebuild,
  mergeEqualityEncodedResults,
} from "../../src/indexing/background/equality-rebuild";
import {
  createSortedBackgroundJob,
  executeSortedChunkRebuild,
  mergeSortedEncodedResults,
} from "../../src/indexing/background/sorted-rebuild";

const ROWS = 10_000;
const CHUNK_SIZE = 1_024;
const STATUS_VALUES = ["active", "passive", "trial"] as const;

const numericStorage = NumericColumnStorage.withSharedBuffer(
  "uint32",
  ROWS,
  numericData(),
  ROWS,
  CHUNK_SIZE,
);
const dictionaryStorage = DictionaryColumnStorage.withSharedBuffer(
  STATUS_VALUES,
  ROWS,
  dictionaryData(),
  ROWS,
  CHUNK_SIZE,
);
const equalityNumericJob = createEqualityBackgroundJob({
  jobId: "codspeed:equality:numeric",
  indexId: "equality:score",
  indexKind: "equality",
  columnName: "score",
  generation: 1,
  columnEpoch: 1,
}, numericStorage.describeChunks());
const equalityDictionaryJob = createEqualityBackgroundJob({
  jobId: "codspeed:equality:dictionary",
  indexId: "equality:status",
  indexKind: "equality",
  columnName: "status",
  generation: 1,
  columnEpoch: 1,
}, dictionaryStorage.describeChunks());
const sortedJob = createSortedBackgroundJob({
  jobId: "codspeed:sorted:score",
  indexId: "sorted:score",
  indexKind: "sorted",
  columnName: "score",
  generation: 1,
  columnEpoch: 1,
  rowCount: ROWS,
}, numericStorage.describeChunks());

let encodedSink = 0;
describe("background-indexing", () => {
  bench("background/equality/numeric-encode-merge/10k", () => {
    const outputs = equalityNumericJob.tasks.map((task) =>
      executeEqualityChunkRebuild({
        ...task,
        jobId: equalityNumericJob.jobId,
        indexId: equalityNumericJob.indexId,
        indexKind: equalityNumericJob.indexKind,
        columnName: equalityNumericJob.columnName,
        generation: equalityNumericJob.generation,
        columnEpoch: equalityNumericJob.columnEpoch,
      }),
    );
    encodedSink += mergeEqualityEncodedResults("score", outputs).stats().uniqueValues;
  });

  bench("background/equality/dictionary-code-encode-merge/10k", () => {
    const outputs = equalityDictionaryJob.tasks.map((task) =>
      executeEqualityChunkRebuild({
        ...task,
        jobId: equalityDictionaryJob.jobId,
        indexId: equalityDictionaryJob.indexId,
        indexKind: equalityDictionaryJob.indexKind,
        columnName: equalityDictionaryJob.columnName,
        generation: equalityDictionaryJob.generation,
        columnEpoch: equalityDictionaryJob.columnEpoch,
      }),
    );
    encodedSink += mergeEqualityEncodedResults("status", outputs).stats().uniqueValues;
  });

  bench("background/sorted/numeric-encode-merge/10k", () => {
    const outputs = sortedJob.tasks.map((task) =>
      executeSortedChunkRebuild({
        ...task,
        jobId: sortedJob.jobId,
        indexId: sortedJob.indexId,
        indexKind: sortedJob.indexKind,
        columnName: sortedJob.columnName,
        generation: sortedJob.generation,
        columnEpoch: sortedJob.columnEpoch,
      }),
    );
    encodedSink += mergeSortedEncodedResults(outputs, ROWS)[0];
  });
});

function numericData(): Uint32Array {
  const data = new Uint32Array(ROWS);
  for (let row = 0; row < ROWS; row += 1) {
    data[row] = (row * 1_103) % 1_000_000;
  }
  return data;
}

function dictionaryData(): Uint8Array {
  const data = new Uint8Array(ROWS);
  for (let row = 0; row < ROWS; row += 1) {
    data[row] = row % 17 === 0 ? 2 : row % 3 === 0 ? 1 : 0;
  }
  return data;
}

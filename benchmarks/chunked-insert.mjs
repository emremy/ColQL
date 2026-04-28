import { column, table } from "../dist/index.mjs";
import {
  BenchmarkChunkedTable,
  CHUNK_SIZES,
  formatMB,
  formatMs,
  formatRows,
  makeRow,
  memoryMB,
  productionRows,
  rowCountsFromEnv,
  time,
} from "./chunked-benchmark-utils.mjs";

function createEmptyProductionTable() {
  return table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    status: column.dictionary(["active", "passive", "archived"]),
    is_active: column.boolean(),
  });
}

console.log("ColQL chunked insert benchmark");
console.log("Insert path: insert rows one-by-one and insertMany batches");
console.log("Tip: use COLQL_BENCH_LARGE=1 for 1M rows.\n");

for (const rowCount of rowCountsFromEnv()) {
  console.log(`Dataset: ${rowCount.toLocaleString()} rows`);
  const rows = productionRows(rowCount);

  let baseline = createEmptyProductionTable();
  const baselineInsert = time(() => {
    for (let i = 0; i < rowCount; i += 1) baseline.insert(makeRow(i));
  });
  const baselineInsertMemory = memoryMB();

  baseline = createEmptyProductionTable();
  const baselineInsertMany = time(() => baseline.insertMany(rows));
  const baselineInsertManyMemory = memoryMB();

  console.log("\nINSERT:");
  console.log(`baseline insert:     ${formatMs(baselineInsert.duration)} (${formatRows(rowCount, baselineInsert.duration)})`);
  console.log(`baseline insertMany: ${formatMs(baselineInsertMany.duration)} (${formatRows(rowCount, baselineInsertMany.duration)})`);
  console.log("\nMEMORY:");
  console.log(`baseline insert tracked total:     ${formatMB(baselineInsertMemory.total)}`);
  console.log(`baseline insertMany tracked total: ${formatMB(baselineInsertManyMemory.total)}`);

  baseline = undefined;

  for (const chunkSize of CHUNK_SIZES) {
    let chunked = new BenchmarkChunkedTable(chunkSize);
    const insert = time(() => {
      for (let i = 0; i < rowCount; i += 1) chunked.insert(makeRow(i));
    });
    const insertMemory = memoryMB();

    chunked = new BenchmarkChunkedTable(chunkSize);
    const insertMany = time(() => chunked.insertMany(rows));
    const insertManyMemory = memoryMB();

    if (chunked.rowCount !== rowCount) {
      throw new Error(`Chunked insert sanity check failed for chunk size ${chunkSize}.`);
    }

    console.log(`\nchunk ${chunkSize.toLocaleString()}:`);
    console.log(`insert:          ${formatMs(insert.duration)} (${formatRows(rowCount, insert.duration)})`);
    console.log(`insertMany:      ${formatMs(insertMany.duration)} (${formatRows(rowCount, insertMany.duration)})`);
    console.log(`insert memory:   ${formatMB(insertMemory.total)}`);
    console.log(`insertMany mem:  ${formatMB(insertManyMemory.total)}`);
    console.log(`storage approx:  ${formatMB(chunked.memoryBytesApprox() / 1024 / 1024)}`);

    chunked = undefined;
  }

  console.log("\n---\n");
}

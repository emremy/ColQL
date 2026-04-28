import {
  CHUNK_SIZES,
  GET_OPS,
  createChunkedTable,
  createProductionTable,
  formatMB,
  formatMs,
  formatOps,
  formatRows,
  memoryMB,
  randomIndexes,
  rowCountsFromEnv,
  time,
} from "./chunked-benchmark-utils.mjs";

console.log("ColQL chunked storage benchmark");
console.log("Read path: full scan + random get(rowIndex)");
console.log("Tip: use COLQL_BENCH_LARGE=1 for 1M rows.\n");

for (const rowCount of rowCountsFromEnv()) {
  console.log(`Dataset: ${rowCount.toLocaleString()} rows`);
  const indexes = randomIndexes(GET_OPS, rowCount);

  let baseline = createProductionTable(rowCount);
  const baselineMemory = memoryMB();
  const baselineScan = time(() => {
    let total = 0;
    for (let rowIndex = 0; rowIndex < baseline.rowCount; rowIndex += 1) {
      total += baseline.getValue(rowIndex, "age");
    }
    return total;
  });
  const baselineGet = time(() => {
    let total = 0;
    for (const rowIndex of indexes) total += baseline.get(rowIndex).age;
    return total;
  });

  console.log("\nSCAN:");
  console.log(`baseline:       ${formatMs(baselineScan.duration)} (${formatRows(rowCount, baselineScan.duration)})`);
  console.log("\nGET:");
  console.log(`baseline:       ${formatMs(baselineGet.duration)} (${formatOps(GET_OPS, baselineGet.duration)})`);
  console.log("\nMEMORY:");
  console.log(`baseline heapUsed:       ${formatMB(baselineMemory.heapUsed)}`);
  console.log(`baseline arrayBuffers:   ${formatMB(baselineMemory.arrayBuffers)}`);
  console.log(`baseline tracked total:  ${formatMB(baselineMemory.total)}`);

  baseline = undefined;

  for (const chunkSize of CHUNK_SIZES) {
    let chunked = createChunkedTable(rowCount, chunkSize);
    const chunkedMemory = memoryMB();
    const scanByRowGet = time(() => chunked.scanAgeSumByRowGet());
    const scanByChunks = time(() => chunked.scanAgeSumByChunks());
    const get = time(() => {
      let total = 0;
      for (const rowIndex of indexes) total += chunked.get(rowIndex).age;
      return total;
    });

    if (scanByRowGet.result !== baselineScan.result || scanByChunks.result !== baselineScan.result || get.result !== baselineGet.result) {
      throw new Error(`Chunked read sanity check failed for chunk size ${chunkSize}.`);
    }

    console.log(`\nchunk ${chunkSize.toLocaleString()}:`);
    console.log(`scan via row get: ${formatMs(scanByRowGet.duration)} (${formatRows(rowCount, scanByRowGet.duration)})`);
    console.log(`scan by chunks:   ${formatMs(scanByChunks.duration)} (${formatRows(rowCount, scanByChunks.duration)})`);
    console.log(`get random rows:  ${formatMs(get.duration)} (${formatOps(GET_OPS, get.duration)})`);
    console.log(`heapUsed:         ${formatMB(chunkedMemory.heapUsed)}`);
    console.log(`arrayBuffers:     ${formatMB(chunkedMemory.arrayBuffers)}`);
    console.log(`tracked total:    ${formatMB(chunkedMemory.total)}`);
    console.log(`storage approx:   ${formatMB(chunked.memoryBytesApprox() / 1024 / 1024)}`);

    chunked = undefined;
  }

  console.log("\n---\n");
}

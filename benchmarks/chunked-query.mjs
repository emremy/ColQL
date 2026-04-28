import {
  CHUNK_SIZES,
  createChunkedTable,
  createProductionTable,
  formatMB,
  formatMs,
  formatRows,
  memoryMB,
  randomIndexes,
  rowCountsFromEnv,
  time,
} from "./chunked-benchmark-utils.mjs";

console.log("ColQL chunked query benchmark");
console.log("Scan-based predicates: age > 18, status = active, id = target");
console.log("Tip: use COLQL_BENCH_LARGE=1 for 1M rows.\n");

for (const rowCount of rowCountsFromEnv()) {
  console.log(`Dataset: ${rowCount.toLocaleString()} rows`);
  const target = rowCount - 10;

  let baseline = createProductionTable(rowCount);
  const baselineMemory = memoryMB();
  const baselineAge = time(() => baseline.where("age", ">", 18).count());
  const baselineStatus = time(() => baseline.where("status", "=", "active").count());
  const baselineId = time(() => baseline.where("id", "=", target).count());

  console.log("\nQUERY:");
  console.log(`baseline age > 18:        ${formatMs(baselineAge.duration)} (${formatRows(rowCount, baselineAge.duration)})`);
  console.log(`baseline status = active: ${formatMs(baselineStatus.duration)} (${formatRows(rowCount, baselineStatus.duration)})`);
  console.log(`baseline id = target:     ${formatMs(baselineId.duration)} (${formatRows(rowCount, baselineId.duration)})`);
  console.log("\nMEMORY:");
  console.log(`baseline tracked total:   ${formatMB(baselineMemory.total)}`);

  baseline = undefined;

  for (const chunkSize of CHUNK_SIZES) {
    let chunked = createChunkedTable(rowCount, chunkSize);
    const chunkedMemory = memoryMB();
    const age = time(() => chunked.countWhere("age", ">", 18));
    const status = time(() => chunked.countWhere("status", "=", "active"));
    const id = time(() => chunked.countWhere("id", "=", target));

    if (age.result !== baselineAge.result || status.result !== baselineStatus.result || id.result !== baselineId.result) {
      throw new Error(`Chunked query sanity check failed for chunk size ${chunkSize}.`);
    }

    console.log(`\nchunk ${chunkSize.toLocaleString()}:`);
    console.log(`age > 18:        ${formatMs(age.duration)} (${formatRows(rowCount, age.duration)})`);
    console.log(`status = active: ${formatMs(status.duration)} (${formatRows(rowCount, status.duration)})`);
    console.log(`id = target:     ${formatMs(id.duration)} (${formatRows(rowCount, id.duration)})`);
    console.log(`tracked total:   ${formatMB(chunkedMemory.total)}`);
    console.log(`storage approx:  ${formatMB(chunked.memoryBytesApprox() / 1024 / 1024)}`);

    chunked = undefined;
  }

  console.log("\nCOMBINED WORKLOAD:");
  console.log("insert existing dataset -> delete 10k random rows -> scan age -> query status");

  for (const chunkSize of CHUNK_SIZES) {
    const deleteCount = Math.min(10_000, Math.floor(rowCount / 2));
    const deleteIndexes = randomIndexes(deleteCount, rowCount);
    let chunked = createChunkedTable(rowCount, chunkSize);
    const workload = time(() => {
      for (const rowIndex of deleteIndexes) chunked.delete(Math.min(rowIndex, chunked.rowCount - 1));
      const scanTotal = chunked.scanAgeSumByRowGet();
      const queryCount = chunked.countWhere("status", "=", "active");
      return { scanTotal, queryCount };
    });

    if (workload.result.scanTotal <= 0 || workload.result.queryCount <= 0) {
      throw new Error(`Chunked combined workload sanity check failed for chunk size ${chunkSize}.`);
    }

    console.log(`chunk ${chunkSize.toLocaleString()}: ${formatMs(workload.duration)} after ${deleteCount.toLocaleString()} deletes`);
    chunked = undefined;
  }

  console.log("\n---\n");
}

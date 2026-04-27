import { column, table } from "../dist/index.mjs";

const RUNS = 3;
const DEFAULT_ROWS = 100_000;
const LARGE_ROWS = 1_000_000;
const rowCounts = process.argv[2]
  ? [Number.parseInt(process.argv[2], 10)]
  : process.env.COLQL_BENCH_LARGE === "1"
    ? [DEFAULT_ROWS, LARGE_ROWS]
    : [DEFAULT_ROWS];

for (const rows of rowCounts) {
  if (!Number.isInteger(rows) || rows < 1) {
    throw new Error(`Row count must be a positive integer. Received ${String(rows)}.`);
  }
}

function format(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function average(values) {
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function collect() {
  if (global.gc) {
    global.gc();
  }
}

function memory() {
  collect();
  const usage = process.memoryUsage();
  return {
    heapUsed: usage.heapUsed,
    arrayBuffers: usage.arrayBuffers,
    trackedTotal: usage.heapUsed + usage.arrayBuffers,
  };
}

function diff(after, before) {
  return {
    heapUsed: after.heapUsed - before.heapUsed,
    arrayBuffers: after.arrayBuffers - before.arrayBuffers,
    trackedTotal: after.trackedTotal - before.trackedTotal,
  };
}

function createObjectArray(rowCount) {
  const users = [];
  for (let i = 0; i < rowCount; i += 1) {
    users.push({
      id: i,
      age: i % 100,
      status: i % 2 === 0 ? "active" : "passive",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

function createColqlTable(rowCount) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive"]),
    is_active: column.boolean(),
  });

  for (let i = 0; i < rowCount; i += 1) {
    users.insert({
      id: i,
      age: i % 100,
      status: i % 2 === 0 ? "active" : "passive",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

function runOnce(rows) {
  collect();
  const baseline = memory();
  const objectArray = createObjectArray(rows);
  const objectMemory = diff(memory(), baseline);

  objectArray.length = 0;
  collect();

  const beforeColql = memory();
  const users = createColqlTable(rows);
  const colqlMemory = diff(memory(), beforeColql);

  if (users.rowCount !== rows) {
    throw new Error(`Sanity check failed: expected ${rows} rows, got ${users.rowCount}.`);
  }

  return { objectMemory, colqlMemory };
}

console.log("ColQL memory benchmark");
console.log("Tip: run with `COLQL_BENCH_LARGE=1 npm run benchmark:memory` to include 1M rows.");
console.log("Tip: --expose-gc is enabled by the npm script for steadier numbers.");

for (const rows of rowCounts) {
  const runs = Array.from({ length: RUNS }, () => runOnce(rows));
  const objectHeap = average(runs.map((run) => run.objectMemory.heapUsed));
  const objectBuffers = average(runs.map((run) => run.objectMemory.arrayBuffers));
  const objectTotal = average(runs.map((run) => run.objectMemory.trackedTotal));
  const colqlHeap = average(runs.map((run) => run.colqlMemory.heapUsed));
  const colqlBuffers = average(runs.map((run) => run.colqlMemory.arrayBuffers));
  const colqlTotal = average(runs.map((run) => run.colqlMemory.trackedTotal));

  console.log(`\n${rows.toLocaleString()} rows, average over ${RUNS} runs:`);
  console.log(`Object Array heapUsed:      ${format(objectHeap)}`);
  console.log(`Object Array arrayBuffers:  ${format(objectBuffers)}`);
  console.log(`Object Array tracked total: ${format(objectTotal)}`);
  console.log(`ColQL heapUsed:             ${format(colqlHeap)}`);
  console.log(`ColQL arrayBuffers:         ${format(colqlBuffers)}`);
  console.log(`ColQL tracked total:        ${format(colqlTotal)}`);
  console.log(`heapUsed reduction:         ~${(objectHeap / Math.max(colqlHeap, 1)).toFixed(2)}x`);
  console.log(`tracked total reduction:    ~${(objectTotal / Math.max(colqlTotal, 1)).toFixed(2)}x`);
}

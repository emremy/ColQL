import { column, table } from "../dist/index.mjs";

const DEFAULT_ROWS = 250_000;
const LARGE_ROWS = 1_000_000;
const RANDOM_DELETES = 1_000;
const rowCounts = process.argv[2]
  ? [Number.parseInt(process.argv[2], 10)]
  : process.env.COLQL_BENCH_LARGE === "1"
    ? [DEFAULT_ROWS, LARGE_ROWS]
    : [DEFAULT_ROWS];

function forceGC() {
  if (typeof global.gc === "function") global.gc();
}

function snapshot(label) {
  forceGC();
  const m = process.memoryUsage();
  return {
    label,
    heapUsed: m.heapUsed,
    arrayBuffers: m.arrayBuffers,
    trackedTotal: m.heapUsed + m.arrayBuffers,
  };
}

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

function formatMs(value) {
  return `${value.toFixed(3)}ms`;
}

function formatMB(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function printSnapshot(item) {
  console.log(`${item.label.padEnd(32)} heap ${formatMB(item.heapUsed).padStart(9)} | buffers ${formatMB(item.arrayBuffers).padStart(9)} | total ${formatMB(item.trackedTotal).padStart(9)}`);
}

function createUsers(rowCount) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    status: column.dictionary(["active", "passive", "archived"]),
    is_active: column.boolean(),
  });

  for (let i = 0; i < rowCount; i += 1) {
    users.insert({
      id: i,
      age: i % 100,
      score: i / 10,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

function randomIndexes(count, rowCount) {
  let seed = 42;
  const indexes = [];
  let remaining = rowCount;
  for (let i = 0; i < count; i += 1) {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    indexes.push(seed % remaining);
    remaining -= 1;
  }
  return indexes;
}

function measureSingleDelete(rowCount, rowIndexFor) {
  let users = createUsers(rowCount);
  const result = time(() => users.delete(rowIndexFor(users.rowCount)));
  users = null;
  forceGC();
  return result.duration;
}

console.log("ColQL production delete benchmark");
console.log("chunkSize: 65,536");
console.log("Memory snapshots force GC when available and report heapUsed + arrayBuffers.\n");

for (const rows of rowCounts) {
  console.log(`${rows.toLocaleString()} rows`);

  const phases = [];
  phases.push(snapshot("start"));

  const firstDuration = measureSingleDelete(rows, () => 0);
  const middleDuration = measureSingleDelete(rows, (rowCount) => Math.floor(rowCount / 2));
  const lastDuration = measureSingleDelete(rows, (rowCount) => rowCount - 1);
  phases.push(snapshot("after single deletes"));

  let users = createUsers(rows);
  phases.push(snapshot("after build"));

  users.createIndex("id").createIndex("status");
  phases.push(snapshot("after indexes"));
  console.log(`indexes: ${JSON.stringify(users.indexes())}`);
  console.log(`sortedIndexes: ${typeof users.sortedIndexes === "function" ? JSON.stringify(users.sortedIndexes()) : "[]"}`);

  let randomRows = randomIndexes(RANDOM_DELETES, rows);
  phases.push(snapshot("after random index generation"));

  const randomDuration = time(() => {
    for (const rowIndex of randomRows) users.delete(rowIndex);
  });
  phases.push(snapshot("after 1k random deletes"));

  randomRows = null;
  phases.push(snapshot("after delete GC"));
  phases.push(snapshot("before query"));

  const firstIndexedCount = time(() => users.where("status", "=", "active").where("age", ">", 18).count());
  phases.push(snapshot("after first indexed count"));

  const secondIndexedCount = time(() => users.where("status", "=", "active").where("age", ">", 18).count());
  phases.push(snapshot("after second indexed count"));

  let result = null;
  const toArrayQuery = time(() => {
    result = users.where("status", "=", "active").where("age", ">", 18).toArray();
    return result.length;
  });
  phases.push(snapshot("after query toArray"));

  result = null;
  phases.push(snapshot("after query result released"));

  users.dropIndex("id");
  users.dropIndex("status");
  phases.push(snapshot("after indexes dropped"));

  if (firstIndexedCount.result <= 0 || secondIndexedCount.result !== firstIndexedCount.result || toArrayQuery.result !== firstIndexedCount.result) {
    throw new Error("Delete benchmark sanity check failed.");
  }

  console.log(`delete first row:              ${formatMs(firstDuration)}`);
  console.log(`delete middle row:             ${formatMs(middleDuration)}`);
  console.log(`delete last row:               ${formatMs(lastDuration)}`);
  console.log(`delete 1k random rows:         ${formatMs(randomDuration.duration)}`);
  console.log(`first indexed query count():   ${formatMs(firstIndexedCount.duration)}`);
  console.log(`second indexed query count():  ${formatMs(secondIndexedCount.duration)}`);
  console.log(`query after deletes toArray(): ${formatMs(toArrayQuery.duration)}`);
  console.log("\nMEMORY PHASES:");
  for (const phase of phases) printSnapshot(phase);

  users = null;
  phases.length = 0;
  forceGC();
  console.log("");
}

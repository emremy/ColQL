import { column, table } from "../dist/index.mjs";

const DEFAULT_ROWS = 250_000;
const LARGE_ROWS = 1_000_000;
const RANDOM_DELETES = 1_000;
const rowCount = process.argv[2]
  ? Number.parseInt(process.argv[2], 10)
  : process.env.COLQL_BENCH_LARGE === "1"
    ? LARGE_ROWS
    : DEFAULT_ROWS;

if (!Number.isInteger(rowCount) || rowCount < 1) {
  throw new Error(`Row count must be a positive integer. Received ${String(rowCount)}.`);
}

function mb(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function snapshot(label) {
  if (global.gc) {
    global.gc();
  }

  const memory = process.memoryUsage();
  return {
    label,
    heapUsed: memory.heapUsed,
    arrayBuffers: memory.arrayBuffers,
    trackedTotal: memory.heapUsed + memory.arrayBuffers,
  };
}

function printSnapshot(item) {
  console.log(`${item.label.padEnd(31)} heap ${mb(item.heapUsed).padStart(9)} | buffers ${mb(item.arrayBuffers).padStart(9)} | total ${mb(item.trackedTotal).padStart(9)}`);
}

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

function createUsers(rows) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    score: column.float64(),
    status: column.dictionary(["active", "passive", "archived"]),
    is_active: column.boolean(),
  });

  for (let i = 0; i < rows; i += 1) {
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

function randomIndexes(count, rows) {
  let seed = 0xdecafbad;
  const indexes = [];
  for (let index = 0; index < count; index += 1) {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    indexes.push(seed % (rows - index));
  }
  return indexes;
}

function measureSingleDelete(rows, pickIndex) {
  const users = createUsers(rows);
  return time(() => users.delete(pickIndex(rows))).duration;
}

function measureSingleUpdate(rows, pickIndex) {
  const users = createUsers(rows);
  return time(() => users.update(pickIndex(rows), { age: 42, status: "active" })).duration;
}

const snapshots = [];
snapshots.push(snapshot("start"));

const deleteFirst = measureSingleDelete(rowCount, () => 0);
const deleteMiddle = measureSingleDelete(rowCount, (rows) => Math.floor(rows / 2));
const deleteLast = measureSingleDelete(rowCount, (rows) => rows - 1);
const updateFirst = measureSingleUpdate(rowCount, () => 0);
const updateMiddle = measureSingleUpdate(rowCount, (rows) => Math.floor(rows / 2));
const updateLast = measureSingleUpdate(rowCount, (rows) => rows - 1);
snapshots.push(snapshot("after single deletes"));

const users = createUsers(rowCount);
snapshots.push(snapshot("after build"));

users.createIndex("id").createIndex("status").createSortedIndex("age");
snapshots.push(snapshot("after indexes"));

const predicateUpdate = time(() => users.updateWhere("status", "=", "archived", { is_active: true }));
snapshots.push(snapshot("after predicate update"));
const firstQueryAfterUpdate = time(() => users.where("status", "=", "archived").count());
snapshots.push(snapshot("after update index rebuild"));
users.rebuildIndexes();
snapshots.push(snapshot("after explicit rebuild"));

let rowsToDelete = randomIndexes(RANDOM_DELETES, users.rowCount);
snapshots.push(snapshot("after random index generation"));

const randomDeletes = time(() => {
  for (const index of rowsToDelete) {
    users.delete(index);
  }
});
rowsToDelete = null;
snapshots.push(snapshot("after 1k random deletes"));
snapshots.push(snapshot("after delete GC"));
snapshots.push(snapshot("before query"));

const firstIndexedCount = time(() => users.where("id", "=", Math.floor(rowCount / 2)).count());
snapshots.push(snapshot("after first indexed count"));
const secondIndexedCount = time(() => users.where("id", "=", Math.floor(rowCount / 2)).count());
snapshots.push(snapshot("after second indexed count"));
const countAfterDeletes = time(() => users.where("status", "=", "active").count());
snapshots.push(snapshot("after status count"));
let result = time(() => users.where("status", "=", "active").select(["id", "age", "status"]).limit(10_000).toArray());
snapshots.push(snapshot("after query toArray"));
const materializedRows = result.result.length;
const toArrayDuration = result.duration;
result = null;
snapshots.push(snapshot("after query result released"));

const activeCount = countAfterDeletes.result;
const updatedRows = predicateUpdate.result.affectedRows;
const firstQueryAfterUpdateResult = firstQueryAfterUpdate.result;
const firstIndexedResult = firstIndexedCount.result;
const secondIndexedResult = secondIndexedCount.result;
users.dropIndex("id").dropIndex("status").dropSortedIndex("age");
snapshots.push(snapshot("after indexes dropped"));

console.log("ColQL production delete benchmark");
console.log(`${rowCount.toLocaleString()} rows`);
console.log("chunkSize: 65,536");
console.log("Indexes are marked dirty by delete and rebuilt lazily on first indexed query.");
console.log("Tip: run with `COLQL_BENCH_LARGE=1 npm run benchmark:delete` to benchmark 1M rows.\n");
console.log(`delete first row:              ${deleteFirst.toFixed(3)}ms`);
console.log(`delete middle row:             ${deleteMiddle.toFixed(3)}ms`);
console.log(`delete last row:               ${deleteLast.toFixed(3)}ms`);
console.log(`update first row:              ${updateFirst.toFixed(3)}ms`);
console.log(`update middle row:             ${updateMiddle.toFixed(3)}ms`);
console.log(`update last row:               ${updateLast.toFixed(3)}ms`);
console.log(`predicate update:              ${predicateUpdate.duration.toFixed(3)}ms (${updatedRows} rows)`);
console.log(`query after update count:      ${firstQueryAfterUpdate.duration.toFixed(3)}ms (${firstQueryAfterUpdateResult} rows)`);
console.log(`delete 1k random rows:         ${randomDeletes.duration.toFixed(3)}ms`);
console.log(`first indexed query count:     ${firstIndexedCount.duration.toFixed(3)}ms (${firstIndexedResult} rows)`);
console.log(`second indexed query count:    ${secondIndexedCount.duration.toFixed(3)}ms (${secondIndexedResult} rows)`);
console.log(`query after deletes count():   ${countAfterDeletes.duration.toFixed(3)}ms (${activeCount} rows)`);
console.log(`query after deletes toArray(): ${toArrayDuration.toFixed(3)}ms (${materializedRows.toLocaleString()} rows materialized)\n`);
console.log("MEMORY PHASES:");
for (const item of snapshots) {
  printSnapshot(item);
}
console.log("\ntracked total = heapUsed + arrayBuffers");

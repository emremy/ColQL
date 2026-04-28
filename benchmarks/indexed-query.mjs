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

function average(values) {
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

function createUsers(rowCount) {
  const users = table({
    id: column.uint32(),
    age: column.uint8(),
    status: column.dictionary(["active", "passive", "blocked"]),
    is_active: column.boolean(),
  });

  for (let i = 0; i < rowCount; i += 1) {
    users.insert({
      id: i,
      age: i % 100,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "blocked",
      is_active: i % 2 === 0,
    });
  }

  return users;
}

function runOnce(rows) {
  const scanUsers = createUsers(rows);
  const indexedUsers = createUsers(rows);
  indexedUsers.createIndex("status");
  indexedUsers.createIndex("id");

  const scanStatus = time(() => scanUsers.where("status", "=", "active").count());
  const plannedStatus = time(() => indexedUsers.where("status", "=", "active").count());
  const scanId = time(() => scanUsers.where("id", "=", rows - 10).count());
  const plannedId = time(() => indexedUsers.where("id", "=", rows - 10).count());
  const scanStatusInAll = time(() => scanUsers.whereIn("status", ["active", "passive", "blocked"]).count());
  const plannedStatusInAll = time(() => indexedUsers.whereIn("status", ["active", "passive", "blocked"]).count());
  const scanIdInSelective = time(() => scanUsers.where("id", "in", [10, Math.floor(rows / 2), rows - 10]).count());
  const plannedIdInSelective = time(() => indexedUsers.where("id", "in", [10, Math.floor(rows / 2), rows - 10]).count());
  const plannedPlusFilter = time(() => indexedUsers.where("status", "=", "active").where("age", ">", 50).count());

  if (
    scanStatus.result !== plannedStatus.result ||
    scanId.result !== plannedId.result ||
    scanStatusInAll.result !== plannedStatusInAll.result ||
    scanIdInSelective.result !== plannedIdInSelective.result ||
    plannedPlusFilter.result <= 0
  ) {
    throw new Error("Indexed benchmark sanity check failed.");
  }

  return {
    scanStatus,
    plannedStatus,
    scanId,
    plannedId,
    scanStatusInAll,
    plannedStatusInAll,
    scanIdInSelective,
    plannedIdInSelective,
    plannedPlusFilter,
  };
}

console.log("ColQL indexed query benchmark");
console.log("Planner skips indexes when candidate ratio exceeds 40%.");
console.log("Tip: run with `COLQL_BENCH_LARGE=1 npm run benchmark:indexed` to include 1M rows.");

for (const rows of rowCounts) {
  const runs = Array.from({ length: RUNS }, () => runOnce(rows));

  console.log(`\n${rows.toLocaleString()} rows, average over ${RUNS} runs:`);
  console.log(`scan status = active:        ${average(runs.map((run) => run.scanStatus.duration)).toFixed(3)}ms`);
  console.log(`planned status = active:     ${average(runs.map((run) => run.plannedStatus.duration)).toFixed(3)}ms`);
  console.log(`scan id = ${rows - 10}:             ${average(runs.map((run) => run.scanId.duration)).toFixed(3)}ms`);
  console.log(`planned id = ${rows - 10}:          ${average(runs.map((run) => run.plannedId.duration)).toFixed(3)}ms`);
  console.log(`scan status in all:          ${average(runs.map((run) => run.scanStatusInAll.duration)).toFixed(3)}ms`);
  console.log(`planned status in all:       ${average(runs.map((run) => run.plannedStatusInAll.duration)).toFixed(3)}ms`);
  console.log(`scan id in selective:        ${average(runs.map((run) => run.scanIdInSelective.duration)).toFixed(3)}ms`);
  console.log(`planned id in selective:     ${average(runs.map((run) => run.plannedIdInSelective.duration)).toFixed(3)}ms`);
  console.log(`planned + age filter:        ${average(runs.map((run) => run.plannedPlusFilter.duration)).toFixed(3)}ms`);
}

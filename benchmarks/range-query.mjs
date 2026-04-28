import { column, table } from "../dist/index.mjs";

const RUNS = 3;
const DEFAULT_ROWS = 250_000;
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
    score: column.float64(),
    status: column.dictionary(["active", "passive", "blocked"]),
  });

  for (let i = 0; i < rowCount; i += 1) {
    users.insert({
      id: i,
      age: i % 100,
      score: i / 10,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "blocked",
    });
  }

  return users;
}

function runOnce(rows) {
  const scanUsers = createUsers(rows);
  const indexedUsers = createUsers(rows);
  indexedUsers.createSortedIndex("age");
  indexedUsers.createIndex("id");

  const scanAgeGt90 = time(() => scanUsers.where("age", ">", 90).count());
  const sortedAgeGt90 = time(() => indexedUsers.where("age", ">", 90).count());
  const scanAgeGte90 = time(() => scanUsers.where("age", ">=", 90).count());
  const sortedAgeGte90 = time(() => indexedUsers.where("age", ">=", 90).count());
  const scanAgeLt10 = time(() => scanUsers.where("age", "<", 10).count());
  const sortedAgeLt10 = time(() => indexedUsers.where("age", "<", 10).count());
  const broadScanAgeGt10 = time(() => scanUsers.where("age", ">", 10).count());
  const plannedAgeGt10 = time(() => indexedUsers.where("age", ">", 10).count());
  const scanAgeThenId = time(() => scanUsers.where("age", ">", 18).where("id", "=", rows - 10).count());
  const plannedIdThenAge = time(() => indexedUsers.where("age", ">", 18).where("id", "=", rows - 10).count());

  if (
    scanAgeGt90.result !== sortedAgeGt90.result ||
    scanAgeGte90.result !== sortedAgeGte90.result ||
    scanAgeLt10.result !== sortedAgeLt10.result ||
    broadScanAgeGt10.result !== plannedAgeGt10.result ||
    scanAgeThenId.result !== plannedIdThenAge.result
  ) {
    throw new Error("Range benchmark sanity check failed.");
  }

  return {
    scanAgeGt90,
    sortedAgeGt90,
    scanAgeGte90,
    sortedAgeGte90,
    scanAgeLt10,
    sortedAgeLt10,
    broadScanAgeGt10,
    plannedAgeGt10,
    scanAgeThenId,
    plannedIdThenAge,
  };
}

console.log("ColQL range query benchmark");
console.log("Planner skips indexes when candidate ratio exceeds 40%.");
console.log("Tip: run with `COLQL_BENCH_LARGE=1 npm run benchmark:range` to include 1M rows.");

for (const rows of rowCounts) {
  const runs = Array.from({ length: RUNS }, () => runOnce(rows));

  console.log(`\n${rows.toLocaleString()} rows, average over ${RUNS} runs:`);
  console.log(`scan age > 90:                  ${average(runs.map((run) => run.scanAgeGt90.duration)).toFixed(3)}ms`);
  console.log(`sorted age > 90:                ${average(runs.map((run) => run.sortedAgeGt90.duration)).toFixed(3)}ms`);
  console.log(`scan age >= 90:                 ${average(runs.map((run) => run.scanAgeGte90.duration)).toFixed(3)}ms`);
  console.log(`sorted age >= 90:               ${average(runs.map((run) => run.sortedAgeGte90.duration)).toFixed(3)}ms`);
  console.log(`scan age < 10:                  ${average(runs.map((run) => run.scanAgeLt10.duration)).toFixed(3)}ms`);
  console.log(`sorted age < 10:                ${average(runs.map((run) => run.sortedAgeLt10.duration)).toFixed(3)}ms`);
  console.log(`broad scan age > 10:            ${average(runs.map((run) => run.broadScanAgeGt10.duration)).toFixed(3)}ms`);
  console.log(`planned age > 10:               ${average(runs.map((run) => run.plannedAgeGt10.duration)).toFixed(3)}ms`);
  console.log(`scan age > 18 then id target:   ${average(runs.map((run) => run.scanAgeThenId.duration)).toFixed(3)}ms`);
  console.log(`planned id target + age filter: ${average(runs.map((run) => run.plannedIdThenAge.duration)).toFixed(3)}ms`);
}

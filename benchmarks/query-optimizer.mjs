import { column, table } from "../dist/index.mjs";

const RUNS = 3;
const DEFAULT_ROWS = 250_000;
const LARGE_ROWS = 1_000_000;
const rowCounts = process.argv[2]
  ? [Number.parseInt(process.argv[2], 10)]
  : process.env.COLQL_BENCH_LARGE === "1"
    ? [DEFAULT_ROWS, LARGE_ROWS]
    : [DEFAULT_ROWS];

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
  });

  for (let i = 0; i < rowCount; i += 1) {
    users.insert({
      id: i,
      age: i % 100,
      status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "blocked",
    });
  }

  return users;
}

function runOnce(rows) {
  const scanUsers = createUsers(rows);
  const plannedUsers = createUsers(rows);
  plannedUsers.createIndex("id");
  plannedUsers.createIndex("status");
  plannedUsers.createSortedIndex("age");

  const target = rows - 10;
  const scanAgeThenId = time(() => scanUsers.where("age", ">", 18).where("id", "=", target).count());
  const plannedIdThenAge = time(() => plannedUsers.where("age", ">", 18).where("id", "=", target).count());
  const scanStatusThenAge = time(() => scanUsers.where("status", "=", "active").where("age", ">", 90).count());
  const plannedStatusThenAge = time(() => plannedUsers.where("status", "=", "active").where("age", ">", 90).count());

  if (scanAgeThenId.result !== plannedIdThenAge.result || scanStatusThenAge.result !== plannedStatusThenAge.result) {
    throw new Error("Optimizer benchmark sanity check failed.");
  }

  return { scanAgeThenId, plannedIdThenAge, scanStatusThenAge, plannedStatusThenAge };
}

console.log("ColQL query optimizer benchmark");
console.log("Planner chooses the smallest useful indexed candidate source and reorders filter evaluation.");
console.log("Tip: run with `COLQL_BENCH_LARGE=1 npm run benchmark:optimizer` to include 1M rows.");

for (const rows of rowCounts) {
  const runs = Array.from({ length: RUNS }, () => runOnce(rows));

  console.log(`\n${rows.toLocaleString()} rows, average over ${RUNS} runs:`);
  console.log(`scan age > 18 then id = target:      ${average(runs.map((run) => run.scanAgeThenId.duration)).toFixed(3)}ms`);
  console.log(`planned id = target + age filter:    ${average(runs.map((run) => run.plannedIdThenAge.duration)).toFixed(3)}ms`);
  console.log(`scan status = active then age > 90:  ${average(runs.map((run) => run.scanStatusThenAge.duration)).toFixed(3)}ms`);
  console.log(`planned age > 90 + status filter:    ${average(runs.map((run) => run.plannedStatusThenAge.duration)).toFixed(3)}ms`);
}

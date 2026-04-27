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

function time(label, fn) {
  const start = performance.now();
  const result = fn();
  const duration = performance.now() - start;
  return { label, duration, result };
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
  const arr = createObjectArray(rows);
  const users = createColqlTable(rows);

  const results = [
    time("array.filter where", () => arr.filter((user) => user.age >= 18 && user.status === "active").length),
    time("ColQL where count", () => users.where("age", ">=", 18).where("status", "=", "active").count()),
    time("array select + limit", () =>
      arr
        .filter((user) => user.age >= 18 && user.status === "active")
        .slice(0, 100)
        .map((user) => ({ id: user.id, age: user.age, status: user.status })).length,
    ),
    time("ColQL select + limit", () =>
      users
        .where("age", ">=", 18)
        .where("status", "=", "active")
        .select(["id", "age", "status"])
        .limit(100)
        .toArray().length,
    ),
  ];

  if (results[0].result !== results[1].result || results[2].result !== results[3].result) {
    throw new Error("Benchmark sanity check failed: array and ColQL results differ.");
  }

  return results;
}

console.log("ColQL query benchmark");
console.log("Tip: run with `COLQL_BENCH_LARGE=1 npm run benchmark:query` to include 1M rows.");

for (const rows of rowCounts) {
  const runs = Array.from({ length: RUNS }, () => runOnce(rows));
  const labels = runs[0].map((result) => result.label);

  console.log(`\n${rows.toLocaleString()} rows, average over ${RUNS} runs:`);
  for (let index = 0; index < labels.length; index += 1) {
    const avg = average(runs.map((run) => run[index].duration));
    console.log(`${labels[index]}: ${avg.toFixed(3)}ms`);
  }
}

import { column, table } from "../dist/index.mjs";

const RUNS = 3;
const DEFAULT_ROWS = 100_000;
const LARGE_ROWS = 1_000_000;
const rowCounts = process.argv[2]
  ? [Number.parseInt(process.argv[2], 10)]
  : process.env.MEMQL_BENCH_LARGE === "1"
    ? [DEFAULT_ROWS, LARGE_ROWS]
    : [DEFAULT_ROWS];

function average(values) {
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function formatBytes(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function createMemqlTable(rowCount) {
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
  const users = createMemqlTable(rows);
  const serializeStart = performance.now();
  const buffer = users.serialize();
  const serializeMs = performance.now() - serializeStart;

  const deserializeStart = performance.now();
  const restored = table.deserialize(buffer);
  const deserializeMs = performance.now() - deserializeStart;

  if (restored.rowCount !== users.rowCount || restored.count() !== users.count()) {
    throw new Error("Serialization benchmark sanity check failed.");
  }

  return { serializeMs, deserializeMs, size: buffer.byteLength };
}

console.log("memql serialization benchmark");
console.log("Tip: run with `MEMQL_BENCH_LARGE=1 npm run benchmark:serialization` to include 1M rows.");

for (const rows of rowCounts) {
  const runs = Array.from({ length: RUNS }, () => runOnce(rows));
  console.log(`\n${rows.toLocaleString()} rows, average over ${RUNS} runs:`);
  console.log(`serialize:   ${average(runs.map((run) => run.serializeMs)).toFixed(3)}ms`);
  console.log(`deserialize: ${average(runs.map((run) => run.deserializeMs)).toFixed(3)}ms`);
  console.log(`size:        ${formatBytes(average(runs.map((run) => run.size)))}`);
}

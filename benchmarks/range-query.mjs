import { column, table } from "../dist/index.mjs";

const rows = process.env.COLQL_BENCH_LARGE === "1" ? 1_000_000 : 250_000;
function time(fn) { const start = performance.now(); const result = fn(); return { duration: performance.now() - start, result }; }
const users = table({ id: column.uint32(), age: column.uint8(), score: column.float64() });
for (let i = 0; i < rows; i += 1) users.insert({ id: i, age: i % 100, score: i / 10 });
console.log("ColQL range query benchmark");
console.log(`${rows.toLocaleString()} rows`);
for (const [label, op, value] of [["age > 90", ">", 90], ["age >= 90", ">=", 90], ["age < 10", "<", 10]]) {
  const run = time(() => users.where("age", op, value).count());
  console.log(`${label}: ${run.duration.toFixed(3)}ms (${run.result.toLocaleString()} rows)`);
}

import { column, table } from "../dist/index.mjs";

const rows = process.env.COLQL_BENCH_LARGE === "1" ? 1_000_000 : 250_000;
function time(fn) { const start = performance.now(); const result = fn(); return { duration: performance.now() - start, result }; }
const users = table({ id: column.uint32(), age: column.uint8(), status: column.dictionary(["active", "passive", "archived"]) });
for (let i = 0; i < rows; i += 1) users.insert({ id: i, age: i % 100, status: i % 3 === 0 ? "active" : i % 3 === 1 ? "passive" : "archived" });
users.createIndex("id");
console.log("ColQL query optimizer benchmark");
console.log(`${rows.toLocaleString()} rows`);
const planned = time(() => users.where("age", ">", 18).where("id", "=", rows - 10).count());
const scanLike = time(() => users.where("status", "=", "active").where("age", ">", 90).count());
console.log(`planned id target + age filter: ${planned.duration.toFixed(3)}ms (${planned.result} rows)`);
console.log(`status + age scan filter:       ${scanLike.duration.toFixed(3)}ms (${scanLike.result.toLocaleString()} rows)`);

import { column, table } from "../dist/index.mjs";

const DEFAULT_ROWS = 100_000;
const rows = Number.parseInt(process.argv[2] ?? String(DEFAULT_ROWS), 10);

if (!Number.isInteger(rows) || rows < 1) {
  throw new Error(`Row count must be a positive integer. Received ${process.argv[2]}.`);
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

console.log(`memql query benchmark (${rows.toLocaleString()} rows)`);

const arr = createObjectArray(rows);
const users = createMemqlTable(rows);

console.time("array.filter where");
const arrayWhere = arr.filter((user) => user.age >= 18 && user.status === "active");
console.timeEnd("array.filter where");

console.time("memql where count");
const memqlCount = users.where("age", ">=", 18).where("status", "=", "active").count();
console.timeEnd("memql where count");

console.time("array filter count");
const arrayCount = arr.filter((user) => user.age >= 18 && user.status === "active").length;
console.timeEnd("array filter count");

console.time("memql count");
const count = users.where("age", ">=", 18).where("status", "=", "active").count();
console.timeEnd("memql count");

console.time("array select + limit");
const arrayLimited = arr
  .filter((user) => user.age >= 18 && user.status === "active")
  .slice(0, 100)
  .map((user) => ({ id: user.id, age: user.age, status: user.status }));
console.timeEnd("array select + limit");

console.time("memql select + limit");
const memqlLimited = users
  .where("age", ">=", 18)
  .where("status", "=", "active")
  .select(["id", "age", "status"])
  .limit(100)
  .toArray();
console.timeEnd("memql select + limit");

if (arrayWhere.length !== memqlCount || arrayCount !== count || arrayLimited.length !== memqlLimited.length) {
  throw new Error("Benchmark sanity check failed: array and memql results differ.");
}

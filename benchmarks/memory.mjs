import { column, table } from "../dist/index.mjs";

const DEFAULT_ROWS = 100_000;
const rows = Number.parseInt(process.argv[2] ?? String(DEFAULT_ROWS), 10);

if (!Number.isInteger(rows) || rows < 1) {
  throw new Error(`Row count must be a positive integer. Received ${process.argv[2]}.`);
}

function format(bytes) {
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function collect() {
  if (global.gc) {
    global.gc();
  }
}

function memory() {
  collect();
  const usage = process.memoryUsage();
  return {
    heapUsed: usage.heapUsed,
    arrayBuffers: usage.arrayBuffers,
    trackedTotal: usage.heapUsed + usage.arrayBuffers,
  };
}

function diff(after, before) {
  return {
    heapUsed: after.heapUsed - before.heapUsed,
    arrayBuffers: after.arrayBuffers - before.arrayBuffers,
    trackedTotal: after.trackedTotal - before.trackedTotal,
  };
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

console.log(`memql memory benchmark (${rows.toLocaleString()} rows)`);
console.log("Tip: run with `node --expose-gc benchmarks/memory.mjs` for steadier numbers.");

collect();
const baseline = memory();
const objectArray = createObjectArray(rows);
const objectMemory = diff(memory(), baseline);
console.log(`Object Array heapUsed:      ${format(objectMemory.heapUsed)}`);
console.log(`Object Array arrayBuffers:  ${format(objectMemory.arrayBuffers)}`);
console.log(`Object Array tracked total: ${format(objectMemory.trackedTotal)}`);

objectArray.length = 0;
collect();

const beforeMemql = memory();
const users = createMemqlTable(rows);
const memqlMemory = diff(memory(), beforeMemql);
console.log(`memql heapUsed:             ${format(memqlMemory.heapUsed)}`);
console.log(`memql arrayBuffers:         ${format(memqlMemory.arrayBuffers)}`);
console.log(`memql tracked total:        ${format(memqlMemory.trackedTotal)}`);
console.log(`heapUsed reduction:         ~${(objectMemory.heapUsed / Math.max(memqlMemory.heapUsed, 1)).toFixed(2)}x`);
console.log(`tracked total reduction:    ~${(objectMemory.trackedTotal / Math.max(memqlMemory.trackedTotal, 1)).toFixed(2)}x`);

if (users.rowCount !== rows) {
  throw new Error(`Sanity check failed: expected ${rows} rows, got ${users.rowCount}.`);
}

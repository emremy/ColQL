import os from "node:os";
import { column, fromRows, table } from "../dist/index.mjs";

const DEFAULT_SIZES = [1_000, 100_000, 1_000_000];
const RUNS = 3;

const sizes = process.env.COLQL_ARRAY_BENCH_SIZES
  ? process.env.COLQL_ARRAY_BENCH_SIZES.split(",").map((value) => Number.parseInt(value.trim(), 10))
  : DEFAULT_SIZES;
const jsonOutput = process.argv.includes("--json");

for (const size of sizes) {
  if (!Number.isInteger(size) || size < 1) {
    throw new Error(`Invalid benchmark size: ${String(size)}`);
  }
}

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  score: column.uint32(),
  status: column.dictionary(["active", "passive", "archived"]),
  active: column.boolean(),
};

function mb(bytes) {
  return bytes / 1024 / 1024;
}

function memoryTotal() {
  const usage = process.memoryUsage();
  return usage.heapUsed + usage.arrayBuffers;
}

function forceGc() {
  if (global.gc) {
    global.gc();
  }
}

function createRows(rowCount) {
  return Array.from({ length: rowCount }, (_unused, id) => ({
    id,
    age: (id * 7) % 100,
    score: (id * 13) % 10_000,
    status: id % 3 === 0 ? "active" : id % 3 === 1 ? "passive" : "archived",
    active: id % 2 === 0,
  }));
}

function createTable(rows, indexes = "none") {
  const users = fromRows(schema, rows);
  if (indexes === "equality") {
    users.createIndex("id").createIndex("status");
  }
  if (indexes === "sorted") {
    users.createSortedIndex("age");
  }
  if (indexes === "unique") {
    users.createUniqueIndex("id");
  }
  return users;
}

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

function average(values) {
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)];
}

function measureMemory(label, factory) {
  forceGc();
  const before = memoryTotal();
  const value = factory();
  forceGc();
  const after = memoryTotal();
  return { label, ms: 0, memoryMB: Math.max(0, mb(after - before)), value };
}

function runWorkloads(rows) {
  const rowCount = rows.length;
  const targetId = rowCount - 10;
  const broadAge = 10;
  const selectiveAge = 95;
  const results = [];

  const memoryArray = measureMemory("memory: JS object array", () => createRows(rowCount));
  results.push({ label: memoryArray.label, ms: 0, memoryMB: memoryArray.memoryMB });
  memoryArray.value.length = 0;

  const memoryColql = measureMemory("memory: ColQL scan table", () => createTable(rows));
  results.push({ label: memoryColql.label, ms: 0, memoryMB: memoryColql.memoryMB });

  const buildFromRows = time(() => createTable(rows).rowCount);
  results.push({ label: "fromRows / insertMany", ms: buildFromRows.duration });
  if (buildFromRows.result !== rowCount) throw new Error("fromRows sanity check failed.");

  const scan = createTable(rows);
  const equality = createTable(rows, "equality");
  const sorted = createTable(rows, "sorted");
  const unique = createTable(rows, "unique");

  const workloads = [
    { label: "filter/count: JS array", fn: () => rows.filter((row) => row.status === "active" && row.age >= 18).length },
    { label: "filter/count: ColQL scan", fn: () => scan.where({ status: "active", age: { gte: 18 } }).count() },
    { label: "filter/count: ColQL equality index", fn: () => equality.where({ status: "active", age: { gte: 18 } }).count() },
    [
      "projection+limit: JS array",
      () =>
        rows
          .filter((row) => row.status === "active" && row.age >= 18)
          .slice(0, 100)
          .map((row) => ({ id: row.id, age: row.age })).length,
    ],
    [
      "projection+limit: ColQL",
      () => scan.where({ status: "active", age: { gte: 18 } }).select(["id", "age"]).limit(100).toArray().length,
    ],
    { label: "find by id: JS array", fn: () => rows.find((row) => row.id === targetId)?.id },
    { label: "find by id: ColQL equality index", fn: () => equality.where("id", "=", targetId).first()?.id },
    { label: "unique lookup findBy", fn: () => unique.findBy("id", targetId)?.id },
    { label: "exists: JS array", fn: () => rows.some((row) => row.id === targetId) },
    { label: "exists: ColQL helper", fn: () => equality.exists("id", "=", targetId) },
    { label: "countWhere: JS array", fn: () => rows.filter((row) => row.status === "passive").length },
    { label: "countWhere: ColQL helper", fn: () => equality.countWhere({ status: "passive" }) },
    { label: "range query: JS array", fn: () => rows.filter((row) => row.age >= selectiveAge).length },
    { label: "range query: ColQL scan", fn: () => scan.where("age", ">=", selectiveAge).count() },
    { label: "range query: ColQL sorted index", fn: () => sorted.where("age", ">=", selectiveAge).count() },
    { label: "broad scan: JS array", fn: () => rows.filter((row) => row.age >= broadAge).length },
    { label: "broad scan: ColQL", fn: () => sorted.where("age", ">=", broadAge).count() },
    { label: "callback filter(fn): JS array", fn: () => rows.filter((row) => row.active && row.score % 7 === 0).length },
    { label: "callback filter(fn): ColQL", fn: () => scan.filter((row) => row.active && row.score % 7 === 0).count() },
    [
      "update by predicate: JS array",
      () => rows.map((row) => (row.status === "archived" ? { ...row, status: "passive" } : row)).filter((row) => row.status === "passive").length,
    ],
    {
      label: "update by predicate: ColQL",
      prepare: () => createTable(rows),
      fn: (users) => {
        users.updateMany({ status: "archived" }, { status: "passive" });
        return users.where("status", "=", "passive").count();
      },
    },
    [
      "delete by predicate: JS array",
      () => rows.filter((row) => row.age < selectiveAge).length,
    ],
    {
      label: "delete by predicate: ColQL",
      prepare: () => createTable(rows),
      fn: (users) => {
        users.deleteMany({ age: { gte: selectiveAge } });
        return users.rowCount;
      },
    },
    {
      label: "updateBy: unique index",
      prepare: () => createTable(rows, "unique"),
      fn: (users) => {
        const result = users.updateBy("id", targetId, { score: 123 });
        return `${result.affectedRows}:${users.findBy("id", targetId)?.score}`;
      },
    },
    {
      label: "deleteBy: unique index",
      prepare: () => createTable(rows, "unique"),
      fn: (users) => {
        const result = users.deleteBy("id", targetId);
        return `${result.affectedRows}:${users.findBy("id", targetId) === undefined}`;
      },
    },
  ];

  const expected = new Map();
  for (const workload of workloads) {
    const normalizedWorkload = Array.isArray(workload)
      ? { label: workload[0], fn: workload[1] }
      : workload;
    const runs = Array.from({ length: RUNS }, () => {
      const input = normalizedWorkload.prepare?.();
      return time(() => normalizedWorkload.fn(input));
    });
    const normalized = runs.map((run) => (run.result === undefined ? "undefined" : JSON.stringify(run.result)));
    if (new Set(normalized).size !== 1) {
      throw new Error(`Benchmark sanity check failed for ${normalizedWorkload.label}.`);
    }

    const group = normalizedWorkload.label.replace(/: (JS array|ColQL.*|unique index)$/, "");
    const value = normalized[0];
    if (expected.has(group) && expected.get(group) !== value) {
      throw new Error(`Array and ColQL results differ for ${group}.`);
    }
    expected.set(group, value);

    results.push({ label: normalizedWorkload.label, ms: average(runs.map((run) => run.duration)), medianMs: median(runs.map((run) => run.duration)) });
  }

  return results;
}

function printHuman(allResults) {
  console.log("JS Array vs ColQL comparison benchmark");
  console.log(`Node ${process.version} on ${process.platform} ${process.arch}`);
  console.log(`CPU: ${os.cpus()[0]?.model ?? "unknown"} (${os.cpus().length} logical cores)`);
  console.log("Caveats: local machine only; results vary with Node version, CPU, memory pressure, data distribution, selectivity, and workload shape.");
  console.log("These numbers are not CI requirements or universal guarantees.\n");

  for (const group of allResults) {
    console.log(`${group.rows.toLocaleString()} rows, average over ${RUNS} runs`);
    console.log("workload                                      avg ms    median ms   memory MB");
    console.log("--------------------------------------------------------------------------");
    for (const result of group.results) {
      const ms = result.ms.toFixed(3).padStart(8);
      const medianMs = (result.medianMs ?? result.ms).toFixed(3).padStart(9);
      const memory = result.memoryMB === undefined ? "".padStart(9) : result.memoryMB.toFixed(2).padStart(9);
      console.log(`${result.label.padEnd(44)} ${ms} ${medianMs} ${memory}`);
    }
    console.log("");
  }
}

const allResults = sizes.map((rows) => {
  const sourceRows = createRows(rows);
  return { rows, results: runWorkloads(sourceRows) };
});

if (jsonOutput) {
  console.log(JSON.stringify({ env: { node: process.version, platform: process.platform, arch: process.arch, cpu: os.cpus()[0]?.model }, results: allResults }, null, 2));
} else {
  printHuman(allResults);
}

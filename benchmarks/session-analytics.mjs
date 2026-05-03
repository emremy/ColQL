import os from "node:os";
import { column, fromRows, table } from "../dist/index.mjs";

const DEFAULT_ROWS = 25_000;
const NOW = 1_725_000_000;
const STALE_ACTIVE_CUTOFF = NOW - 7 * 86_400;
const OLD_INACTIVE_CUTOFF = NOW - 21 * 86_400;

const rows = process.env.ROWS
  ? Number.parseInt(process.env.ROWS, 10)
  : DEFAULT_ROWS;
const jsonOutput = process.argv.includes("--json");

if (!Number.isInteger(rows) || rows < 1) {
  throw new Error(`Invalid ROWS value: ${String(process.env.ROWS)}`);
}

const schema = {
  id: column.uint32(),
  userId: column.uint32(),
  segment: column.dictionary(["free", "pro", "enterprise"]),
  status: column.dictionary(["active", "expired", "inactive"]),
  startedAt: column.uint32(),
  durationMs: column.uint32(),
  country: column.dictionary(["US", "TR", "DE", "GB"]),
};

function createRows(rowCount) {
  return Array.from({ length: rowCount }, (_unused, id) => ({
    id,
    userId: 10_000 + (id % 3_000),
    segment: id % 19 === 0 ? "enterprise" : id % 4 === 0 ? "pro" : "free",
    status: id % 29 === 0 ? "inactive" : id % 7 === 0 ? "expired" : "active",
    startedAt: NOW - ((id * 137) % (30 * 86_400)),
    durationMs: 30_000 + ((id * 9_973) % 9_000_000),
    country: id % 11 === 0 ? "TR" : id % 7 === 0 ? "DE" : id % 5 === 0 ? "GB" : "US",
  }));
}

function createIndexedTable(sourceRows) {
  return fromRows(schema, sourceRows)
    .createIndex("status")
    .createIndex("segment")
    .createIndex("country")
    .createSortedIndex("startedAt")
    .createSortedIndex("durationMs");
}

function time(fn) {
  const start = performance.now();
  const result = fn();
  return { duration: performance.now() - start, result };
}

function scanType(query) {
  return query.explain().scanType;
}

function pushResult(results, phase, rowCount, operation, resultCount, scan, duration) {
  results.push({
    phase,
    rows: rowCount,
    operation,
    resultCount,
    scanType: scan,
    ms: duration,
  });
}

function assertCount(label, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${label} sanity check failed: expected ${expected}, received ${actual}.`);
  }
}

function updateOracle(sourceRows, predicate, patch) {
  let affectedRows = 0;
  for (const row of sourceRows) {
    if (!predicate(row)) {
      continue;
    }
    Object.assign(row, patch);
    affectedRows += 1;
  }
  return affectedRows;
}

function deleteOracle(sourceRows, predicate) {
  let affectedRows = 0;
  for (let index = sourceRows.length - 1; index >= 0; index -= 1) {
    if (!predicate(sourceRows[index])) {
      continue;
    }
    sourceRows.splice(index, 1);
    affectedRows += 1;
  }
  return affectedRows;
}

function queryCount(query) {
  return query.count();
}

function queryArrayLength(query) {
  return query.toArray().length;
}

function runSessionAnalyticsBenchmark(rowCount) {
  const results = [];
  const dataset = time(() => createRows(rowCount));
  const sourceRows = dataset.result;
  const oracleRows = sourceRows.map((row) => ({ ...row }));
  pushResult(results, "dataset", rowCount, "generate deterministic sessions", sourceRows.length, "n/a", dataset.duration);

  const setup = time(() => createIndexedTable(sourceRows));
  const sessions = setup.result;
  pushResult(results, "setup", rowCount, "insert rows and create explicit indexes", sessions.rowCount, "n/a", setup.duration);

  const activeQuery = sessions.where("status", "=", "inactive");
  const activeExpected = oracleRows.filter((row) => row.status === "inactive").length;
  const activeScan = scanType(activeQuery);
  const active = time(() => queryCount(activeQuery));
  assertCount("equality index query", active.result, activeExpected);
  pushResult(results, "query", rowCount, "equality index: status = inactive", active.result, activeScan, active.duration);

  const windowStart = NOW - 6 * 60 * 60;
  const windowQuery = sessions.where({ startedAt: { gte: windowStart, lte: NOW } });
  const windowExpected = oracleRows.filter((row) => row.startedAt >= windowStart && row.startedAt <= NOW).length;
  const windowScan = scanType(windowQuery);
  const window = time(() => queryCount(windowQuery));
  assertCount("sorted range query", window.result, windowExpected);
  pushResult(results, "query", rowCount, "sorted range: startedAt last 6h", window.result, windowScan, window.duration);

  const projectionQuery = sessions
    .where({ country: "TR", status: "active" })
    .select(["id", "userId", "startedAt", "durationMs"])
    .limit(100);
  const projectionExpected = oracleRows.filter((row) => row.country === "TR" && row.status === "active").slice(0, 100).length;
  const projectionScan = scanType(projectionQuery);
  const projection = time(() => queryArrayLength(projectionQuery));
  assertCount("projection + limit query", projection.result, projectionExpected);
  pushResult(results, "query", rowCount, "projection + limit: active TR sessions", projection.result, projectionScan, projection.duration);

  const combinedQuery = sessions.where({ segment: "enterprise", startedAt: { gte: NOW - 3 * 86_400 } });
  const combinedExpected = oracleRows.filter((row) => row.segment === "enterprise" && row.startedAt >= NOW - 3 * 86_400).length;
  const combinedScan = scanType(combinedQuery);
  const combined = time(() => queryCount(combinedQuery));
  assertCount("combined equality + range query", combined.result, combinedExpected);
  pushResult(results, "query", rowCount, "combined: enterprise sessions in last 3d", combined.result, combinedScan, combined.duration);

  const callbackQuery = sessions.filter((row) => row.status === "active" && row.durationMs > 8_000_000);
  const callbackExpected = oracleRows.filter((row) => row.status === "active" && row.durationMs > 8_000_000).length;
  const callbackScan = scanType(callbackQuery);
  const callback = time(() => queryCount(callbackQuery));
  assertCount("callback full scan query", callback.result, callbackExpected);
  pushResult(results, "query", rowCount, "callback filter: active slow sessions", callback.result, callbackScan, callback.duration);

  const updateExpected = updateOracle(
    oracleRows,
    (row) => row.status === "active" && row.segment === "enterprise" && row.startedAt < STALE_ACTIVE_CUTOFF,
    { status: "expired" },
  );
  const update = time(() =>
    sessions.updateMany(
      { status: "active", segment: "enterprise", startedAt: { lt: STALE_ACTIVE_CUTOFF } },
      { status: "expired" },
    ),
  );
  assertCount("expire mutation", update.result.affectedRows, updateExpected);
  pushResult(results, "mutation", rowCount, "update: expire stale enterprise sessions", update.result.affectedRows, "n/a", update.duration);

  const firstDirtyQuery = sessions.where("status", "=", "inactive");
  const firstDirtyExpected = oracleRows.filter((row) => row.status === "inactive").length;
  const firstDirtyScan = scanType(firstDirtyQuery);
  const firstDirty = time(() => queryCount(firstDirtyQuery));
  assertCount("first post-update dirty index query", firstDirty.result, firstDirtyExpected);
  pushResult(results, "post-mutation", rowCount, "first query after dirty equality index", firstDirty.result, firstDirtyScan, firstDirty.duration);

  const secondDirtyQuery = sessions.where("status", "=", "inactive");
  const secondDirtyScan = scanType(secondDirtyQuery);
  const secondDirty = time(() => queryCount(secondDirtyQuery));
  assertCount("second post-update equality query", secondDirty.result, firstDirtyExpected);
  pushResult(results, "post-mutation", rowCount, "second identical equality query", secondDirty.result, secondDirtyScan, secondDirty.duration);

  const deleteExpected = deleteOracle(
    oracleRows,
    (row) => row.status === "inactive" && row.startedAt < OLD_INACTIVE_CUTOFF,
  );
  const deletion = time(() =>
    sessions.deleteMany({
      status: "inactive",
      startedAt: { lt: OLD_INACTIVE_CUTOFF },
    }),
  );
  assertCount("delete mutation", deletion.result.affectedRows, deleteExpected);
  pushResult(results, "mutation", rowCount, "delete: old inactive sessions", deletion.result.affectedRows, "n/a", deletion.duration);

  const firstRangeAfterDeleteQuery = sessions.where("startedAt", "<", OLD_INACTIVE_CUTOFF);
  const firstRangeAfterDeleteExpected = oracleRows.filter((row) => row.startedAt < OLD_INACTIVE_CUTOFF).length;
  const firstRangeAfterDeleteScan = scanType(firstRangeAfterDeleteQuery);
  const firstRangeAfterDelete = time(() => queryCount(firstRangeAfterDeleteQuery));
  assertCount("first post-delete dirty sorted query", firstRangeAfterDelete.result, firstRangeAfterDeleteExpected);
  pushResult(results, "post-mutation", rowCount, "first query after dirty sorted index", firstRangeAfterDelete.result, firstRangeAfterDeleteScan, firstRangeAfterDelete.duration);

  const secondRangeAfterDeleteQuery = sessions.where("startedAt", "<", OLD_INACTIVE_CUTOFF);
  const secondRangeAfterDeleteScan = scanType(secondRangeAfterDeleteQuery);
  const secondRangeAfterDelete = time(() => queryCount(secondRangeAfterDeleteQuery));
  assertCount("second post-delete sorted query", secondRangeAfterDelete.result, firstRangeAfterDeleteExpected);
  pushResult(results, "post-mutation", rowCount, "second identical sorted query", secondRangeAfterDelete.result, secondRangeAfterDeleteScan, secondRangeAfterDelete.duration);

  const serialization = time(() => sessions.serialize());
  pushResult(results, "lifecycle", sessions.rowCount, "serialize table data", serialization.result.byteLength, "n/a", serialization.duration);

  const restore = time(() => table.deserialize(serialization.result));
  const restored = restore.result;
  assertCount("restore row count", restored.rowCount, oracleRows.length);
  pushResult(results, "lifecycle", restored.rowCount, "restore table data", restored.rowCount, "n/a", restore.duration);

  const recreateIndexes = time(() => {
    restored
      .createIndex("status")
      .createIndex("segment")
      .createIndex("country")
      .createSortedIndex("startedAt")
      .createSortedIndex("durationMs");
    return restored.indexes().length + restored.sortedIndexes().length;
  });
  assertCount("recreate index count", recreateIndexes.result, 5);
  pushResult(results, "lifecycle", restored.rowCount, "recreate explicit indexes", recreateIndexes.result, "n/a", recreateIndexes.duration);

  const restoredQuery = restored.where("status", "=", "inactive");
  const restoredExpected = oracleRows.filter((row) => row.status === "inactive").length;
  const restoredScan = scanType(restoredQuery);
  const restoredRun = time(() => queryCount(restoredQuery));
  assertCount("restored indexed query", restoredRun.result, restoredExpected);
  pushResult(results, "lifecycle", restored.rowCount, "indexed query after restore + reindex", restoredRun.result, restoredScan, restoredRun.duration);

  return results;
}

function formatMs(value) {
  return value.toFixed(3);
}

function printHuman(results) {
  console.log("ColQL session analytics benchmark");
  console.log(`Node ${process.version} on ${process.platform} ${process.arch}`);
  console.log(`CPU: ${os.cpus()[0]?.model ?? "unknown"} (${os.cpus().length} logical cores)`);
  console.log("Caveats: local machine only; timings vary with runtime, CPU, memory pressure, data shape, and selectivity.");
  console.log("Tip: run with `ROWS=100000 npm run benchmark:session-analytics` or add `-- --json` for JSON output.\n");

  console.log("Phase         Rows       Operation                                      Result Count  Scan Type  Time (ms)");
  console.log("--------------------------------------------------------------------------------------------------------");
  for (const result of results) {
    console.log(
      `${result.phase.padEnd(13)} ${String(result.rows).padStart(9)}  ${result.operation.padEnd(45)} ${String(result.resultCount).padStart(12)}  ${result.scanType.padEnd(9)} ${formatMs(result.ms).padStart(9)}`,
    );
  }
}

const results = runSessionAnalyticsBenchmark(rows);

if (jsonOutput) {
  console.log(JSON.stringify({
    env: {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      cpu: os.cpus()[0]?.model,
    },
    rows,
    results,
  }, null, 2));
} else {
  printHuman(results);
}

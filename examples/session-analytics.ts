import { column, table } from "@colql/colql";

const SESSION_COUNT = 15_000;
const NOW = 1_725_000_000;
const STALE_ACTIVE_CUTOFF = NOW - 86_400;
const OLD_INACTIVE_CUTOFF = NOW - 14 * 86_400;

const sessions = table({
  id: column.uint32(),
  userId: column.uint32(),
  segment: column.dictionary(["free", "pro", "enterprise"] as const),
  status: column.dictionary(["active", "expired", "inactive"] as const),
  startedAt: column.uint32(),
  durationMs: column.uint32(),
  country: column.dictionary(["US", "TR", "DE", "GB"] as const),
  device: column.dictionary(["desktop", "mobile", "tablet"] as const),
});

type Session = (typeof sessions)["toArray"] extends () => Array<infer Row>
  ? Row
  : never;

function generateSessions(count: number): Session[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    userId: 10_000 + (id % 3_000),
    segment:
      id % 19 === 0 ? "enterprise" : id % 4 === 0 ? "pro" : "free",
    status:
      id % 29 === 0 ? "inactive" : id % 7 === 0 ? "expired" : "active",
    startedAt: NOW - ((id * 137) % (30 * 86_400)),
    durationMs: 30_000 + ((id * 9_973) % 9_000_000),
    country: id % 11 === 0 ? "TR" : id % 7 === 0 ? "DE" : id % 5 === 0 ? "GB" : "US",
    device: id % 6 === 0 ? "tablet" : id % 2 === 0 ? "mobile" : "desktop",
  }));
}

sessions
  .insertMany(generateSessions(SESSION_COUNT))
  .createIndex("status")
  .createIndex("segment")
  .createIndex("country")
  .createSortedIndex("startedAt")
  .createSortedIndex("durationMs");

function printQuery<T>(label: string, query: { explain(): unknown; toArray(): T[] }): T[] {
  console.log(`\n${label}`);
  console.log("explain", query.explain());
  const rows = query.toArray();
  console.log("rows", rows.length);
  console.log("sample", rows.slice(0, 3));
  return rows;
}

console.log("session analytics table", {
  rows: sessions.rowCount,
  equalityIndexes: sessions.indexes(),
  sortedIndexes: sessions.sortedIndexes(),
});

const activeEnterprise = sessions
  .where({ status: "active", segment: "enterprise" })
  .select(["id", "userId", "country", "device", "durationMs"])
  .limit(10);
printQuery("GET /sessions?status=active&segment=enterprise&limit=10", activeEnterprise);

const recentWindow = sessions
  .where({ startedAt: { gte: NOW - 6 * 60 * 60, lte: NOW } })
  .select(["id", "userId", "startedAt", "status"])
  .limit(10);
printQuery("GET /sessions?from=last_6h&to=now&limit=10", recentWindow);

const countryDevice = sessions
  .where({ country: "TR", device: "mobile", status: "active" })
  .select(["id", "userId", "segment", "startedAt"])
  .limit(10);
printQuery("GET /sessions?country=TR&device=mobile&status=active", countryDevice);

const slowActiveSessions = sessions
  .where("status", "=", "active")
  .select(["id", "userId", "durationMs", "country", "device"]);
console.log("\nGET /sessions/slow?status=active&limit=5");
console.log("explain", slowActiveSessions.explain());
console.log("top", slowActiveSessions.top(5, "durationMs"));

const activePro = sessions.where({ status: "active", segment: "pro" });
console.log("\nGET /sessions/summary?status=active&segment=pro");
console.log("explain", activePro.explain());
console.log({
  count: activePro.count(),
  averageDurationMs: Math.round(activePro.avg("durationMs") ?? 0),
  totalDurationMs: activePro.sum("durationMs"),
});

console.log("\nPATCH /sessions/expire");
const expiredBefore = sessions.where("status", "=", "expired").count();
const expirePlan = sessions
  .where({ status: "active", startedAt: { lt: STALE_ACTIVE_CUTOFF } })
  .explain();
console.log("explain", expirePlan);
const expireResult = sessions.updateMany(
  { status: "active", startedAt: { lt: STALE_ACTIVE_CUTOFF } },
  { status: "expired" },
);
console.log("result", {
  ...expireResult,
  expiredBefore,
  expiredAfter: expiredBefore + expireResult.affectedRows,
});

const expiredAfterMutation = sessions
  .where("status", "=", "expired")
  .select(["id", "userId", "startedAt", "durationMs"])
  .limit(10);
printQuery("GET /sessions?status=expired after mutation", expiredAfterMutation);

console.log("\nDELETE /sessions?status=inactive&startedAt<old");
const rowsBeforeDelete = sessions.rowCount;
const deletePlan = sessions
  .where({ status: "inactive", startedAt: { lt: OLD_INACTIVE_CUTOFF } })
  .explain();
console.log("explain", deletePlan);
const deleteResult = sessions.deleteMany({
  status: "inactive",
  startedAt: { lt: OLD_INACTIVE_CUTOFF },
});
console.log("result", {
  ...deleteResult,
  rowsBefore: rowsBeforeDelete,
  rowsAfter: sessions.rowCount,
});

import { column, fromRows, table } from "../../src/index";
import type { RowForSchema } from "../../src/index";

export const SMALL_ROWS = 1_000;
export const MEDIUM_ROWS = 10_000;
export const LARGE_ROWS = 100_000;

const NOW = 1_725_000_000;

export const sessionSchema = {
  id: column.uint32(),
  tenantId: column.uint16(),
  userId: column.uint32(),
  status: column.dictionary(["active", "trial", "paused", "churned"] as const),
  plan: column.dictionary(["free", "pro", "team", "enterprise"] as const),
  region: column.dictionary(["na", "eu", "apac", "latam"] as const),
  createdAt: column.uint32(),
  score: column.float64(),
  active: column.boolean(),
  durationMs: column.uint32(),
  revenueCents: column.uint32(),
};

export type SessionRow = RowForSchema<typeof sessionSchema>;

export type SessionFixture = {
  readonly rows: readonly SessionRow[];
  readonly indexed: ReturnType<typeof createIndexedSessionTable>;
  readonly scanOnly: ReturnType<typeof createSessionTable>;
  readonly serialized: ArrayBuffer;
};

function mix32(value: number): number {
  let state = value + 0x9e3779b9;
  state = Math.imul(state ^ (state >>> 16), 0x85ebca6b);
  state = Math.imul(state ^ (state >>> 13), 0xc2b2ae35);
  return (state ^ (state >>> 16)) >>> 0;
}

function pick<const Values extends readonly string[]>(values: Values, seed: number): Values[number] {
  return values[seed % values.length];
}

export function createSessionRows(rowCount: number): SessionRow[] {
  const rows: SessionRow[] = new Array(rowCount);

  for (let id = 0; id < rowCount; id += 1) {
    const seed = mix32(id);
    const tenantId = (seed % 64) + 1;
    const planSeed = Math.floor(seed / 17);
    const status =
      id % 41 === 0 ? "churned" : id % 13 === 0 ? "paused" : id % 5 === 0 ? "trial" : "active";

    rows[id] = {
      id,
      tenantId,
      userId: 10_000 + (mix32(id + 11) % Math.max(1_000, Math.floor(rowCount / 2))),
      status,
      plan: pick(["free", "pro", "team", "enterprise"] as const, planSeed),
      region: pick(["na", "eu", "apac", "latam"] as const, Math.floor(seed / 97)),
      createdAt: NOW - (mix32(id + 23) % (90 * 86_400)),
      score: (mix32(id + 37) % 10_000) / 100,
      active: status === "active" || status === "trial",
      durationMs: 20_000 + (mix32(id + 53) % 9_000_000),
      revenueCents: status === "churned" ? 0 : 500 + (mix32(id + 71) % 250_000),
    };
  }

  return rows;
}

export function createSessionTable(rows: readonly SessionRow[]) {
  return fromRows(sessionSchema, rows);
}

export function createIndexedSessionTable(rows: readonly SessionRow[]) {
  return createSessionTable(rows)
    .createUniqueIndex("id")
    .createIndex("tenantId")
    .createIndex("status")
    .createIndex("plan")
    .createIndex("region")
    .createSortedIndex("createdAt")
    .createSortedIndex("score");
}

export function createSessionFixture(rowCount: number): SessionFixture {
  const rows = createSessionRows(rowCount);
  const indexed = createIndexedSessionTable(rows);

  return {
    rows,
    indexed,
    scanOnly: createSessionTable(rows),
    serialized: indexed.serialize(),
  };
}

export function deserializeSessionTable(input: ArrayBuffer | Uint8Array) {
  return table.deserialize(input);
}

export function recreateSessionIndexes(input: ArrayBuffer | Uint8Array) {
  return deserializeSessionTable(input)
    .createUniqueIndex("id")
    .createIndex("tenantId")
    .createIndex("status")
    .createIndex("plan")
    .createIndex("region")
    .createSortedIndex("createdAt")
    .createSortedIndex("score");
}

export const smallSessions = createSessionFixture(SMALL_ROWS);
export const mediumSessions = createSessionFixture(MEDIUM_ROWS);
export const largeSessions = createSessionFixture(LARGE_ROWS);

export const dashboardTenantId = 17;
export const dashboardStart = NOW - 14 * 86_400;
export const dashboardEnd = NOW - 2 * 86_400;

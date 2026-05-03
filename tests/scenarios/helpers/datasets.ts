import { column, type RowForSchema } from "../../../src";

export const userAnalyticsSchema = {
  id: column.uint32(),
  status: column.dictionary(["active", "inactive", "suspended"] as const),
  segment: column.dictionary(["free", "pro", "enterprise"] as const),
  age: column.uint8(),
  score: column.uint32(),
  lastSeen: column.uint32(),
};

export const eventLogSchema = {
  id: column.uint32(),
  timestamp: column.uint32(),
  severity: column.dictionary(["debug", "info", "warn", "error"] as const),
  service: column.dictionary(["api", "worker", "billing", "search"] as const),
  durationMs: column.uint32(),
};

export const productCatalogSchema = {
  id: column.uint32(),
  category: column.dictionary(["books", "games", "tools", "apparel"] as const),
  status: column.dictionary(["active", "inactive", "discontinued"] as const),
  price: column.uint32(),
  rating: column.uint8(),
  stock: column.uint16(),
};

export const sessionAnalyticsSchema = {
  id: column.uint32(),
  userId: column.uint32(),
  segment: column.dictionary(["free", "pro", "enterprise"] as const),
  status: column.dictionary(["active", "expired", "revoked"] as const),
  startedAt: column.uint32(),
  durationMs: column.uint32(),
  country: column.dictionary(["US", "TR", "DE", "GB"] as const),
};

export type UserAnalyticsRow = RowForSchema<typeof userAnalyticsSchema>;
export type EventLogRow = RowForSchema<typeof eventLogSchema>;
export type ProductCatalogRow = RowForSchema<typeof productCatalogSchema>;
export type SessionAnalyticsRow = RowForSchema<typeof sessionAnalyticsSchema>;

export function makeUsers(count = 5_000): UserAnalyticsRow[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    status: id % 11 === 0 ? "suspended" : id % 5 === 0 ? "inactive" : "active",
    segment: id % 13 === 0 ? "enterprise" : id % 3 === 0 ? "pro" : "free",
    age: 18 + ((id * 7) % 55),
    score: (id * 37) % 10_000,
    lastSeen: 1_700_000_000 + ((id * 97) % 120_000),
  }));
}

export function makeEventLogs(count = 5_000): EventLogRow[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    timestamp: 1_710_000_000 + id * 30,
    severity: id % 17 === 0 ? "error" : id % 7 === 0 ? "warn" : id % 3 === 0 ? "debug" : "info",
    service: id % 5 === 0 ? "billing" : id % 4 === 0 ? "search" : id % 2 === 0 ? "worker" : "api",
    durationMs: 20 + ((id * 23) % 2_000),
  }));
}

export function makeProducts(count = 4_000): ProductCatalogRow[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    category: id % 7 === 0 ? "tools" : id % 5 === 0 ? "games" : id % 3 === 0 ? "apparel" : "books",
    status: id % 19 === 0 ? "discontinued" : id % 6 === 0 ? "inactive" : "active",
    price: 500 + ((id * 41) % 50_000),
    rating: 1 + ((id * 3) % 5),
    stock: (id * 11) % 500,
  }));
}

export function makeSessions(count = 5_000): SessionAnalyticsRow[] {
  return Array.from({ length: count }, (_unused, id) => ({
    id,
    userId: 1_000 + (id % 1_250),
    segment: id % 17 === 0 ? "enterprise" : id % 4 === 0 ? "pro" : "free",
    status: id % 23 === 0 ? "revoked" : id % 6 === 0 ? "expired" : "active",
    startedAt: 1_720_000_000 + ((id * 45) % 200_000),
    durationMs: 60_000 + ((id * 1_337) % 7_200_000),
    country: id % 11 === 0 ? "TR" : id % 7 === 0 ? "DE" : id % 5 === 0 ? "GB" : "US",
  }));
}

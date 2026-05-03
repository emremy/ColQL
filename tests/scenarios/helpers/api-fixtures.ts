import { fromRows } from "../../../src";
import {
  eventLogSchema,
  makeEventLogs,
  makeProducts,
  makeSessions,
  makeUsers,
  productCatalogSchema,
  sessionAnalyticsSchema,
  userAnalyticsSchema,
} from "./datasets";

export function buildUserAnalyticsFixture(count = 5_000) {
  const rows = makeUsers(count);
  const users = fromRows(userAnalyticsSchema, rows)
    .createUniqueIndex("id")
    .createIndex("status")
    .createIndex("segment")
    .createSortedIndex("age")
    .createSortedIndex("lastSeen");

  return { users, oracle: rows.map((row) => ({ ...row })) };
}

export function buildEventLogsFixture(count = 5_000) {
  const rows = makeEventLogs(count);
  const events = fromRows(eventLogSchema, rows)
    .createUniqueIndex("id")
    .createIndex("severity")
    .createIndex("service")
    .createSortedIndex("timestamp")
    .createSortedIndex("durationMs");

  return { events, oracle: rows.map((row) => ({ ...row })) };
}

export function buildProductCatalogFixture(count = 4_000) {
  const rows = makeProducts(count);
  const products = fromRows(productCatalogSchema, rows)
    .createUniqueIndex("id")
    .createIndex("category")
    .createIndex("status")
    .createSortedIndex("price")
    .createSortedIndex("rating")
    .createSortedIndex("stock");

  return { products, oracle: rows.map((row) => ({ ...row })) };
}

export function buildSessionAnalyticsFixture(count = 5_000) {
  const rows = makeSessions(count);
  const sessions = fromRows(sessionAnalyticsSchema, rows)
    .createUniqueIndex("id")
    .createIndex("status")
    .createIndex("segment")
    .createIndex("country")
    .createSortedIndex("startedAt")
    .createSortedIndex("durationMs");

  return { sessions, oracle: rows.map((row) => ({ ...row })) };
}

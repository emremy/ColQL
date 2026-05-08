import { bench, describe } from "vitest";
import { dashboardEnd, dashboardStart, dashboardTenantId, mediumSessions } from "./fixtures";

const compoundExplainQuery = mediumSessions.indexed
  .where({
    tenantId: dashboardTenantId,
    status: "active",
    createdAt: { gte: dashboardStart, lt: dashboardEnd },
  })
  .select(["id", "revenueCents"]);

let explainPredicateSink = 0;

describe("query", () => {
  bench("query/indexed/equality/10k", () => {
    mediumSessions.indexed.where("tenantId", "=", dashboardTenantId).count();
  });

  bench("query/indexed/in/10k", () => {
    mediumSessions.indexed.where("status", "in", ["active", "trial"]).count();
  });

  bench("query/indexed/range/10k", () => {
    mediumSessions.indexed.where({ createdAt: { gte: dashboardStart, lt: dashboardEnd } }).count();
  });

  bench("query/indexed/compound/10k", () => {
    mediumSessions.indexed
      .where({
        tenantId: dashboardTenantId,
        status: "active",
        createdAt: { gte: dashboardStart, lt: dashboardEnd },
      })
      .count();
  });

  bench("query/projection/pushdown/10k", () => {
    mediumSessions.indexed
      .where({
        tenantId: dashboardTenantId,
        status: "active",
        createdAt: { gte: dashboardStart, lt: dashboardEnd },
      })
      .select(["id", "userId", "revenueCents"])
      .toArray();
  });

  bench("query/materialize/broad-filter/10k", () => {
    mediumSessions.indexed.where("status", "=", "active").toArray();
  });

  bench("aggregation/sum/filtered-revenue/10k", () => {
    mediumSessions.indexed
      .where({
        tenantId: dashboardTenantId,
        active: true,
        createdAt: { gte: dashboardStart, lt: dashboardEnd },
      })
      .sum("revenueCents");
  });

  bench("query/full-scan/fallback/10k", () => {
    mediumSessions.scanOnly.filter((row) => row.active && row.score >= 80).count();
  });

  bench("query/explain/compound-filter-batch/10k", () => {
    let predicateCount = 0;
    for (let iteration = 0; iteration < 1_000; iteration += 1) {
      predicateCount += compoundExplainQuery.explain().predicates;
    }
    explainPredicateSink = predicateCount;
  });
});

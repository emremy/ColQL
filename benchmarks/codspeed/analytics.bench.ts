import { bench, describe } from "vitest";
import { dashboardEnd, dashboardStart, dashboardTenantId, largeSessions } from "./fixtures";

describe("analytics", () => {
  bench("analytics/session-dashboard/100k", () => {
    const query = largeSessions.indexed.where({
      tenantId: dashboardTenantId,
      active: true,
      createdAt: { gte: dashboardStart, lt: dashboardEnd },
    });

    query.count();
    query.avg("durationMs");
    query.sum("revenueCents");
    query.min("score");
    query.max("score");
  });
});

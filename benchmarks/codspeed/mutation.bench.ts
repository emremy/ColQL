import { bench, describe } from "vitest";
import {
  createIndexedSessionTable,
  dashboardTenantId,
  mediumSessions,
  smallSessions,
} from "./fixtures";

let syncFallbackTenantId = 62;
let batchScore = 25;
let unrelatedDuration = 30_000;

describe("mutation", () => {
  bench("mutation/updateBy/single/10k", () => {
    const id = 5_001;
    const row = mediumSessions.indexed.findBy("id", id);
    const nextScore = row?.score === 12.5 ? 87.5 : 12.5;
    mediumSessions.indexed.updateBy("id", id, { score: nextScore });
  });

  bench("mutation/updateBy/batch/10k", () => {
    batchScore = batchScore === 25 ? 75 : 25;
    for (let offset = 0; offset < 50; offset += 1) {
      mediumSessions.indexed.updateBy("id", 4_000 + offset * 7, {
        score: batchScore + offset / 100,
      });
    }
  });

  bench("mutation/updateWhere/subset/1k", () => {
    const currentTrialCount = smallSessions.indexed.where("status", "=", "trial").count();
    if (currentTrialCount > 0) {
      smallSessions.indexed.where("status", "=", "trial").limit(10).update({ status: "paused" });
      return;
    }

    smallSessions.indexed.where("status", "=", "paused").limit(10).update({ status: "trial" });
  });

  bench("mutation/deleteWhere/setup-inclusive/1k", () => {
    const table = createIndexedSessionTable(smallSessions.rows);
    table.where("status", "=", "paused").limit(10).delete();
  });

  bench("index/sync-fallback-rebuild/after-indexed-column-mutation/10k", () => {
    syncFallbackTenantId = syncFallbackTenantId === 62 ? 63 : 62;
    mediumSessions.indexed.updateBy("id", 7_501, { tenantId: syncFallbackTenantId });
    mediumSessions.indexed.where("tenantId", "=", syncFallbackTenantId).count();
  });

  bench("index/no-rebuild/after-unindexed-column-mutation/10k", () => {
    unrelatedDuration = unrelatedDuration === 30_000 ? 45_000 : 30_000;
    mediumSessions.indexed.updateBy("id", 7_502, { durationMs: unrelatedDuration });
    mediumSessions.indexed.where("tenantId", "=", dashboardTenantId).count();
  });

  bench("index/requery/after-sync-fallback-rebuild/10k", () => {
    mediumSessions.indexed.where("tenantId", "=", syncFallbackTenantId).count();
  });
});

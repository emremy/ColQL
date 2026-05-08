import { bench, describe } from "vitest";
import { deserializeSessionTable, mediumSessions, recreateSessionIndexes } from "./fixtures";

const restoredIndexedSessions = recreateSessionIndexes(mediumSessions.serialized);

describe("serialization", () => {
  bench("serialization/serialize/10k", () => {
    mediumSessions.indexed.serialize();
  });

  bench("serialization/deserialize/no-indexes/10k", () => {
    deserializeSessionTable(mediumSessions.serialized);
  });

  bench("serialization/deserialize/recreate-indexes/10k", () => {
    recreateSessionIndexes(mediumSessions.serialized);
  });

  bench("serialization/query-after-deserialize/no-indexes/10k", () => {
    deserializeSessionTable(mediumSessions.serialized).where("tenantId", "=", 17).count();
  });

  bench("serialization/query/prebuilt-deserialized-indexed/10k", () => {
    restoredIndexedSessions.where("tenantId", "=", 17).count();
  });
});

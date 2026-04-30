import { buildApp } from "../src/app";
import { injectJson, printLatency, time } from "./helpers";

const SEED_SIZE = Number(process.env.COLQL_EXAMPLE_STRESS_SIZE ?? 1_000_000);
const CONCURRENCY = Number(process.env.COLQL_EXAMPLE_STRESS_CONCURRENCY ?? 50);

type CountResponse = { readonly count: number };

async function main(): Promise<void> {
  console.log(`Starting stress validation with ${SEED_SIZE.toLocaleString()} users and ${CONCURRENCY} concurrent requests.`);
  const app = buildApp({ seedSize: SEED_SIZE });

  try {
    const baseline = await injectJson<CountResponse>(app, { method: "GET", url: "/users/count?country=TR&minAge=25" });
    const started = performance.now();
    const responses = await Promise.all(
      Array.from({ length: CONCURRENCY }, () =>
        time(() => injectJson<CountResponse>(app, { method: "GET", url: "/users/count?country=TR&minAge=25" })),
      ),
    );
    const totalDuration = performance.now() - started;
    const counts = new Set(responses.map((response) => response.value.count));

    if (counts.size !== 1 || !counts.has(baseline.count)) {
      throw new Error(`Concurrent responses returned inconsistent counts: ${[...counts].join(", ")}.`);
    }

    printLatency("concurrent indexed structured query", responses.map((response) => response.duration));
    console.log(`total=${totalDuration.toFixed(2)}ms average=${(totalDuration / CONCURRENCY).toFixed(2)}ms`);
    console.log("Stress validation completed.");
  } finally {
    await app.close();
  }
}

await main();

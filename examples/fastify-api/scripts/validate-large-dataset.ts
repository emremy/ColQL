import { buildApp } from "../src/app";
import { injectJson, printLatency, printMemory, time } from "./helpers";

const SEED_SIZE = Number(process.env.COLQL_EXAMPLE_LARGE_SIZE ?? 1_000_000);

type CountResponse = { readonly count: number };
type UsersResponse = {
  readonly users: readonly {
    readonly id: number;
    readonly name: string;
    readonly age: number;
    readonly country: string;
    readonly active: boolean;
  }[];
};
type MutationResponse = { readonly affectedRows: number };
type QueryLogResponse = { readonly entries: readonly unknown[] };
type LatencyCase = {
  readonly label: string;
  readonly url: string;
  readonly values: number[];
};

async function main(): Promise<void> {
  console.log(
    `Starting Fastify ColQL app with ${SEED_SIZE.toLocaleString()} generated users.`,
  );
  printMemory("before cold start");
  const coldStart = await time(async () => buildApp({ seedSize: SEED_SIZE }));
  const app = coldStart.value;
  console.log(`cold start: ${coldStart.duration.toFixed(2)}ms`);
  printMemory("after cold start");

  try {
    const initial = await injectJson<CountResponse>(app, {
      method: "GET",
      url: "/users/count",
    });
    if (initial.count !== SEED_SIZE) {
      throw new Error(
        `Expected ${SEED_SIZE} seeded users, received ${initial.count}.`,
      );
    }

    const initialCountry = await injectJson<CountResponse>(app, {
      method: "GET",
      url: "/users/count?country=TR",
    });
    const initialInactive = await injectJson<CountResponse>(app, {
      method: "GET",
      url: "/users/count?active=false",
    });
    const inactiveInUpdatedCountry = await injectJson<CountResponse>(app, {
      method: "GET",
      url: "/users/count?country=TR&active=false",
    });

    const updated = await injectJson<MutationResponse>(app, {
      method: "PATCH",
      url: "/users/by-country/TR",
      payload: { active: true, score: 99.9 },
    });
    if (updated.affectedRows !== initialCountry.count) {
      throw new Error(
        `Unexpected updateMany affectedRows: ${updated.affectedRows}.`,
      );
    }

    const deleted = await injectJson<MutationResponse>(app, {
      method: "DELETE",
      url: "/users/inactive",
    });
    const expectedDeleted =
      initialInactive.count - inactiveInUpdatedCountry.count;
    if (deleted.affectedRows !== expectedDeleted) {
      throw new Error(
        `Unexpected deleteMany affectedRows: ${deleted.affectedRows}; expected ${expectedDeleted}.`,
      );
    }

    const insertMany = await injectJson<{
      readonly inserted: number;
      readonly rowCount: number;
    }>(app, {
      method: "POST",
      url: "/users/bulk",
      payload: {
        users: [
          {
            id: SEED_SIZE + 1,
            age: 45,
            country: "TR",
            active: true,
            name: "Ada",
            score: 98.1,
          },
          {
            id: SEED_SIZE + 2,
            age: 52,
            country: "US",
            active: true,
            name: "Grace",
            score: 97.2,
          },
          {
            id: SEED_SIZE + 3,
            age: 28,
            country: "JP",
            active: false,
            name: "Ken",
            score: 76.4,
          },
        ],
      },
    });
    if (insertMany.inserted !== 3) {
      throw new Error(
        `Unexpected insertMany inserted count: ${insertMany.inserted}.`,
      );
    }

    const finalCount = await injectJson<CountResponse>(app, {
      method: "GET",
      url: "/users/count",
    });
    const expectedFinal = SEED_SIZE - expectedDeleted + 3;
    if (finalCount.count !== expectedFinal) {
      throw new Error(
        `Expected final count ${expectedFinal}, received ${finalCount.count}.`,
      );
    }

    const sample = await injectJson<UsersResponse>(app, {
      method: "GET",
      url: "/users?country=TR&minAge=44&limit=5",
    });
    if (
      sample.users.length === 0 ||
      !sample.users.every((user) => user.country === "TR" && user.age > 44)
    ) {
      throw new Error("Filtered sampled users are not correct after mutation.");
    }

    const inserted = await injectJson<UsersResponse>(app, {
      method: "GET",
      url: `/users?id=${SEED_SIZE + 2}`,
    });
    if (!inserted.users.some((user) => user.id === SEED_SIZE + 2)) {
      throw new Error("Inserted user was not queryable after insertMany.");
    }

    const inactiveAfterDelete = await injectJson<CountResponse>(app, {
      method: "GET",
      url: "/users/count?active=false",
    });
    if (inactiveAfterDelete.count !== 1) {
      throw new Error(
        `Expected only the newly inserted inactive user after deleteMany, received ${inactiveAfterDelete.count}.`,
      );
    }

    const latencyCases: LatencyCase[] = [
      {
        label: "indexed structured query",
        url: "/users/count?country=TR",
        values: [],
      },
      {
        label: "range query",
        url: "/users/count?minAge=60&maxAge=70",
        values: [],
      },
      {
        label: "broad scan query",
        url: "/users/count?active=true",
        values: [],
      },
      {
        label: "callback filter query",
        url: "/users/count?search=da",
        values: [],
      },
    ];

    for (let index = 0; index < 25; index += 1) {
      for (const latencyCase of latencyCases) {
        latencyCase.values.push(
          (
            await time(() =>
              injectJson<CountResponse>(app, {
                method: "GET",
                url: latencyCase.url,
              }),
            )
          ).duration,
        );
      }
    }

    for (const latencyCase of latencyCases) {
      printLatency(latencyCase.label, latencyCase.values);
    }

    const queryLog = await injectJson<QueryLogResponse>(app, {
      method: "GET",
      url: "/debug/query-log",
    });
    if (queryLog.entries.length === 0) {
      throw new Error(
        "Expected onQuery entries during large dataset validation.",
      );
    }

    printMemory("after mutations and repeated queries");
    console.log("Large dataset validation completed.");
  } finally {
    await app.close();
  }
}

await main();

import { buildApp } from "../src/app";
import { injectJson, printMemory } from "./helpers";

const SEED_SIZE = Number(process.env.COLQL_EXAMPLE_MEMORY_SIZE ?? 1_000_000);

async function main(): Promise<void> {
  printMemory("before seed");
  const app = buildApp({ seedSize: SEED_SIZE });
  printMemory("after seed");

  try {
    await injectJson(app, {
      method: "PATCH",
      url: "/users/by-country/TR",
      payload: { active: true },
    });
    await injectJson(app, { method: "DELETE", url: "/users/inactive" });
    await injectJson(app, {
      method: "POST",
      url: "/users/bulk",
      payload: {
        users: [
          { id: SEED_SIZE + 1, age: 45, country: "TR", active: true, name: "Ada", score: 98.1 },
          { id: SEED_SIZE + 2, age: 52, country: "US", active: true, name: "Grace", score: 97.2 },
        ],
      },
    });
    printMemory("after update/delete/insert");

    for (let index = 0; index < 25; index += 1) {
      await injectJson(app, { method: "GET", url: "/users/count?country=TR&minAge=25" });
      await injectJson(app, { method: "GET", url: "/users/count?search=da" });
    }
    printMemory("after repeated queries");
  } finally {
    await app.close();
  }
}

await main();

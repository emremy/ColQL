import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const outputDir = join(tmpdir(), "colql-background-benchmark-internals");
const tsupBin = resolve(repoRoot, "node_modules/.bin/tsup");

const entries = [
  "src/table.ts",
  "src/storage/numeric-column.ts",
  "src/storage/dictionary-column.ts",
  "src/indexing/background/equality-rebuild.ts",
  "src/indexing/background/sorted-rebuild.ts",
  "src/indexing/background/worker-pool.ts",
];

let loadedModules;

export async function loadBackgroundBenchmarkInternals() {
  if (loadedModules !== undefined) {
    return loadedModules;
  }

  execFileSync(tsupBin, [
    ...entries,
    "--format",
    "esm",
    "--clean",
    "--silent",
    "--out-dir",
    outputDir,
  ], {
    cwd: repoRoot,
    stdio: ["ignore", "ignore", "inherit"],
  });

  const baseUrl = pathToFileURL(`${outputDir}/`);
  const [
    tableModule,
    numericModule,
    dictionaryModule,
    equalityModule,
    sortedModule,
    workerPoolModule,
  ] = await Promise.all([
    import(new URL("table.mjs", baseUrl)),
    import(new URL("storage/numeric-column.mjs", baseUrl)),
    import(new URL("storage/dictionary-column.mjs", baseUrl)),
    import(new URL("indexing/background/equality-rebuild.mjs", baseUrl)),
    import(new URL("indexing/background/sorted-rebuild.mjs", baseUrl)),
    import(new URL("indexing/background/worker-pool.mjs", baseUrl)),
  ]);

  loadedModules = {
    ...tableModule,
    ...numericModule,
    ...dictionaryModule,
    ...equalityModule,
    ...sortedModule,
    ...workerPoolModule,
  };
  return loadedModules;
}

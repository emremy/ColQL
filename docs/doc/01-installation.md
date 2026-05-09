# Installation

Install ColQL from npm:

```sh
npm install @colql/colql
```

## TypeScript Usage

```ts
import { table, column } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
});

users.insert({ id: 1, age: 25, status: "active" });
```

The package publishes CommonJS, ESM, and TypeScript declaration output through `dist`:

- `main`: `dist/index.js`
- `module`: `dist/index.mjs`
- `types`: `dist/index.d.ts`

Most bundlers and TypeScript projects can import from `@colql/colql` directly.

v0.6.0 also publishes internal worker artifacts under `dist/indexing/background` for Node `worker_threads` smoke-tested background-indexing infrastructure. These files are package internals; application code should continue importing from `@colql/colql`.

## Minimal Runnable Example

```ts
import { table, column } from "@colql/colql";

const events = table({
  id: column.uint32(),
  value: column.float64(),
  type: column.dictionary(["view", "click", "purchase"] as const),
});

events.insertMany([
  { id: 1, value: 10, type: "view" },
  { id: 2, value: 35, type: "click" },
  { id: 3, value: 120, type: "purchase" },
]);

const purchaseTotal = events.where("type", "=", "purchase").sum("value");
console.log(purchaseTotal);
```

## Development Scripts

When contributing locally:

```sh
npm install
npm run check
npm test
npm run test:types
npm run build
npm run test:worker-runtime
```

Benchmarks are available after building:

```sh
npm run benchmark:memory
npm run benchmark:query
npm run benchmark:indexed
npm run benchmark:range
npm run benchmark:optimizer
npm run benchmark:serialization
npm run benchmark:delete
npm run benchmark:background-indexing -- --json
npm run benchmark:worker-runtime -- --json
```

See [Performance and Benchmarks](./13-performance-and-benchmarks.md) for what each benchmark measures.

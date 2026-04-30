# ColQL Fastify API Example

Small process-local Fastify backend showing ColQL v0.2.0 in an HTTP API.

The app stores users in memory. It is useful as an integration example, not as a persistent database-backed service. Restarting the process resets the data.

## Run

```sh
npm install
npm run dev
```

The server listens on `http://localhost:3000` by default. Set `PORT` to use another port.

By default the app starts with a tiny deterministic seed. To test a larger process-local dataset, set `COLQL_EXAMPLE_SEED_SIZE`:

```sh
COLQL_EXAMPLE_SEED_SIZE=1000000 npm run dev
```

There is also a shortcut:

```sh
npm run dev:1m
```

The generated seed uses deterministic dictionary values for `country` and `name`, so it still exercises ColQL dictionary columns, equality indexes, and the sorted age index.

## Test

```sh
npm test
```

The tests start the Fastify app in memory and use `app.inject()` to send real HTTP requests.

Large-dataset validation is separate from the normal test suite so everyday tests stay fast:

```sh
npm run test:large
```

`test:large` starts the app with 1M generated users, performs `updateMany`, `deleteMany`, and `insertMany` through HTTP requests, verifies filtered query correctness after lazy index rebuilds, and prints latency summaries for indexed, range, broad scan, and callback-filter requests.

Run the basic concurrent stress check with:

```sh
npm run stress
```

The stress script sends 50 concurrent requests to an index-friendly structured query and checks that all responses are successful and consistent.

Run the memory sanity check with:

```sh
npm run memory:example
```

The memory script reports `heapUsed`, `rss`, and `arrayBuffers` after 1M seed, after mutations, and after repeated queries. These scripts do not enforce strict latency or memory thresholds; they are smoke validations for local machines.

`filter(fn)` is a full-scan escape hatch and is not index-aware. On 1M rows, callback-filter requests are expected to be slower than structured indexed queries.

## Endpoints

- `GET /health`
- `POST /users`
- `POST /users/bulk`
- `GET /users`
- `GET /users/count`
- `PATCH /users/by-country/:country`
- `DELETE /users/inactive`
- `GET /debug/query-log`
- `GET /debug/indexes`
- `GET /debug/memory`

## ColQL Features Demonstrated

- `insert(row)`
- `insertMany(rows)`
- object-based `where({ ... })`
- tuple `where(column, operator, value)`
- `filter(fn)` for callback search
- `select()` projection
- `count()`
- `updateMany(predicate, partialRow)`
- `deleteMany(predicate)`
- equality indexes with `createIndex`
- sorted/range index usage with `createSortedIndex`
- query diagnostics with `onQuery`
- public memory-related counters such as `rowCount`, `capacity`, `materializedRowCount`, and `scannedRowCount`

Example query:

```sh
curl "http://localhost:3000/users?country=TR&minAge=25&search=mi"
```

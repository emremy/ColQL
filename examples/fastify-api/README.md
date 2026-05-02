# ColQL Fastify API Example

A minimal Fastify backend demonstrating how to use ColQL as a process-local data layer in a real HTTP API.

The app stores users in memory. It is an integration example, not a persistent database-backed service. Restarting the process resets the data.

## What This Demonstrates

- HTTP query params mapped to object-based `where({ ... })`
- Range queries with `minAge` and `maxAge` backed by a sorted age index
- `filter(fn)` for callback search after structured predicates
- `insert(row)` and `insertMany(rows)` through HTTP endpoints
- `updateMany(predicate, partialRow)` through `PATCH /users/by-country/:country`
- `deleteMany(predicate)` through `DELETE /users/inactive`
- Equality indexes with `createIndex("country")` and `createIndex("name")`
- Sorted indexes with `createSortedIndex("age")`
- Query diagnostics through `onQuery`
- Public counters such as `rowCount`, `capacity`, `materializedRowCount`, and `scannedRowCount`

`filter(fn)` is a full-scan escape hatch and is not index-aware. In this example, structured filters run first, then `search` applies a callback filter to remaining rows.

## Run

Install dependencies:

```sh
npm install
```

Tiny deterministic seed mode:

```sh
npm run dev
```

The server listens on `http://localhost:3000` by default. Set `PORT` to use another port.

1M generated dataset mode:

```sh
npm run dev:1m
```

Custom generated seed size:

```sh
COLQL_EXAMPLE_SEED_SIZE=100000 npm run dev
```

The generated seed uses deterministic dictionary values for `country` and `name`, so it exercises ColQL dictionary columns, equality indexes, and the sorted age index.

## Try It

Query with indexed structured filters:

```sh
curl "http://localhost:3000/users?country=TR&active=true"
```

Query a numeric range:

```sh
curl "http://localhost:3000/users?minAge=30&maxAge=40"
```

Run a structured filter followed by callback search:

```sh
curl "http://localhost:3000/users?country=TR&search=mi"
```

Update all users in one country:

```sh
curl -X PATCH "http://localhost:3000/users/by-country/TR" \
  -H "content-type: application/json" \
  -d '{"active":true,"score":99.9}'
```

Delete inactive users:

```sh
curl -X DELETE "http://localhost:3000/users/inactive"
```

Inspect query diagnostics:

```sh
curl "http://localhost:3000/debug/query-log"
```

Other useful debug endpoints:

```sh
curl "http://localhost:3000/debug/indexes"
curl "http://localhost:3000/debug/memory"
```

## Test And Validate

Run normal tests:

```sh
npm test
```

The tests start the Fastify app in memory and use `app.inject()` to send real HTTP requests.

Large-dataset validation is separate from the normal test suite so everyday tests stay fast:

```sh
npm run test:large
```

`test:large` starts the app with 1M generated users, performs `updateMany`, `deleteMany`, and `insertMany` through HTTP requests, verifies filtered query correctness after lazy index rebuilds, and prints latency summaries for indexed, range, broad scan, and callback-filter requests.

Run the basic concurrent stress check:

```sh
npm run stress
```

The stress script sends concurrent requests to an index-friendly structured query and checks that all responses are successful and consistent.

Run the memory sanity check:

```sh
npm run memory:example
```

The memory script reports `heapUsed`, `rss`, and `arrayBuffers` after 1M seed, after mutations, and after repeated queries. These scripts do not enforce strict latency or memory thresholds; they are smoke validations for local machines.

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

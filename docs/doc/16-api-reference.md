# API Reference

This is a factual summary of the public API. See the topic docs for deeper behavior.

## Imports

```ts
import { table, column, ColQLError } from "@colql/colql";
import type { MutationResult, Operator, RowForSchema, Schema } from "@colql/colql";
```

## Table Creation

```ts
const users = table(schema);
const restored = table.deserialize(buffer);
```

`table(schema)` returns a `Table` instance.
`table.deserialize(input)` accepts an `ArrayBuffer` or `Uint8Array` and returns a table.

## Columns

```ts
column.int16();
column.int32();
column.uint8();
column.uint16();
column.uint32();
column.float32();
column.float64();
column.boolean();
column.dictionary(["active", "passive"] as const);

column.smallint();
column.integer();
column.real();
column.doublePrecision();
```

## Insert and Read

```ts
users.insert(row);       // this
users.insertMany(rows);  // this
users.get(rowIndex);     // row
users.getSchema();       // schema
users.rowCount;          // number
users.capacity;          // number
```

Low-level typed reads are also exposed:

```ts
users.getValue(rowIndex, column);
users.getComparableValue(rowIndex, column);
users.getNumericValue(rowIndex, numericColumn);
```

These are mainly useful for advanced integrations and diagnostics. Most application code should use query and row APIs.

## Query Construction

```ts
users.where(column, operator, value);
users.whereIn(column, values);
users.whereNotIn(column, values);
users.select(columns);
users.limit(n);
users.offset(n);
users.query();
```

`query()` creates an unfiltered query over the table. The table-level helpers above are the usual entrypoints for application code.

Operators:

```ts
type Operator = "=" | "!=" | ">" | ">=" | "<" | "<=" | "in" | "not in";
```

## Query Execution

```ts
users.toArray();
users.first();
users.count();
users.size();
users.isEmpty();
users.forEach(callback);
users.stream();

for (const row of users) {
  // all rows
}

for (const row of users.where("status", "=", "active")) {
  // matching rows
}
```

Query objects also support `toArray`, `first`, `count`, `size`, `isEmpty`, `forEach`, `stream`, and iteration.

## Query Objects

Queries returned by `where`, `select`, `limit`, `offset`, and `query()` support:

```ts
query.where(column, operator, value);
query.whereIn(column, values);
query.whereNotIn(column, values);
query.select(columns);
query.limit(n);
query.offset(n);

query.first();
query.toArray();
query.forEach(callback);
query.count();
query.size();
query.isEmpty();
query.stream();

query.sum(numericColumn);
query.avg(numericColumn);
query.min(numericColumn);
query.max(numericColumn);
query.top(n, numericColumn);
query.bottom(n, numericColumn);

query.update(partialRow);
query.delete();

for (const row of query) {
  // matching rows
}
```

`query.update()` and `query.delete()` respect filters, `offset`, and `limit`. `select()` affects query output but does not restrict update payloads.

## Aggregations

```ts
users.sum(numericColumn);
users.avg(numericColumn);
users.min(numericColumn);
users.max(numericColumn);
users.top(n, numericColumn);
users.bottom(n, numericColumn);
```

Query objects support the same aggregation methods.

## Mutations

```ts
users.delete(rowIndex); // this

users.update(rowIndex, partialRow); // MutationResult
users.updateWhere(column, operator, value, partialRow); // MutationResult
users.deleteWhere(column, operator, value); // MutationResult

users.where(...).update(partialRow); // MutationResult
users.where(...).delete(); // MutationResult
```

```ts
type MutationResult = {
  affectedRows: number;
};
```

## Equality Indexes

```ts
users.createIndex(column);   // this
users.dropIndex(column);     // this
users.hasIndex(column);      // boolean
users.indexes();             // string[]
users.indexStats();          // EqualityIndexStats[]
users.rebuildIndex(column);  // this
users.rebuildIndexes();      // this
```

## Sorted Indexes

```ts
users.createSortedIndex(numericColumn);   // this
users.dropSortedIndex(column);            // this
users.hasSortedIndex(column);             // boolean
users.sortedIndexes();                    // string[]
users.sortedIndexStats();                 // SortedIndexStats[]
users.rebuildSortedIndex(numericColumn);  // this
users.rebuildIndexes();                   // this
```

## Serialization

```ts
const buffer = users.serialize();      // ArrayBuffer
const restored = table.deserialize(buffer);
```

`deserialize` accepts `ArrayBuffer` or `Uint8Array`.

## Diagnostics

```ts
users.materializedRowCount;
users.resetMaterializationCounter();
users.scannedRowCount;
users.resetScanCounter();
users.getIndexedCandidatePlan(filters);
users.getIndexDebugPlan(filters);
```

Queries expose `__debugPlan()` for planner diagnostics. It is useful in tests and debugging, but application code should not depend on it as a stable planning contract.

## Errors

```ts
try {
  users.get(999);
} catch (error) {
  if (error instanceof ColQLError) {
    console.log(error.code);
    console.log(error.message);
    console.log(error.details);
  }
}
```

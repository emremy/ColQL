# API Reference

This is a factual summary of the public API. See the topic docs for deeper behavior.

## Imports

```ts
import { table, column, fromRows, ColQLError } from "@colql/colql";
import type {
  MutationResult,
  ObjectWherePredicate,
  Operator,
  QueryHook,
  QueryInfo,
  RowForSchema,
  RowPredicate,
  Schema,
  TableOptions,
} from "@colql/colql";
```

## Table Creation

```ts
const users = table(schema);
const loaded = fromRows(schema, rows);
const instrumented = table(schema, { onQuery: (info) => console.log(info) });
const restored = table.deserialize(buffer);
```

`table(schema)` returns a `Table` instance.
`fromRows(schema, rows, options?)` creates a table and inserts rows with `insertMany`.
`table(schema, options)` accepts compatible table options such as `onQuery`.
`table.deserialize(input)` accepts an `ArrayBuffer` or `Uint8Array` and returns a table.

`onQuery` is called by terminal query operations such as `toArray`, `first`, `count`, aggregations, and query mutations. Query construction itself is not instrumented.

```ts
type QueryInfo = {
  duration: number;
  rowsScanned: number;
  indexUsed: boolean;
};

type QueryHook = (info: QueryInfo) => void;

type TableOptions = {
  onQuery?: QueryHook;
};
```

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

These are mainly useful for advanced integrations and diagnostics. Row indexes are internal positions, not stable external IDs. Most application code should use query and row APIs with an explicit ID column when stable identity is required.

## Query Construction

```ts
users.where(column, operator, value);
users.where(objectPredicate);
users.whereIn(column, values);
users.whereNotIn(column, values);
users.filter(callback);
users.firstWhere(predicate);
users.countWhere(predicate);
users.exists(predicate);
users.select(columns);
users.limit(n);
users.offset(n);
users.query();
```

`query()` creates an unfiltered query over the table. The table-level helpers above are the usual entrypoints for application code.

```ts
users.where({ age: { gt: 25 }, status: "active" });
users.filter((row) => row.age > 25);
```

`where(objectPredicate)` is structured predicate syntax and may use indexes. `filter(callback)` is a full-scan callback escape hatch, runs after structured predicates, and is not index-aware.
`firstWhere`, `countWhere`, and `exists` are table-level wrappers over structured `where(...)` or callback `filter(fn)`.

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
query.where(objectPredicate);
query.whereIn(column, values);
query.whereNotIn(column, values);
query.filter(callback);
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

```ts
type ObjectWherePredicate<TSchema extends Schema> = {
  // column-specific object predicate shape
};

type RowPredicate<TSchema extends Schema> = (row: RowForSchema<TSchema>) => boolean;
```

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
users.updateMany(predicate, partialRow); // MutationResult
users.deleteMany(predicate); // MutationResult

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

Equality indexes are derived performance structures. Unsupported predicates fall back to scan without changing query results.

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

Sorted indexes are numeric range indexes. They are derived performance structures and are rebuilt before use when dirty.

## Unique Indexes

```ts
users.createUniqueIndex(column);       // this
users.dropUniqueIndex(column);         // this
users.hasUniqueIndex(column);          // boolean
users.uniqueIndexes();                 // string[]
users.uniqueIndexStats();              // UniqueIndexStats[]
users.rebuildUniqueIndex(column);      // this
users.rebuildUniqueIndexes();          // this

users.findBy(column, value);           // row | undefined
users.updateBy(column, value, partialRow); // MutationResult
users.deleteBy(column, value);         // MutationResult
```

Unique indexes support numeric and dictionary columns. They are derived structures, not serialized, and enforce uniqueness while present. By-key helpers require an existing unique index and do not scan when one is missing.

## Serialization

```ts
const buffer = users.serialize();      // ArrayBuffer
const restored = table.deserialize(buffer);
```

`deserialize` accepts `ArrayBuffer` or `Uint8Array`. Indexes are not serialized; recreate equality, sorted, and unique indexes after deserialization when indexed performance or uniqueness enforcement is needed.

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

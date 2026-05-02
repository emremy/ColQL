# TypeScript Type Safety

ColQL infers row, predicate, projection, aggregation, and mutation types from the schema.

## Schema Inference

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
});
```

Inferred row shape:

```ts
type User = {
  id: number;
  age: number;
  status: "active" | "passive";
  is_active: boolean;
};
```

## Insert Typing

```ts
users.insert({ id: 1, age: 25, status: "active", is_active: true });

// Compile-time errors:
users.insert({ id: 1, age: 25, status: "deleted", is_active: true });
users.insert({ id: 1, age: 25, status: "active" });
```

## Where Typing

```ts
users.where("age", ">", 18);
users.where("status", "=", "active");
users.whereIn("status", ["active"]);
```

Wrong value types are rejected in TypeScript when enough type information is available:

```ts
users.where("age", "=", "active");      // error
users.where("status", "=", "deleted");  // error with literal dictionary
```

Runtime validation still runs.

## Object Predicate Typing

Object predicates are typed from the table schema:

```ts
users.where({
  age: { gt: 18, lte: 65 },
  status: { in: ["active"] },
  is_active: true,
});
```

Numeric columns allow `eq`, `gt`, `gte`, `lt`, `lte`, and `in`:

```ts
users.where({ age: 25 });
users.where({ age: { eq: 25, gt: 18, in: [25, 30] } });
```

Boolean columns allow default equality, `eq`, and `in`:

```ts
users.where({ is_active: true });
users.where({ is_active: { eq: false, in: [true, false] } });
```

Dictionary columns allow default equality, `eq`, and `in`:

```ts
users.where({ status: "active" });
users.where({ status: { eq: "passive", in: ["active"] } });
```

Range operators on boolean and dictionary columns are compile-time errors:

```ts
users.where({ status: { gt: "active" } }); // error
users.where({ is_active: { lt: true } });  // error
```

## Callback Filter Typing

`filter(fn)` receives a typed full row and must return a boolean:

```ts
users.filter((row) => row.age > 18 && row.status === "active");

users.filter((row) => row.email === "x"); // error: unknown column
users.filter((row) => row.age);           // error: must return boolean
```

## Select Typing

```ts
const rows = users.select(["id", "status"]).toArray();
// Array<{ id: number; status: "active" | "passive" }>
```

## Update Typing

Update payloads are partial rows:

```ts
users.update(0, { age: 26 });
users.where("status", "=", "active").update({ is_active: false });
```

Invalid update keys or value types are compile-time errors:

```ts
users.update(0, { email: "x" });     // error
users.update(0, { age: "twenty" });  // error
```

## MutationResult

```ts
import type { MutationResult } from "@colql/colql";

const result: MutationResult = users.deleteWhere("status", "=", "passive");
console.log(result.affectedRows);
```

## Unique and Migration Helper Typing

Unique indexes accept numeric and dictionary columns. Boolean columns are rejected by the TypeScript surface and by runtime validation:

```ts
users.createUniqueIndex("id");
users.findBy("id", 123);
users.updateBy("id", 123, { status: "active" });
users.deleteBy("id", 123);
```

JS Array migration helpers keep the same schema-derived row and predicate typing:

```ts
const users = fromRows(schema, rows);
users.firstWhere({ status: "active" });
users.countWhere("age", ">=", 18);
users.exists((row) => row.is_active);
```

Structured helper predicates use `where(...)`; callback predicates use `filter(fn)` and are full scans.

## Type Tests

The repository includes `tests/type-inference.test-d.ts` with `@ts-expect-error` examples. These are useful references for the intended type surface.

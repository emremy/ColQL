# Schema and Columns

A ColQL table starts with a schema:

```ts
import { table, column } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
});
```

The schema controls runtime validation, storage selection, TypeScript inference, query typing, serialization metadata, and index support.

## Column Types

Numeric columns:

```ts
column.int16();   // -32768..32767 integers
column.int32();   // -2147483648..2147483647 integers
column.uint8();   // 0..255 integers
column.uint16();  // 0..65535 integers
column.uint32();  // 0..4294967295 integers
column.float32(); // finite number
column.float64(); // finite number
```

Aliases:

```ts
column.smallint();        // int16
column.integer();         // int32
column.real();            // float32
column.doublePrecision(); // float64
```

Boolean and dictionary columns:

```ts
column.boolean();
column.dictionary(["active", "passive", "archived"] as const);
```

Dictionary columns store string-like values as compact numeric codes. Use `as const` when declaring values so TypeScript infers a literal union instead of plain `string`.

## Storage Behavior

- Numeric columns use chunked `TypedArray` storage.
- Dictionary columns store compact integer codes; the code width depends on dictionary size.
- Boolean columns are bit-packed.
- All columns grow together as rows are inserted.

See [Memory Model](./12-memory-model.md) for details.

## Example Schemas

Users:

```ts
const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  is_active: column.boolean(),
});
```

Events:

```ts
const events = table({
  timestamp: column.float64(),
  user_id: column.uint32(),
  type: column.dictionary(["view", "click", "purchase"] as const),
  value: column.float32(),
});
```

Analytics:

```ts
const metrics = table({
  account_id: column.uint32(),
  day: column.uint32(),
  impressions: column.uint32(),
  ctr: column.float32(),
  segment: column.dictionary(["free", "pro", "enterprise"] as const),
});
```

## Validation Examples

ColQL validates at runtime before writing values:

```ts
users.insert({ id: 1, age: 300, status: "active", is_active: true });
// ColQLError code: COLQL_OUT_OF_RANGE

users.insert({ id: 2, age: 20, status: "deleted", is_active: true });
// ColQLError code: COLQL_UNKNOWN_VALUE

users.insert({ id: 3, age: 20, status: "active", is_active: "true" });
// ColQLError code: COLQL_TYPE_MISMATCH
```

TypeScript catches many mistakes earlier:

```ts
users.insert({
  id: 1,
  age: 25,
  status: "active",
  is_active: true,
});

// Compile-time error when dictionary values use `as const`.
users.where("status", "=", "deleted");
```

Runtime validation still matters for JavaScript callers and data from APIs, files, queues, or other untyped sources.

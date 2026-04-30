# Error Handling

ColQL throws `ColQLError` for validation and operational failures.

```ts
import { table, column, ColQLError } from "@colql/colql";

try {
  users.insert({ id: 1, age: 999, status: "active", is_active: true });
} catch (error) {
  if (error instanceof ColQLError) {
    console.log(error.code);
    console.log(error.message);
    console.log(error.details);
  }
}
```

`ColQLError` has:

- `name`: `"ColQLError"`
- `code`: a stable string for programmatic handling
- `message`: human-readable explanation
- `details`: optional structured context

## Validation Philosophy

TypeScript catches many mistakes in typed code, but ColQL also validates runtime input. This prevents silent typed-array coercion and protects JavaScript callers or TypeScript code that receives untrusted data.

Validation happens before mutation for inserts, bulk inserts, updates, and predicate updates.

## Common Error Categories

Schema errors:

- `COLQL_INVALID_SCHEMA`
- `COLQL_INVALID_COLUMN`
- `COLQL_INVALID_COLUMN_TYPE`
- `COLQL_DUPLICATE_COLUMN`

Value errors:

- `COLQL_TYPE_MISMATCH`
- `COLQL_OUT_OF_RANGE`
- `COLQL_UNKNOWN_VALUE`

Query errors:

- `COLQL_INVALID_COLUMN`
- `COLQL_INVALID_OPERATOR`
- `COLQL_INVALID_PREDICATE`
- `COLQL_INVALID_LIMIT`
- `COLQL_INVALID_OFFSET`
- `COLQL_INVALID_ROW_INDEX`

Index errors:

- `COLQL_INDEX_EXISTS`
- `COLQL_INDEX_NOT_FOUND`
- `COLQL_INDEX_UNSUPPORTED_COLUMN`
- `COLQL_SORTED_INDEX_EXISTS`
- `COLQL_SORTED_INDEX_NOT_FOUND`
- `COLQL_SORTED_INDEX_UNSUPPORTED_COLUMN`

Serialization errors:

- `COLQL_INVALID_SERIALIZED_DATA`

## Examples

Unknown column:

```ts
users.where("email", "=", "x");
// COLQL_INVALID_COLUMN
```

Invalid operator:

```ts
users.where("status", ">", "active");
// COLQL_INVALID_OPERATOR because range operators require numeric columns
```

Invalid object predicate:

```ts
users.where({});
// COLQL_INVALID_PREDICATE
```

`COLQL_INVALID_PREDICATE` is thrown for empty object predicates, invalid object predicate operators, and invalid predicate shapes.

Invalid row index:

```ts
users.delete(999);
// COLQL_INVALID_ROW_INDEX
```

Invalid serialized data:

```ts
table.deserialize(new ArrayBuffer(2));
// COLQL_INVALID_SERIALIZED_DATA
```

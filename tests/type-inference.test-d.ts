import { column, table } from "../src";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
});

users.insert({
  id: 1,
  age: 25,
  status: "active",
  is_active: true,
});

const selected: Array<{ id: number; status: "active" | "passive" }> = users
  .select(["id", "status"])
  .toArray();

void selected;

users.sum("age");
users.where("status", "=", "active").avg("age");
users.top(2, "age");
users.select(["id", "age"]).top(1, "age");
users.whereIn("status", ["active"]);
users.whereNotIn("age", [18, 21]);
users.where("age", ">", 18).whereIn("status", ["passive"]).size();
const row: { id: number; age: number; status: "active" | "passive"; is_active: boolean } = users.get(0);
const serialized: ArrayBuffer = users.serialize();
const restored = table.deserialize(serialized);
restored.count();
void row;

// @ts-expect-error unknown column
users.where("missing", "=", 1);

// @ts-expect-error wrong dictionary value
users.where("status", "=", "deleted");

// @ts-expect-error wrong value type
users.where("age", "=", "active");

// @ts-expect-error insert rejects missing fields
users.insert({ id: 1, age: 25, status: "active" });

// @ts-expect-error aggregation requires a numeric column
users.sum("status");

// @ts-expect-error top requires a numeric column
users.top(2, "is_active");

// @ts-expect-error wrong whereIn value type
users.whereIn("age", ["active"]);

// @ts-expect-error wrong whereIn dictionary value
users.whereIn("status", ["deleted"]);

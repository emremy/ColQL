import { column, table } from "../src";
import type { MutationResult } from "../src";

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
const selectedFirst: { id: number; status: "active" | "passive" } | undefined = users
  .where("status", "=", "active")
  .select(["id", "status"])
  .first();

void selected;
void selectedFirst;

users.sum("age");
users.where("status", "=", "active").avg("age");
users.top(2, "age");
users.select(["id", "age"]).top(1, "age");
users.whereIn("status", ["active"]);
users.whereNotIn("age", [18, 21]);
users.where("age", ">", 18).whereIn("status", ["passive"]).size();
users.createIndex("id");
users.createIndex("status");
users.hasIndex("id");
users.indexes();
users.indexStats();
users.dropIndex("status");
users.createSortedIndex("age");
users.hasSortedIndex("age");
users.sortedIndexes();
users.sortedIndexStats();
users.dropSortedIndex("age");
const deleteReturn: typeof users = users.delete(0);
const updateResult: MutationResult = users.update(0, { age: 30 });
const updateStatusResult: MutationResult = users.update(0, { status: "active" });
const updateWhereResult: MutationResult = users.updateWhere("age", ">", 18, { status: "active" });
const queryUpdateResult: MutationResult = users.where("status", "=", "active").select(["id"]).limit(1).update({ age: 25 });
const deleteWhereResult: MutationResult = users.deleteWhere("status", "=", "passive");
const queryDeleteResult: MutationResult = users.where("age", ">", 18).offset(1).limit(1).delete();
users.rebuildIndex("id");
users.rebuildSortedIndex("age");
users.rebuildIndexes();
void deleteReturn;
void updateResult;
void updateStatusResult;
void updateWhereResult;
void queryUpdateResult;
void deleteWhereResult;
void queryDeleteResult;
const row: { id: number; age: number; status: "active" | "passive"; is_active: boolean } = users.get(0);
const serialized: ArrayBuffer = users.serialize();
const restored = table.deserialize(serialized);
restored.count();
void row;

// @ts-expect-error unknown column
users.where("missing", "=", 1);

// @ts-expect-error wrong dictionary value
users.where("status", "=", "deleted");

// @ts-expect-error unknown selected column
users.select(["missing"]);

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

// @ts-expect-error unknown index column
users.createIndex("missing");

// @ts-expect-error sorted indexes require numeric columns
users.createSortedIndex("status");

// @ts-expect-error unknown sorted index column
users.createSortedIndex("missing");

// @ts-expect-error update rejects unknown columns
users.update(0, { missing: 1 });

// @ts-expect-error update rejects wrong value type
users.update(0, { age: "old" });

// @ts-expect-error update rejects wrong dictionary value
users.update(0, { status: "deleted" });

// @ts-expect-error updateWhere rejects unknown partial columns
users.updateWhere("age", "=", 18, { missing: 1 });

// @ts-expect-error updateWhere rejects wrong predicate dictionary value
users.updateWhere("status", "=", "deleted", { age: 1 });

// @ts-expect-error updateWhere rejects wrong predicate value type
users.updateWhere("age", "=", "active", { status: "active" });

// @ts-expect-error query update rejects wrong dictionary value
users.where("age", ">", 18).update({ status: "deleted" });

// @ts-expect-error unknown rebuild index column
users.rebuildIndex("missing");

// @ts-expect-error unknown sorted rebuild index column
users.rebuildSortedIndex("missing");

// @ts-expect-error sorted rebuild indexes require numeric columns
users.rebuildSortedIndex("status");

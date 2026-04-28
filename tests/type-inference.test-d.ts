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
users.delete(0);
const updateResult: { affectedRows: number } = users.update(0, { age: 26 });
const updateWhereResult: { affectedRows: number } = users.updateWhere("status", "=", "active", { is_active: false });
const queryUpdateResult: { affectedRows: number } = users.where("age", ">", 18).select(["id"]).limit(1).update({ status: "passive" });
const deleteWhereResult: { affectedRows: number } = users.deleteWhere("status", "=", "passive");
const queryDeleteResult: { affectedRows: number } = users.where("age", ">", 18).offset(1).limit(1).delete();
users.rebuildIndex("id");
users.rebuildSortedIndex("age");
users.rebuildIndexes();
void updateResult;
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
users.update(0, { age: "active" });

// @ts-expect-error updateWhere rejects unknown partial columns
users.updateWhere("age", "=", 18, { missing: 1 });

// @ts-expect-error query update rejects wrong dictionary value
users.where("age", ">", 18).update({ status: "deleted" });

// @ts-expect-error unknown rebuild index column
users.rebuildIndex("missing");

// @ts-expect-error unknown sorted rebuild index column
users.rebuildSortedIndex("missing");

// @ts-expect-error sorted rebuild indexes require numeric columns
users.rebuildSortedIndex("status");

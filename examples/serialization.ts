import { table, column } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
  is_active: column.boolean(),
});

users.insert({ id: 1, age: 25, status: "active", is_active: true });
users.insert({ id: 2, age: 40, status: "passive", is_active: false });

const buffer = users.serialize();
const restored = table.deserialize(buffer);

console.log(restored.toArray());

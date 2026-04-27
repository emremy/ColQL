import { table, column } from "@colql/colql";

const users = table({
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive"] as const),
});

for (let i = 0; i < 5; i += 1) {
  users.insert({
    id: i,
    age: 20 + i,
    status: i % 2 === 0 ? "active" : "passive",
  });
}

for (const row of users.where("status", "=", "active").select(["id", "age"])) {
  console.log(row);
}

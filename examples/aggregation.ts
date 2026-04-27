import { table, column } from "colql";

const events = table({
  id: column.uint32(),
  value: column.float64(),
  type: column.dictionary(["click", "view"] as const)
});

events.insert({ id: 1, value: 10, type: "click" });
events.insert({ id: 2, value: 20, type: "view" });
events.insert({ id: 3, value: 30, type: "click" });

console.log({
  count: events.count(),
  clickTotal: events.where("type", "=", "click").sum("value"),
  average: events.avg("value"),
  max: events.max("value")
});

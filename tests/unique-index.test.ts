import { describe, expect, it } from "vitest";
import { ColQLError, column, table } from "../src";

const schema = {
  id: column.uint32(),
  age: column.uint8(),
  status: column.dictionary(["active", "passive", "archived"] as const),
  active: column.boolean(),
};

function createUsers() {
  return table(schema).insertMany([
    { id: 1, age: 20, status: "active", active: true },
    { id: 2, age: 30, status: "passive", active: false },
    { id: 3, age: 40, status: "archived", active: true },
  ]);
}

function expectCode(fn: () => unknown, code: string): ColQLError {
  expect(fn).toThrow(ColQLError);
  try {
    fn();
  } catch (error) {
    expect((error as ColQLError).code).toBe(code);
    return error as ColQLError;
  }
  throw new Error("Expected ColQLError");
}

describe("unique indexes", () => {
  it("creates unique indexes on numeric and dictionary columns", () => {
    const users = createUsers();

    expect(users.createUniqueIndex("id")).toBe(users);
    expect(users.createUniqueIndex("status")).toBe(users);

    expect(users.hasUniqueIndex("id")).toBe(true);
    expect(users.uniqueIndexes()).toEqual(["id", "status"]);
    expect(users.uniqueIndexStats()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ column: "id", rowCount: 3, uniqueValues: 3, dirty: false }),
        expect.objectContaining({ column: "status", rowCount: 3, uniqueValues: 3, dirty: false }),
      ]),
    );
  });

  it("rejects boolean unique indexes and duplicate lifecycle operations", () => {
    const users = createUsers();

    expectCode(() => users.createUniqueIndex("active" as never), "COLQL_UNIQUE_INDEX_UNSUPPORTED");
    users.createUniqueIndex("id");
    expectCode(() => users.createUniqueIndex("id"), "COLQL_UNIQUE_INDEX_EXISTS");
    expectCode(() => users.dropUniqueIndex("age"), "COLQL_UNIQUE_INDEX_NOT_FOUND");
    expectCode(() => users.rebuildUniqueIndex("age"), "COLQL_UNIQUE_INDEX_NOT_FOUND");
  });

  it("rejects duplicate existing data on create and rebuild atomically", () => {
    const users = table(schema).insertMany([
      { id: 1, age: 20, status: "active", active: true },
      { id: 1, age: 30, status: "passive", active: false },
    ]);

    const error = expectCode(() => users.createUniqueIndex("id"), "COLQL_DUPLICATE_KEY");
    expect(error.details).toEqual(expect.objectContaining({ columnName: "id" }));
    expect(users.hasUniqueIndex("id")).toBe(false);

    const clean = createUsers().createUniqueIndex("id");
    clean.dropUniqueIndex("id");
    clean.insert({ id: 2, age: 50, status: "active", active: true });
    expectCode(() => clean.createUniqueIndex("id"), "COLQL_DUPLICATE_KEY");
    expect(clean.hasUniqueIndex("id")).toBe(false);
  });

  it("rejects duplicate insert and insertMany while preserving all-or-nothing", () => {
    const users = createUsers().createUniqueIndex("id");
    const before = users.toArray();

    expectCode(() => users.insert({ id: 2, age: 50, status: "active", active: true }), "COLQL_DUPLICATE_KEY");
    expect(users.toArray()).toEqual(before);

    expectCode(
      () =>
        users.insertMany([
          { id: 4, age: 50, status: "active", active: true },
          { id: 2, age: 51, status: "passive", active: false },
        ]),
      "COLQL_DUPLICATE_KEY",
    );
    expect(users.toArray()).toEqual(before);

    expectCode(
      () =>
        users.insertMany([
          { id: 4, age: 50, status: "active", active: true },
          { id: 4, age: 51, status: "passive", active: false },
        ]),
      "COLQL_DUPLICATE_KEY",
    );
    expect(users.toArray()).toEqual(before);

    users.insertMany([
      { id: 4, age: 50, status: "active", active: true },
      { id: 5, age: 51, status: "passive", active: false },
    ]);
    expect(users.findBy("id", 5)).toEqual({ id: 5, age: 51, status: "passive", active: false });
    expect(users.uniqueIndexStats()[0]).toEqual(expect.objectContaining({ rowCount: 5, uniqueValues: 5 }));
  });

  it("rejects duplicate-producing update and updateMany all-or-nothing", () => {
    const users = createUsers().createUniqueIndex("id");
    const before = users.toArray();

    expectCode(() => users.update(0, { id: 2 }), "COLQL_DUPLICATE_KEY");
    expect(users.toArray()).toEqual(before);

    expectCode(() => users.updateMany({ id: { in: [1, 2] } }, { id: 9 }), "COLQL_DUPLICATE_KEY");
    expect(users.toArray()).toEqual(before);

    expectCode(() => users.where("id", "in", [1, 2]).update({ id: 9 }), "COLQL_DUPLICATE_KEY");
    expect(users.toArray()).toEqual(before);
  });

  it("allows unchanged unique-key updates and frees keys after delete", () => {
    const users = createUsers().createUniqueIndex("id");

    expect(users.update(0, { id: 1, age: 21 })).toEqual({ affectedRows: 1 });
    users.deleteBy("id", 2);
    users.insert({ id: 2, age: 55, status: "passive", active: true });

    expect(users.findBy("id", 2)).toEqual({ id: 2, age: 55, status: "passive", active: true });
  });

  it("dropUniqueIndex removes enforcement", () => {
    const users = createUsers().createUniqueIndex("id");

    users.dropUniqueIndex("id");
    users.insert({ id: 1, age: 55, status: "active", active: true });

    expect(users.where("id", "=", 1).count()).toBe(2);
  });

  it("rebuilds dirty unique indexes before stats and by-key lookup", () => {
    const users = createUsers().createUniqueIndex("id");

    users.delete(0);
    expect(users.findBy("id", 2)).toEqual({ id: 2, age: 30, status: "passive", active: false });
    expect(users.uniqueIndexStats()[0]).toEqual(expect.objectContaining({ rowCount: 2, uniqueValues: 2, dirty: false }));

    users.updateBy("id", 2, { id: 20 });
    expect(users.findBy("id", 2)).toBeUndefined();
    expect(users.findBy("id", 20)).toEqual({ id: 20, age: 30, status: "passive", active: false });
  });

  it("does not serialize unique indexes and can recreate after deserialize", () => {
    const users = createUsers().createUniqueIndex("id");
    const restored = table.deserialize(users.serialize());

    expect(restored.uniqueIndexes()).toEqual([]);
    expect(restored.toArray()).toEqual(users.toArray());

    restored.createUniqueIndex("id");
    expect(restored.hasUniqueIndex("id")).toBe(true);
  });
});

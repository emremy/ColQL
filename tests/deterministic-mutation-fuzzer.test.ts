import { describe, expect, it } from "vitest";
import { column, table, type RowForSchema } from "../src";

const schema = {
  id: column.uint32(),
  tenantId: column.uint16(),
  age: column.uint8(),
  score: column.uint32(),
  createdAt: column.uint32(),
  status: column.dictionary(["active", "trial", "paused", "churned"] as const),
  category: column.dictionary(["free", "pro", "team", "enterprise"] as const),
  active: column.boolean(),
};

type Row = RowForSchema<typeof schema>;

function rng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

function integer(random: () => number, maxExclusive: number): number {
  return Math.floor(random() * maxExclusive);
}

function pick<const Values extends readonly unknown[]>(
  random: () => number,
  values: Values,
): Values[number] {
  return values[integer(random, values.length)];
}

function rowForId(id: number, random: () => number): Row {
  const statuses: readonly Row["status"][] = ["active", "trial", "paused", "churned"];
  const categories: readonly Row["category"][] = ["free", "pro", "team", "enterprise"];
  return {
    id,
    tenantId: 1 + integer(random, 32),
    age: integer(random, 100),
    score: integer(random, 10_000),
    createdAt: 1_700_000_000 + integer(random, 1_000_000),
    status: pick(random, statuses),
    category: pick(random, categories),
    active: integer(random, 2) === 0,
  };
}

function createIndexedTable(rows: readonly Row[]) {
  return table(schema)
    .insertMany(rows)
    .createUniqueIndex("id")
    .createIndex("tenantId")
    .createIndex("status")
    .createIndex("category")
    .createSortedIndex("age")
    .createSortedIndex("createdAt");
}

function reindex(restored: ReturnType<typeof table>) {
  return restored
    .createUniqueIndex("id")
    .createIndex("tenantId")
    .createIndex("status")
    .createIndex("category")
    .createSortedIndex("age")
    .createSortedIndex("createdAt");
}

type PredicateCase = {
  readonly label: string;
  readonly apply: (users: ReturnType<typeof createIndexedTable>) => ReturnType<typeof createIndexedTable>["query"];
  readonly test: (row: Row) => boolean;
};

function predicateCase(random: () => number): PredicateCase {
  const status = pick(random, ["active", "trial", "paused", "churned"] as const);
  const category = pick(random, ["free", "pro", "team", "enterprise"] as const);
  const tenantId = 1 + integer(random, 32);
  const age = integer(random, 100);
  const createdAt = 1_700_000_000 + integer(random, 1_000_000);

  switch (integer(random, 6)) {
    case 0:
      return {
        label: `status=${status}`,
        apply: (users) => users.where("status", "=", status),
        test: (row) => row.status === status,
      };
    case 1:
      return {
        label: `category=${category}`,
        apply: (users) => users.where("category", "=", category),
        test: (row) => row.category === category,
      };
    case 2:
      return {
        label: `tenantId=${tenantId}`,
        apply: (users) => users.where("tenantId", "=", tenantId),
        test: (row) => row.tenantId === tenantId,
      };
    case 3:
      return {
        label: `age>=${age}`,
        apply: (users) => users.where("age", ">=", age),
        test: (row) => row.age >= age,
      };
    case 4:
      return {
        label: `createdAt<${createdAt}`,
        apply: (users) => users.where("createdAt", "<", createdAt),
        test: (row) => row.createdAt < createdAt,
      };
    default:
      return {
        label: "active=true",
        apply: (users) => users.where("active", "=", true),
        test: (row) => row.active,
      };
  }
}

function patch(random: () => number): Partial<Row> {
  switch (integer(random, 5)) {
    case 0:
      return { score: integer(random, 10_000) };
    case 1:
      return { age: integer(random, 100) };
    case 2:
      return { status: pick(random, ["active", "trial", "paused", "churned"] as const) };
    case 3:
      return { category: pick(random, ["free", "pro", "team", "enterprise"] as const) };
    default:
      return { active: integer(random, 2) === 0 };
  }
}

function assertParity(
  users: ReturnType<typeof createIndexedTable>,
  oracle: readonly Row[],
  random: () => number,
  context: string,
): void {
  expect(users.toArray(), context).toEqual(oracle);

  for (let index = 0; index < 3; index += 1) {
    const predicate = predicateCase(random);
    expect(predicate.apply(users).toArray(), `${context}:${predicate.label}`).toEqual(
      oracle.filter(predicate.test),
    );
  }

  const target = oracle[integer(random, Math.max(oracle.length, 1))];
  if (target !== undefined) {
    expect(users.findBy("id", target.id), `${context}:findBy`).toEqual(target);
  }
}

describe("deterministic mutation/index/serialization fuzzer", () => {
  it("matches a JS array oracle across long deterministic sequences", () => {
    for (const seed of [0xC01C, 0xD00D, 0x5EED, 0xBEEF, 0xFACE]) {
      const random = rng(seed);
      let nextId = 0;
      const initialRows = Array.from({ length: 160 }, () => rowForId(nextId++, random));
      let oracle = initialRows.map((row) => ({ ...row }));
      let users = createIndexedTable(oracle);
      const history: string[] = [];

      const record = (entry: string): void => {
        history.push(entry);
        if (history.length > 40) {
          history.shift();
        }
      };

      for (let step = 0; step < 260; step += 1) {
        try {
          switch (integer(random, 9)) {
            case 0: {
              const row = rowForId(nextId++, random);
              users.insert(row);
              oracle.push(row);
              record(`insert:${row.id}`);
              break;
            }
            case 1: {
              const rows = Array.from({ length: 1 + integer(random, 3) }, () =>
                rowForId(nextId++, random),
              );
              users.insertMany(rows);
              oracle.push(...rows);
              record(`insertMany:${rows.map((row) => row.id).join(",")}`);
              break;
            }
            case 2: {
              if (oracle.length === 0) break;
              const rowIndex = integer(random, oracle.length);
              const values = patch(random);
              users.update(rowIndex, values);
              oracle[rowIndex] = { ...oracle[rowIndex], ...values };
              record(`update:${rowIndex}:${JSON.stringify(values)}`);
              break;
            }
            case 3: {
              const predicate = predicateCase(random);
              const values = patch(random);
              const affected = oracle.filter(predicate.test).length;
              expect(predicate.apply(users).update(values)).toEqual({ affectedRows: affected });
              oracle = oracle.map((row) => predicate.test(row) ? { ...row, ...values } : row);
              record(`predicateUpdate:${predicate.label}:${JSON.stringify(values)}`);
              break;
            }
            case 4: {
              if (oracle.length === 0) break;
              const rowIndex = integer(random, oracle.length);
              users.delete(rowIndex);
              oracle.splice(rowIndex, 1);
              record(`delete:${rowIndex}`);
              break;
            }
            case 5: {
              const predicate = predicateCase(random);
              const limit = 1 + integer(random, 8);
              const offset = integer(random, 4);
              const targetIds = oracle
                .filter(predicate.test)
                .slice(offset, offset + limit)
                .map((row) => row.id);
              expect(predicate.apply(users).offset(offset).limit(limit).delete()).toEqual({
                affectedRows: targetIds.length,
              });
              oracle = oracle.filter((row) => !targetIds.includes(row.id));
              record(`predicateDelete:${predicate.label}:${offset}:${limit}`);
              break;
            }
            case 6: {
              if (oracle.length === 0) break;
              const target = oracle[integer(random, oracle.length)];
              const values = patch(random);
              expect(users.updateBy("id", target.id, values)).toEqual({ affectedRows: 1 });
              Object.assign(target, values);
              record(`updateBy:${target.id}:${JSON.stringify(values)}`);
              break;
            }
            case 7: {
              if (oracle.length === 0) break;
              const target = oracle[integer(random, oracle.length)];
              expect(users.deleteBy("id", target.id)).toEqual({ affectedRows: 1 });
              oracle = oracle.filter((row) => row.id !== target.id);
              record(`deleteBy:${target.id}`);
              break;
            }
            default: {
              const restored = table.deserialize(users.serialize());
              expect(restored.indexes()).toEqual([]);
              expect(restored.sortedIndexes()).toEqual([]);
              expect(restored.uniqueIndexes()).toEqual([]);
              users = reindex(restored) as ReturnType<typeof createIndexedTable>;
              record("serialize:restore:reindex");
              break;
            }
          }

          if (step % 10 === 0) {
            assertParity(users, oracle, random, `seed=${seed}:step=${step}`);
          }
        } catch (error) {
          throw new Error(
            `Fuzzer failed seed=${seed} step=${step} history=${history.join(" | ")} cause=${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }

      assertParity(users, oracle, random, `seed=${seed}:final`);
    }
  });
});

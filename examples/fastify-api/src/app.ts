import Fastify, { type FastifyInstance } from "fastify";
import { column, table, type QueryInfo, type RowForSchema } from "../../../dist/index.mjs";

const COUNTRIES = [
  "TR",
  "US",
  "DE",
  "GB",
  "FR",
  "NL",
  "ES",
  "IT",
  "CA",
  "BR",
  "JP",
  "KR",
  "IN",
  "AU",
  "SE",
  "NO",
  "DK",
  "FI",
  "PL",
  "MX",
] as const;
const NAMES = [
  "Ada",
  "Grace",
  "Linus",
  "Mina",
  "Emre",
  "Aylin",
  "Nora",
  "Ken",
  "Alan",
  "Katherine",
  "Edsger",
  "Barbara",
  "Dennis",
  "Radia",
  "Margaret",
  "Guido",
  "Donald",
  "Frances",
  "Tim",
  "Anita",
  "Brendan",
  "Sophie",
  "James",
  "Mary",
  "Bjarne",
  "Leslie",
  "Yukihiro",
  "Jean",
  "Niklaus",
  "Lynn",
  "Martin",
  "Evelyn",
] as const;

const userSchema = {
  id: column.uint32(),
  age: column.uint8(),
  country: column.dictionary(COUNTRIES),
  active: column.boolean(),
  name: column.dictionary(NAMES),
  score: column.float64(),
};

type User = RowForSchema<typeof userSchema>;
type Country = User["country"];

type UserInput = User;
type UserPatch = Partial<Pick<User, "active" | "score" | "country">>;

type QueryLogEntry = QueryInfo & {
  readonly operation: number;
};

const seedUsers: readonly User[] = [
  { id: 1, age: 29, country: "TR", active: true, name: "Ada", score: 91.5 },
  { id: 2, age: 41, country: "US", active: true, name: "Grace", score: 88.2 },
  { id: 3, age: 22, country: "DE", active: false, name: "Linus", score: 72.4 },
  { id: 4, age: 35, country: "TR", active: false, name: "Mina", score: 79.1 },
  { id: 5, age: 31, country: "GB", active: true, name: "Emre", score: 84.6 },
];

function parseSeedSize(value: string | undefined): number {
  if (value === undefined || value.trim() === "") {
    return seedUsers.length;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`COLQL_EXAMPLE_SEED_SIZE must be a non-negative integer. Received ${value}.`);
  }

  return parsed;
}

export function generateUsers(count: number): User[] {
  return Array.from({ length: count }, (_unused, index) => {
    const id = index + 1;
    return {
      id,
      age: 18 + (index % 65),
      country: COUNTRIES[index % COUNTRIES.length],
      active: index % 3 !== 0,
      name: NAMES[index % NAMES.length],
      score: 50 + (index % 500) / 10,
    };
  });
}

function createUserStore(initialUsers: readonly User[] = seedUsers) {
  const queryLog: QueryLogEntry[] = [];
  const users = table(userSchema, {
    onQuery(info) {
      queryLog.push({ ...info, operation: queryLog.length + 1 });
    },
  });

  users.insertMany(initialUsers);
  users.createIndex("country");
  users.createIndex("name");
  users.createSortedIndex("age");

  return { users, queryLog };
}

function parseBoolean(value: unknown): boolean | undefined {
  if (value === undefined) {
    return undefined;
  }

  if (value === "true" || value === true) {
    return true;
  }

  if (value === "false" || value === false) {
    return false;
  }

  return undefined;
}

function parseNumber(value: unknown): number | undefined {
  if (typeof value !== "string" || value.trim() === "") {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function isCountry(value: unknown): value is Country {
  return typeof value === "string" && (COUNTRIES as readonly string[]).includes(value);
}

type UserQuery = {
  readonly id?: string;
  readonly country?: string;
  readonly minAge?: string;
  readonly maxAge?: string;
  readonly active?: string;
  readonly search?: string;
  readonly limit?: string;
};

function applyUserFilters(store: ReturnType<typeof createUserStore>["users"], params: UserQuery) {
  let usersQuery = store.query();

  const where: {
    id?: number;
    country?: Country;
    age?: { gt?: number; lt?: number };
    active?: boolean;
  } = {};

  const id = parseNumber(params.id);
  if (id !== undefined) {
    where.id = id;
  }

  if (isCountry(params.country)) {
    where.country = params.country;
  }

  const minAge = parseNumber(params.minAge);
  if (minAge !== undefined) {
    where.age = { ...where.age, gt: minAge };
  }

  const maxAge = parseNumber(params.maxAge);
  if (maxAge !== undefined) {
    where.age = { ...where.age, lt: maxAge };
  }

  const active = parseBoolean(params.active);
  if (active !== undefined) {
    where.active = active;
  }

  if (Object.keys(where).length > 0) {
    usersQuery = usersQuery.where(where);
  }

  if (params.search !== undefined && params.search.trim() !== "") {
    const term = params.search.toLowerCase();
    usersQuery = usersQuery.filter((row) => row.name.toLowerCase().includes(term));
  }

  const limit = parseNumber(params.limit);
  if (limit !== undefined) {
    usersQuery = usersQuery.limit(limit);
  }

  return usersQuery;
}

function resolveInitialUsers(options: { readonly seed?: boolean; readonly seedSize?: number }): readonly User[] {
  if (options.seed === false) {
    return [];
  }

  if (options.seedSize !== undefined) {
    return generateUsers(options.seedSize);
  }

  const envSeedSize = parseSeedSize(process.env.COLQL_EXAMPLE_SEED_SIZE);
  return envSeedSize === seedUsers.length ? seedUsers : generateUsers(envSeedSize);
}

export function buildApp(options: { readonly seed?: boolean; readonly seedSize?: number } = {}): FastifyInstance {
  const app = Fastify({ logger: false });
  const { users, queryLog } = createUserStore(resolveInitialUsers(options));

  app.get("/health", async () => ({ ok: true }));

  app.post<{ Body: UserInput }>("/users", async (request, reply) => {
    users.insert(request.body);
    return reply.code(201).send({ user: users.where("id", "=", request.body.id).first() });
  });

  app.post<{ Body: { users: UserInput[] } }>("/users/bulk", async (request) => {
    users.insertMany(request.body.users);
    return { inserted: request.body.users.length, rowCount: users.rowCount };
  });

  app.get<{ Querystring: UserQuery }>("/users", async (request) => {
    const rows = applyUserFilters(users, request.query)
      .select(["id", "name", "age", "country", "active"])
      .toArray();
    return { users: rows };
  });

  app.get<{ Querystring: UserQuery }>("/users/count", async (request) => {
    return { count: applyUserFilters(users, request.query).count() };
  });

  app.patch<{ Params: { country: string }; Body: UserPatch }>("/users/by-country/:country", async (request) => {
    if (!isCountry(request.params.country)) {
      return { affectedRows: 0 };
    }

    return users.updateMany({ country: request.params.country }, request.body);
  });

  app.delete("/users/inactive", async () => users.deleteMany({ active: false }));

  app.get("/debug/query-log", async () => ({ entries: queryLog }));

  app.get("/debug/indexes", async () => ({
    equality: users.indexStats(),
    sorted: users.sortedIndexStats(),
  }));

  app.get("/debug/memory", async () => ({
    rowCount: users.rowCount,
    capacity: users.capacity,
    materializedRowCount: users.materializedRowCount,
    scannedRowCount: users.scannedRowCount,
  }));

  return app;
}

import { afterEach, describe, expect, it } from "vitest";
import type { FastifyInstance } from "fastify";
import { buildApp, generateUsers } from "../src/app";

let app: FastifyInstance | undefined;

function createApp() {
  app = buildApp({ seed: true });
  return app;
}

afterEach(async () => {
  await app?.close();
  app = undefined;
});

describe("fastify ColQL example", () => {
  it("generates deterministic larger seed data", () => {
    const users = generateUsers(12);

    expect(users).toHaveLength(12);
    expect(users[0]).toEqual({ id: 1, age: 18, country: "TR", active: false, name: "Ada", score: 50 });
    expect(users[4]).toEqual({ id: 5, age: 22, country: "FR", active: true, name: "Emre", score: 50.4 });
  });

  it("can boot with a generated seed size", async () => {
    const server = buildApp({ seedSize: 100 });
    app = server;

    const memory = await server.inject({ method: "GET", url: "/debug/memory" });
    expect(memory.json()).toEqual(expect.objectContaining({ rowCount: 100 }));
  });

  it("serves health checks", async () => {
    const response = await createApp().inject({ method: "GET", url: "/health" });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
  });

  it("inserts one user with insert(row)", async () => {
    const server = createApp();
    const response = await server.inject({
      method: "POST",
      url: "/users",
      payload: { id: 10, age: 27, country: "US", active: true, name: "Nora", score: 93.2 },
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toEqual({
      user: { id: 10, age: 27, country: "US", active: true, name: "Nora", score: 93.2 },
    });
  });

  it("bulk inserts users with insertMany(rows)", async () => {
    const server = createApp();
    const response = await server.inject({
      method: "POST",
      url: "/users/bulk",
      payload: {
        users: [
          { id: 11, age: 24, country: "TR", active: true, name: "Aylin", score: 82 },
          { id: 12, age: 44, country: "GB", active: false, name: "Ken", score: 77 },
        ],
      },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ inserted: 2, rowCount: 7 });
  });

  it("queries users with object where filters and projection", async () => {
    const response = await createApp().inject({
      method: "GET",
      url: "/users?country=TR&active=true",
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      users: [{ id: 1, name: "Ada", age: 29, country: "TR", active: true }],
    });
  });

  it("queries users with range filters backed by sorted index planning", async () => {
    const response = await createApp().inject({
      method: "GET",
      url: "/users?minAge=30&maxAge=40",
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      users: [
        { id: 4, name: "Mina", age: 35, country: "TR", active: false },
        { id: 5, name: "Emre", age: 31, country: "GB", active: true },
      ],
    });
  });

  it("uses callback filter search after structured filters", async () => {
    const response = await createApp().inject({
      method: "GET",
      url: "/users?country=TR&search=mi",
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      users: [{ id: 4, name: "Mina", age: 35, country: "TR", active: false }],
    });
  });

  it("counts filtered users", async () => {
    const response = await createApp().inject({
      method: "GET",
      url: "/users/count?active=true",
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ count: 3 });
  });

  it("updates matching users with updateMany(predicate, partialRow)", async () => {
    const server = createApp();
    const update = await server.inject({
      method: "PATCH",
      url: "/users/by-country/TR",
      payload: { active: true },
    });

    expect(update.statusCode).toBe(200);
    expect(update.json()).toEqual({ affectedRows: 2 });

    const users = await server.inject({ method: "GET", url: "/users?country=TR&active=true" });
    expect(users.json().users).toHaveLength(2);
  });

  it("deletes inactive users with deleteMany(predicate)", async () => {
    const server = createApp();
    const deleted = await server.inject({ method: "DELETE", url: "/users/inactive" });

    expect(deleted.statusCode).toBe(200);
    expect(deleted.json()).toEqual({ affectedRows: 2 });

    const count = await server.inject({ method: "GET", url: "/users/count" });
    expect(count.json()).toEqual({ count: 3 });
  });

  it("records onQuery hook entries", async () => {
    const server = createApp();
    await server.inject({ method: "GET", url: "/users?country=US" });
    await server.inject({ method: "GET", url: "/users/count?minAge=30" });

    const response = await server.inject({ method: "GET", url: "/debug/query-log" });
    const body = response.json();

    expect(response.statusCode).toBe(200);
    expect(body.entries.length).toBeGreaterThanOrEqual(2);
    expect(body.entries[0]).toEqual(
      expect.objectContaining({
        duration: expect.any(Number),
        rowsScanned: expect.any(Number),
        indexUsed: expect.any(Boolean),
        operation: 1,
      }),
    );
  });

  it("exposes index stats and memory counters through debug endpoints", async () => {
    const server = createApp();

    const indexes = await server.inject({ method: "GET", url: "/debug/indexes" });
    expect(indexes.statusCode).toBe(200);
    expect(indexes.json()).toEqual({
      equality: expect.arrayContaining([
        expect.objectContaining({ column: "country" }),
        expect.objectContaining({ column: "name" }),
      ]),
      sorted: expect.arrayContaining([expect.objectContaining({ column: "age" })]),
    });

    const memory = await server.inject({ method: "GET", url: "/debug/memory" });
    expect(memory.statusCode).toBe(200);
    expect(memory.json()).toEqual(
      expect.objectContaining({
        rowCount: 5,
        capacity: expect.any(Number),
        materializedRowCount: expect.any(Number),
        scannedRowCount: expect.any(Number),
      }),
    );
  });
});

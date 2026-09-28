/**
 * Unit tests for `RedisEventStore` with a mocked `redis-orm-lite` module
 * (in-memory fake: no Redis server needed).
 *
 * Retry semantics themselves are tested against the REAL stack in
 * `tests/retry.test.ts`; here we verify the adapter forwards the user's
 * retry policy into `redis-orm-lite` operations.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

interface FakeDoc {
  id: string;
  [key: string]: unknown;
}

const hoisted = vi.hoisted(() => {
  const docs = new Map<string, Map<string, FakeDoc>>();
  const strings = new Map<string, string>();
  const zsets = new Map<string, Map<string, number>>();
  const executeCalls: { command: string; override: unknown }[] = [];
  const modelCalls: { method: string; options: unknown }[] = [];
  const connectUrls: string[] = [];
  let failConnect: Error | undefined;

  class FakeClient {
    async incr(key: string): Promise<number> {
      const next = Number(strings.get(key) ?? "0") + 1;
      strings.set(key, String(next));
      return next;
    }
    async set(key: string, value: string): Promise<string> {
      strings.set(key, value);
      return "OK";
    }
    async get(key: string): Promise<string | null> {
      return strings.get(key) ?? null;
    }
    async del(...keys: string[]): Promise<number> {
      let removed = 0;
      for (const key of keys) {
        if (strings.delete(key)) removed += 1;
        // Docs live in `docs` under their full key namespace.
        for (const table of docs.values()) {
          if (table.delete(key)) removed += 1;
        }
      }
      return removed;
    }
    async scan(
      _cursor: number,
      options: { MATCH: string },
    ): Promise<{ cursor: number; keys: string[] }> {
      const prefix = options.MATCH.replace(/\*/g, "");
      const keys: string[] = [];
      for (const key of strings.keys()) {
        if (key.startsWith(prefix)) keys.push(key);
      }
      for (const table of docs.values()) {
        for (const key of table.keys()) {
          if (key.startsWith(prefix)) keys.push(key);
        }
      }
      return { cursor: 0, keys };
    }
    async zAdd(
      key: string,
      members: { score: number; value: string }[],
    ): Promise<number> {
      let table = zsets.get(key);
      if (!table) {
        table = new Map();
        zsets.set(key, table);
      }
      for (const m of members) table.set(m.value, m.score);
      return members.length;
    }
    async zCard(key: string): Promise<number> {
      return zsets.get(key)?.size ?? 0;
    }
    async zRange(
      key: string,
      start: number,
      stop: number,
      options?: { REV?: boolean },
    ): Promise<string[]> {
      // Redis applies REV before start/stop (like real ZRANGE … REV).
      const ordered = [
        ...(zsets.get(key) ?? new Map<string, number>()).entries(),
      ]
        .sort((a, b) => a[1] - b[1])
        .map(([member]) => member);
      const directed = options?.REV === true ? ordered.reverse() : ordered;
      return stop < 0 ? directed.slice(start) : directed.slice(start, stop + 1);
    }
    async zRangeByScore(
      key: string,
      min: string | number,
      max: string | number,
    ): Promise<string[]> {
      const table: Map<string, number> =
        zsets.get(key) ?? new Map<string, number>();
      const low =
        typeof min === "string" && min.startsWith("(")
          ? { value: Number(min.slice(1)), exclusive: true }
          : {
              value: min === "-inf" ? -Infinity : Number(min),
              exclusive: false,
            };
      const high = max === "+inf" ? Infinity : Number(max);
      return [...table.entries()]
        .filter(
          ([, score]) =>
            (low.exclusive ? score > low.value : score >= low.value) &&
            score <= high,
        )
        .sort((a, b) => a[1] - b[1])
        .map(([member]) => member);
    }
    async zRemRangeByRank(
      key: string,
      start: number,
      stop: number,
    ): Promise<number> {
      const table = zsets.get(key);
      if (!table) return 0;
      const ordered = [...table.entries()].sort((a, b) => a[1] - b[1]);
      const end = stop < 0 ? ordered.length + stop : stop;
      let removed = 0;
      for (let i = start; i <= end && i < ordered.length; i += 1) {
        const member = ordered[i]?.[0];
        if (member !== undefined && table.delete(member)) removed += 1;
      }
      return removed;
    }
    async publish(): Promise<number> {
      return 0;
    }
    duplicate(): this {
      return this;
    }
    async connect(): Promise<this> {
      return this;
    }
    async subscribe(): Promise<void> {}
    async unsubscribe(): Promise<void> {}
    async quit(): Promise<string> {
      return "OK";
    }
  }

  const client = new FakeClient();

  class FakeRedisModel {
    constructor(private modelName: string) {}
    private key(id: string): string {
      return `${this.modelName}:${id}`;
    }
    private table(): Map<string, FakeDoc> {
      let table = docs.get(this.modelName);
      if (!table) {
        table = new Map();
        docs.set(this.modelName, table);
      }
      return table;
    }
    async create(doc: FakeDoc, options?: unknown): Promise<FakeDoc> {
      modelCalls.push({ method: "create", options });
      const stored = { ...doc };
      this.table().set(this.key(doc.id), stored);
      return stored;
    }
    async findById(id: string, options?: unknown): Promise<FakeDoc | null> {
      modelCalls.push({ method: "findById", options });
      return this.table().get(this.key(id)) ?? null;
    }
  }

  return {
    docs,
    strings,
    zsets,
    executeCalls,
    modelCalls,
    connectUrls,
    client,
    FakeRedisModel,
    get failConnect() {
      return failConnect;
    },
    set failConnect(value: Error | undefined) {
      failConnect = value;
    },
  };
});

vi.mock("redis-orm-lite", () => ({
  RedisModel: hoisted.FakeRedisModel,
  connectRedis: async (url: string) => {
    hoisted.connectUrls.push(url);
    if (hoisted.failConnect) throw hoisted.failConnect;
    return hoisted.client;
  },
  executeRedisCommand: async (
    command: string,
    fn: () => Promise<unknown>,
    override?: unknown,
  ) => {
    hoisted.executeCalls.push({ command, override });
    return fn();
  },
}));

import { SSEError } from "../src/errors.js";
import { RedisEventStore } from "../src/redis.js";

beforeEach(() => {
  hoisted.docs.clear();
  hoisted.strings.clear();
  hoisted.zsets.clear();
  hoisted.executeCalls.length = 0;
  hoisted.modelCalls.length = 0;
  hoisted.connectUrls.length = 0;
  hoisted.failConnect = undefined;
});

describe("RedisEventStore", () => {
  it("persists events with Redis-side sequences and replays after an ID", async () => {
    const store = new RedisEventStore({ keyPrefix: "t1" });
    await store.append({ id: "a", data: "one", createdAt: 1 });
    await store.append({ id: "b", data: "two", createdAt: 2 });
    await store.append({ id: "c", data: "three", createdAt: 3 });

    const replayed = await store.getAfter("a");
    expect(replayed.map((e) => e.id)).toEqual(["b", "c"]);
    expect(replayed[0]).toMatchObject({ data: "two", sequence: 2 });
    expect(await store.getAfter("c")).toEqual([]);
    expect(await store.getAfter("unknown")).toEqual([]);
    await store.close();
  });

  it("stores event name, retry, topic, and timestamps", async () => {
    const store = new RedisEventStore({ keyPrefix: "t2" });
    await store.append({
      id: "e1",
      event: "invoice-created",
      data: { invoiceId: "123" },
      retry: 5000,
      topic: "invoices",
      createdAt: 42,
    });
    const [stored] = await store.getRecent(5);
    expect(stored).toMatchObject({
      id: "e1",
      event: "invoice-created",
      data: { invoiceId: "123" },
      retry: 5000,
      topic: "invoices",
      createdAt: 42,
    });
    expect((await store.getAfter("e1", { topic: "other" })).length).toBe(0);
    await store.close();
  });

  it("getRecent returns the newest N oldest-first", async () => {
    const store = new RedisEventStore({ keyPrefix: "t3" });
    for (const id of ["a", "b", "c", "d"]) {
      await store.append({ id, data: id, createdAt: 1 });
    }
    expect((await store.getRecent(2)).map((e) => e.id)).toEqual(["c", "d"]);
    expect(await store.getRecent(0)).toEqual([]);
    await store.close();
  });

  it("evicts oldest entries beyond maxEvents (docs and index)", async () => {
    const store = new RedisEventStore({ keyPrefix: "t4", maxEvents: 2 });
    await store.append({ id: "a", data: 1, createdAt: 1 });
    await store.append({ id: "b", data: 2, createdAt: 2 });
    await store.append({ id: "c", data: 3, createdAt: 3 });
    expect((await store.getRecent(10)).map((e) => e.id)).toEqual(["b", "c"]);
    expect(await store.getAfter("a")).toEqual([]);
    // Evicted documents are deleted, not just unindexed.
    expect(hoisted.docs.get("t4:events")?.has("t4:events:a")).toBe(false);
    await store.close();
  });

  it("re-appending the same ID is idempotent (no duplicate logical event)", async () => {
    const store = new RedisEventStore({ keyPrefix: "t5", maxEvents: 10 });
    await store.append({ id: "dup", data: "v1", createdAt: 1 });
    await store.append({ id: "dup", data: "v2", createdAt: 2 });
    const recent = await store.getRecent(10);
    expect(recent).toHaveLength(1);
    expect(recent[0]).toMatchObject({ id: "dup", data: "v2" });
    await store.close();
  });

  it("isolates histories by keyPrefix", async () => {
    const one = new RedisEventStore({ keyPrefix: "iso-one" });
    const two = new RedisEventStore({ keyPrefix: "iso-two" });
    await one.append({ id: "a", data: 1, createdAt: 1 });
    expect(await two.getRecent(10)).toEqual([]);
    expect((await one.getRecent(10)).map((e) => e.id)).toEqual(["a"]);
    await one.close();
    await two.close();
  });

  it("clear() removes documents, index, and counter", async () => {
    const store = new RedisEventStore({ keyPrefix: "t6" });
    await store.append({ id: "a", data: 1, createdAt: 1 });
    await store.clear();
    expect(await store.getRecent(10)).toEqual([]);
    // Counter resets so sequences restart (deterministic tests/recovery).
    await store.append({ id: "b", data: 2, createdAt: 2 });
    const [doc] = await store.getRecent(10);
    expect(doc?.sequence).toBe(1);
    await store.close();
  });

  it("connects lazily once, using the configured URL", async () => {
    const store = new RedisEventStore({
      url: "redis://example:6379",
      keyPrefix: "t7",
    });
    expect(store.url).toBe("redis://example:6379");
    expect(store.keyPrefix).toBe("t7");
    expect(store.maxEvents).toBe(1000);
    expect(hoisted.connectUrls).toEqual([]);
    await store.connect();
    await store.append({ id: "a", data: 1, createdAt: 1 });
    await store.getRecent(5);
    expect(hoisted.connectUrls).toEqual(["redis://example:6379"]);
    await store.close();
  });

  it("forwards the retry policy to every Redis round-trip", async () => {
    const retry = { retries: 5, backoff: "exponential" as const, delay: 500 };
    const store = new RedisEventStore({ keyPrefix: "t8", retry });
    await store.append({ id: "a", data: 1, createdAt: 1 });
    await store.getAfter("a");
    // Model operations receive the policy as OperationOptions…
    expect(hoisted.modelCalls.filter((c) => c.method === "create")).toEqual([
      { method: "create", options: { retry } },
    ]);
    // …and raw commands flow through executeRedisCommand with it.
    const incrCalls = hoisted.executeCalls.filter((c) => c.command === "incr");
    expect(incrCalls.length).toBeGreaterThan(0);
    for (const call of hoisted.executeCalls) {
      expect(call.override).toEqual({ retry });
    }
    await store.close();
  });

  it("passes no retry override when unconfigured (single attempt)", async () => {
    const store = new RedisEventStore({ keyPrefix: "t9" });
    await store.append({ id: "a", data: 1, createdAt: 1 });
    for (const call of hoisted.executeCalls) {
      expect(call.override).toBeUndefined();
    }
    for (const call of hoisted.modelCalls) {
      expect(call.options).toBeUndefined();
    }
    await store.close();
  });

  it("surfaces connection failures instead of swallowing them", async () => {
    const store = new RedisEventStore({ keyPrefix: "t10" });
    hoisted.failConnect = new Error("ECONNREFUSED");
    await expect(
      store.append({ id: "a", data: 1, createdAt: 1 }),
    ).rejects.toThrow("ECONNREFUSED");
    await store.close();
  });

  it("validates inputs", async () => {
    expect(() => new RedisEventStore({ maxEvents: 0 })).toThrow(SSEError);
    const store = new RedisEventStore({ keyPrefix: "t11" });
    await expect(
      store.append({ id: "", data: 1, createdAt: 1 }),
    ).rejects.toThrow(SSEError);
    await expect(store.getRecent(-1)).rejects.toThrow(SSEError);
    await store.close();
  });
});

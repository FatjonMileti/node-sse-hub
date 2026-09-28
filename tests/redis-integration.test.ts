/**
 * Real-Redis integration tests. These require a Redis server and are
 * SKIPPED by default. Run explicitly with:
 *
 *   npm run test:integration
 *
 * (optionally: SSE_KIT_TEST_REDIS_URL=redis://host:6379).
 * Start a server first, e.g.: docker run -p 6379:6379 redis:7
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { RedisEventBus, RedisEventStore } from "../src/redis.js";
import { SSEServer } from "../src/SSEServer.js";
import { createMocks, sleep } from "./helpers.js";

const enabled = process.env["SSE_KIT_TEST_REDIS"] === "1";
const url = process.env["SSE_KIT_TEST_REDIS_URL"] ?? "redis://localhost:6379";
const prefix = `sse-kit:itest:${process.pid}`;

describe.skipIf(!enabled)("redis integration (real server)", () => {
  let store: RedisEventStore;

  beforeAll(async () => {
    store = new RedisEventStore({ url, keyPrefix: prefix, maxEvents: 50 });
    try {
      await store.connect();
      await store.clear();
    } catch (error) {
      throw new Error(
        `Redis not reachable at ${url} — start one with ` +
          `"docker run -p 6379:6379 redis:7". ` +
          `Original error: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }, 15_000);

  afterAll(async () => {
    await store.clear().catch(() => {});
    await store.close();
  });

  it("round-trips events with ordering and replay", async () => {
    await store.append({ id: "r1", data: "one", createdAt: Date.now() });
    await store.append({ id: "r2", data: "two", createdAt: Date.now() });
    const replayed = await store.getAfter("r1");
    expect(replayed.map((e) => e.id)).toEqual(["r2"]);
    expect(replayed[0]?.sequence).toBeGreaterThan(0);
    expect((await store.getRecent(1)).map((e) => e.id)).toEqual(["r2"]);
  });

  it("replays missed events to a reconnecting SSE client", async () => {
    const sse = new SSEServer({
      history: { enabled: true, store },
      generateEventId: true,
    });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ data: "live-one" });
    sse.broadcast({ data: "live-two" });
    await sse.flushHistory();

    const second = createMocks({ "last-event-id": "1" });
    sse.connect(second.req.asIncomingMessage(), second.res.asServerResponse());
    const start = Date.now();
    while (!second.res.body.includes("live-two") && Date.now() - start < 5000) {
      await sleep(25);
    }
    expect(second.res.body).toContain("live-two");
    await sse.close();
  });

  it("fans out between two buses over real Pub/Sub", async () => {
    const channel = `${prefix}:bus`;
    const busA = new RedisEventBus({ url, channel });
    const busB = new RedisEventBus({ url, channel });
    const received: unknown[] = [];
    await busB.subscribe((envelope) => {
      received.push(envelope);
    });
    await busA.publish({ origin: "itest-a", event: { data: "ping" } });
    const start = Date.now();
    while (received.length === 0 && Date.now() - start < 5000) {
      await sleep(25);
    }
    expect(received).toHaveLength(1);
    await busA.close();
    await busB.close();
  });
});

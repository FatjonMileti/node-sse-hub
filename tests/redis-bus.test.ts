import { beforeEach, describe, expect, it, vi } from "vitest";
import { SSEServer } from "../src/SSEServer.js";
import type { SSEBusEnvelope, SSEEventBus } from "../src/store.js";
import { MemoryEventStore } from "../src/store.js";
import { createMocks, sleep } from "./helpers.js";

/** In-memory bus shared between servers in these tests. */
class FakeBus implements SSEEventBus {
  handlers = new Set<(envelope: SSEBusEnvelope) => void>();
  published: SSEBusEnvelope[] = [];
  failPublish: Error | undefined;
  unsubscribed = 0;

  async publish(envelope: SSEBusEnvelope): Promise<void> {
    this.published.push(envelope);
    if (this.failPublish) throw this.failPublish;
    // Async delivery like a real transport.
    await sleep(0);
    for (const handler of [...this.handlers]) handler(envelope);
  }

  async subscribe(
    handler: (envelope: SSEBusEnvelope) => void,
  ): Promise<() => void> {
    this.handlers.add(handler);
    return () => {
      this.handlers.delete(handler);
      this.unsubscribed += 1;
    };
  }

  async close(): Promise<void> {}
}

function twoNodes() {
  const busA = new FakeBus();
  const busB = new FakeBus();
  // One shared transport: A publishes into B's handlers and vice versa.
  const hub = new Set<(envelope: SSEBusEnvelope) => void>();
  for (const bus of [busA, busB]) {
    bus.handlers = hub;
  }
  const nodeA = new SSEServer({ nodeId: "node-a", bus: busA });
  const nodeB = new SSEServer({ nodeId: "node-b", bus: busB });
  return { busA, busB, nodeA, nodeB };
}

describe("distributed fan-out over SSEEventBus", () => {
  it("broadcasts on one node reach clients of another", async () => {
    const { nodeA, nodeB } = twoNodes();
    const a = createMocks();
    const b = createMocks();
    nodeA.connect(a.req.asIncomingMessage(), a.res.asServerResponse());
    nodeB.connect(b.req.asIncomingMessage(), b.res.asServerResponse());

    // Wait for both subscriptions to register.
    await sleep(10);
    expect(nodeA.broadcast({ event: "news", data: "hello" })).toBe(1);
    await sleep(20);
    expect(b.res.body).toContain("event: news");
    expect(a.res.body).toContain("event: news");
    await nodeA.close();
    await nodeB.close();
  });

  it("delivers exactly once per node (origin loop prevention)", async () => {
    const { nodeA, nodeB } = twoNodes();
    const a = createMocks();
    const b = createMocks();
    nodeA.connect(a.req.asIncomingMessage(), a.res.asServerResponse());
    nodeB.connect(b.req.asIncomingMessage(), b.res.asServerResponse());
    await sleep(10);

    nodeA.broadcast({ data: "once" });
    await sleep(30);
    const count = (body: string): number => body.split("data: once").length - 1;
    expect(count(a.res.body)).toBe(1);
    expect(count(b.res.body)).toBe(1);
    await nodeA.close();
    await nodeB.close();
  });

  it("routes topic broadcasts to subscribers on other nodes", async () => {
    const { nodeA, nodeB } = twoNodes();
    const subscribed = createMocks();
    const stranger = createMocks();
    nodeB
      .connect(
        subscribed.req.asIncomingMessage(),
        subscribed.res.asServerResponse(),
      )
      .subscribe("invoices");
    nodeB.connect(
      stranger.req.asIncomingMessage(),
      stranger.res.asServerResponse(),
    );
    await sleep(10);

    nodeA.to("invoices").broadcast({ event: "created", data: 1 });
    await sleep(20);
    expect(subscribed.res.body).toContain("created");
    expect(stranger.res.body).not.toContain("created");
    await nodeA.close();
    await nodeB.close();
  });

  it("records remote events in local history", async () => {
    const storeB = new MemoryEventStore({ maxEvents: 100 });
    const busA = new FakeBus();
    const busB = new FakeBus();
    const hub = new Set<(envelope: SSEBusEnvelope) => void>();
    busA.handlers = hub;
    busB.handlers = hub;
    const nodeA = new SSEServer({ nodeId: "a", bus: busA });
    const nodeB = new SSEServer({
      nodeId: "b",
      bus: busB,
      history: { enabled: true, store: storeB },
      generateEventId: true,
    });
    await sleep(10);
    nodeA.broadcast({ data: "from-a" });
    await sleep(20);
    await nodeB.flushHistory();
    expect((await storeB.getRecent(5)).map((e) => e.data)).toEqual(["from-a"]);
    await nodeA.close();
    await nodeB.close();
  });

  it("bus failures surface via busError without breaking delivery", async () => {
    const bus = new FakeBus();
    bus.failPublish = new Error("nats down");
    const sse = new SSEServer({ bus });
    const errors: Error[] = [];
    sse.onBusError((error) => {
      errors.push(error);
    });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(10);
    expect(sse.broadcast({ data: "live" })).toBe(1);
    expect(res.body).toContain("data: live");
    await sleep(10);
    expect(errors.map((e) => e.message)).toEqual(["nats down"]);
    await sse.close();
  });

  it("close() releases the bus subscription", async () => {
    const bus = new FakeBus();
    const sse = new SSEServer({ bus });
    await sleep(10);
    expect(bus.handlers.size).toBe(1);
    await sse.close();
    expect(bus.handlers.size).toBe(0);
    expect(bus.unsubscribed).toBe(1);
  });
});

describe("RedisEventBus transport (mocked redis-orm-lite)", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  async function mockedBusModule() {
    vi.doMock("redis-orm-lite", () => {
      const hub = new Map<string, Set<(message: string) => void>>();
      const makeClient = () => ({
        publish: async (channel: string, message: string) => {
          await sleep(0);
          for (const handler of hub.get(channel) ?? []) handler(message);
          return 1;
        },
        duplicate: () => makeClient(),
        connect: async () => {},
        subscribe: async (
          channel: string,
          handler: (message: string) => void,
        ) => {
          let set = hub.get(channel);
          if (!set) {
            set = new Set();
            hub.set(channel, set);
          }
          set.add(handler);
        },
        unsubscribe: async (channel: string) => {
          hub.get(channel)?.clear();
        },
        quit: async () => "OK",
      });
      return {
        RedisModel: vi.fn(),
        connectRedis: async () => makeClient(),
        executeRedisCommand: async (
          _command: string,
          fn: () => Promise<unknown>,
        ) => fn(),
        __hub: hub,
      };
    });
    const redis = await import("../src/redis.js");
    const mocked = (await import("redis-orm-lite")) as unknown as {
      __hub: Map<string, Set<(message: string) => void>>;
    };
    return { ...redis, hub: mocked.__hub };
  }

  it("publishes and receives envelopes over a fake channel hub", async () => {
    const { RedisEventBus } = await mockedBusModule();
    const receivedA: SSEBusEnvelope[] = [];
    const receivedB: SSEBusEnvelope[] = [];
    const busA = new RedisEventBus({ channel: "test-bus" });
    const busB = new RedisEventBus({ channel: "test-bus" });
    await busA.subscribe((e) => {
      receivedA.push(e);
    });
    await busB.subscribe((e) => {
      receivedB.push(e);
    });
    await busA.publish({ origin: "a", event: { data: "hi" } });
    await sleep(20);
    expect(receivedA).toHaveLength(1);
    expect(receivedB).toHaveLength(1);
    expect(receivedB[0]).toMatchObject({ origin: "a" });
    await busA.close();
    await busB.close();
    vi.doUnmock("redis-orm-lite");
  });

  it("ignores malformed payloads and stops after unsubscribe", async () => {
    const { RedisEventBus, hub } = await mockedBusModule();
    const received: SSEBusEnvelope[] = [];
    const bus = new RedisEventBus({ channel: "test-malformed" });
    const unsubscribe = await bus.subscribe((e) => {
      received.push(e);
    });
    // Inject garbage straight into the channel, bypassing publish().
    for (const handler of hub.get("test-malformed") ?? []) {
      handler("not json");
      handler(JSON.stringify({ nope: true }));
    }
    await sleep(10);
    expect(received).toEqual([]);

    await bus.publish({ origin: "a", event: { data: "ok" } });
    await sleep(20);
    expect(received).toHaveLength(1);

    unsubscribe();
    await sleep(10);
    await bus.publish({ origin: "a", event: { data: "late" } });
    await sleep(20);
    expect(received).toHaveLength(1);
    await bus.close();
    vi.doUnmock("redis-orm-lite");
  });
});

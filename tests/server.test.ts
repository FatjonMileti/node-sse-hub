import { describe, expect, it, vi } from "vitest";
import { SSEClosedError, SSEError } from "../src/errors.js";
import { SSEServer } from "../src/SSEServer.js";
import { createMocks } from "./helpers.js";

function connectedServer(
  options: ConstructorParameters<typeof SSEServer>[0] = {},
) {
  const sse = new SSEServer(options);
  const mocks = [createMocks(), createMocks(), createMocks()].map((m) => {
    const connection = sse.connect(
      m.req.asIncomingMessage(),
      m.res.asServerResponse(),
    );
    return { ...m, connection };
  });
  return { sse, mocks };
}

describe("broadcast", () => {
  it("sends to all connections and returns the recipient count", async () => {
    const { sse, mocks } = connectedServer();
    const count = sse.broadcast({
      event: "notification",
      data: { message: "Hello everyone" },
    });
    expect(count).toBe(3);
    for (const m of mocks) {
      expect(m.res.body).toBe(
        'event: notification\ndata: {"message":"Hello everyone"}\n\n',
      );
    }
    await sse.close();
  });

  it("send() is an alias for broadcast()", async () => {
    const { sse, mocks } = connectedServer();
    expect(sse.send({ data: "hi" })).toBe(3);
    expect(mocks[0]?.res.body).toContain("data: hi");
    await sse.close();
  });

  it("returns 0 with no connections", async () => {
    const sse = new SSEServer();
    expect(sse.broadcast({ data: "nobody" })).toBe(0);
    await sse.close();
  });

  it("does not let one failing client block others", async () => {
    const sse = new SSEServer();
    const good1 = createMocks();
    const bad = createMocks({}, { throwOnWrite: true });
    const good2 = createMocks();
    sse.connect(good1.req.asIncomingMessage(), good1.res.asServerResponse());
    const badConn = sse.connect(
      bad.req.asIncomingMessage(),
      bad.res.asServerResponse(),
    );
    sse.connect(good2.req.asIncomingMessage(), good2.res.asServerResponse());

    const count = sse.broadcast({ data: "hi" });
    expect(count).toBe(2);
    expect(good1.res.body).toContain("data: hi");
    expect(good2.res.body).toContain("data: hi");
    expect(badConn.closed).toBe(true);
    await sse.close();
  });

  it("assigns automatic IDs when generateEventId is enabled", async () => {
    const { sse, mocks } = connectedServer({ generateEventId: true });
    sse.broadcast({ data: "a" });
    sse.broadcast({ data: "b" });
    expect(mocks[0]?.res.body).toContain("id: 1\ndata: a");
    expect(mocks[0]?.res.body).toContain("id: 2\ndata: b");
    await sse.close();
  });

  it("does not assign IDs by default", async () => {
    const { sse, mocks } = connectedServer();
    sse.broadcast({ data: "a" });
    expect(mocks[0]?.res.body).not.toContain("id:");
    await sse.close();
  });

  it("explicit IDs win over generated ones", async () => {
    const { sse, mocks } = connectedServer({ generateEventId: true });
    sse.broadcast({ id: "custom", data: "a" });
    expect(mocks[0]?.res.body).toContain("id: custom");
    await sse.close();
  });

  it("supports the retry field", async () => {
    const { sse, mocks } = connectedServer();
    sse.broadcast({ data: "a", retry: 5000 });
    expect(mocks[0]?.res.body).toContain("retry: 5000");
    await sse.close();
  });
});

describe("sendTo", () => {
  it("sends only to the targeted connection", async () => {
    const { sse, mocks } = connectedServer();
    const target = mocks[0]?.connection;
    if (target === undefined) throw new Error("test setup failed");
    const ok = sse.sendTo(target.id, {
      event: "private-message",
      data: { message: "Hello" },
    });
    expect(ok).toBe(true);
    expect(mocks[0]?.res.body).toContain("private-message");
    expect(mocks[1]?.res.body).not.toContain("private-message");
    expect(mocks[2]?.res.body).not.toContain("private-message");
    await sse.close();
  });

  it("returns false for unknown connection IDs", async () => {
    const sse = new SSEServer();
    expect(sse.sendTo("nope", { data: "x" })).toBe(false);
    await sse.close();
  });

  it("connection.send() delivers directly", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(conn.send({ event: "direct", data: "hi" })).toBe(true);
    expect(res.body).toContain("event: direct");
    await sse.close();
  });

  it("send on a closed connection returns false", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.close();
    expect(conn.send({ data: "x" })).toBe(false);
    await sse.close();
  });
});

describe("topics", () => {
  it("routes topic broadcasts only to subscribers", async () => {
    const sse = new SSEServer();
    const a = createMocks();
    const b = createMocks();
    const connA = sse.connect(
      a.req.asIncomingMessage(),
      a.res.asServerResponse(),
    );
    const connB = sse.connect(
      b.req.asIncomingMessage(),
      b.res.asServerResponse(),
    );
    connA.subscribe("invoices");
    connB.subscribe("notifications");

    const count = sse.to("invoices").broadcast({
      event: "invoice-created",
      data: { invoiceId: "123" },
    });
    expect(count).toBe(1);
    expect(a.res.body).toContain("invoice-created");
    expect(b.res.body).not.toContain("invoice-created");
    await sse.close();
  });

  it("supports multiple topics per connection", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.subscribe("invoices");
    conn.subscribe("notifications");
    expect(conn.getTopics().sort()).toEqual(["invoices", "notifications"]);
    expect(sse.getTopics().sort()).toEqual(["invoices", "notifications"]);
    await sse.close();
  });

  it("unsubscribe stops delivery", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.subscribe("invoices");
    expect(conn.unsubscribe("invoices")).toBe(true);
    expect(conn.unsubscribe("invoices")).toBe(false);
    expect(sse.to("invoices").broadcast({ data: "x" })).toBe(0);
    expect(res.body).not.toContain("data: x");
    await sse.close();
  });

  it("returns 0 for topics with no subscribers", async () => {
    const sse = new SSEServer();
    expect(sse.to("ghost").broadcast({ data: "x" })).toBe(0);
    await sse.close();
  });

  it("removes disconnected clients from topics", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.subscribe("invoices");
    expect(sse.getTopicSubscriberCount("invoices")).toBe(1);
    conn.close();
    expect(sse.getTopicSubscriberCount("invoices")).toBe(0);
    expect(sse.getTopics()).toEqual([]);
    expect(sse.getTopicSubscribers("invoices")).toEqual([]);
    await sse.close();
  });

  it("getTopicSubscribers returns the subscribed connections", async () => {
    const sse = new SSEServer();
    const a = createMocks();
    const b = createMocks();
    const connA = sse.connect(
      a.req.asIncomingMessage(),
      a.res.asServerResponse(),
    );
    sse.connect(b.req.asIncomingMessage(), b.res.asServerResponse());
    connA.subscribe("invoices");
    expect(sse.getTopicSubscribers("invoices")).toEqual([connA]);
    await sse.close();
  });

  it("rejects invalid topics and enforces the per-connection limit", async () => {
    const sse = new SSEServer({ maxTopicsPerConnection: 1 });
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(() => {
      conn.subscribe("");
    }).toThrow(SSEError);
    conn.subscribe("one");
    expect(() => {
      conn.subscribe("two");
    }).toThrow(SSEError);
    await sse.close();
  });
});

describe("lifecycle events", () => {
  it("emits connection, disconnect, and error events", async () => {
    const sse = new SSEServer();
    const onConnection = vi.fn();
    const onDisconnect = vi.fn();
    const onError = vi.fn();
    sse.on("connection", onConnection);
    sse.onDisconnect(onDisconnect);
    sse.onError(onError);

    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(onConnection).toHaveBeenCalledWith(conn, {});
    res.simulateClientClose();
    expect(onDisconnect).toHaveBeenCalledWith(conn);
    // Emitting an error surfaces through the typed listener.
    const err = new Error("boom");
    sse.emit("error", err);
    expect(onError).toHaveBeenCalledWith(err);
    await sse.close();
  });

  it("once() and off() are typed and functional", async () => {
    const sse = new SSEServer();
    const listener = vi.fn();
    sse.once("disconnect", listener);
    const a = createMocks();
    const connA = sse.connect(
      a.req.asIncomingMessage(),
      a.res.asServerResponse(),
    );
    connA.close();
    const b = createMocks();
    const connB = sse.connect(
      b.req.asIncomingMessage(),
      b.res.asServerResponse(),
    );
    connB.close();
    expect(listener).toHaveBeenCalledTimes(1);

    const other = vi.fn();
    sse.on("disconnect", other);
    sse.off("disconnect", other);
    const c = createMocks();
    sse.connect(c.req.asIncomingMessage(), c.res.asServerResponse()).close();
    expect(other).not.toHaveBeenCalled();
    await sse.close();
  });
});

describe("shutdown", () => {
  it("close() shuts down connections and blocks new ones", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sse.close();
    expect(conn.closed).toBe(true);
    expect(sse.closed).toBe(true);
    expect(sse.connectionCount).toBe(0);
    expect(sse.getStats()).toMatchObject({ connections: 0, closed: true });

    const m = createMocks();
    expect(() =>
      sse.connect(m.req.asIncomingMessage(), m.res.asServerResponse()),
    ).toThrow(SSEClosedError);
    expect(sse.broadcast({ data: "x" })).toBe(0);
  });

  it("repeated close() calls are safe", async () => {
    const sse = new SSEServer();
    await sse.close();
    await sse.close();
    await sse.close();
    expect(sse.closed).toBe(true);
  });
});

describe("validation and limits", () => {
  it("rejects malformed events", async () => {
    const sse = new SSEServer();
    expect(() => sse.broadcast("nope" as never)).toThrow(SSEError);
    expect(() => sse.broadcast({ id: "a\nb", data: "x" })).toThrow(SSEError);
    await sse.close();
  });

  it("rejects oversized frames", async () => {
    const sse = new SSEServer({ maxEventBytes: 10 });
    expect(() => sse.broadcast({ data: "way too long payload" })).toThrow(
      SSEError,
    );
    await sse.close();
  });

  it("rejects invalid constructor options", () => {
    expect(() => new SSEServer({ maxBufferedEvents: 0 })).toThrow(SSEError);
    expect(() => new SSEServer({ heartbeatInterval: -1 })).toThrow(SSEError);
    expect(
      () => new SSEServer({ history: { enabled: true, maxEvents: 0 } }),
    ).toThrow(SSEError);
  });

  it("supports a custom serializer", async () => {
    const sse = new SSEServer({
      serialize: (data) => `ser:${JSON.stringify(data)}`,
    });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    sse.broadcast({ data: { a: 1 } });
    expect(res.body).toContain("data: ser:");
    await sse.close();
  });

  it("getStats reports connections, topics, and history", async () => {
    const sse = new SSEServer({ history: { enabled: true } });
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.subscribe("t");
    sse.broadcast({ data: "x" });
    expect(sse.getStats()).toMatchObject({
      connections: 1,
      topics: 1,
      historySize: 1,
      closed: false,
    });
    await sse.close();
  });
});

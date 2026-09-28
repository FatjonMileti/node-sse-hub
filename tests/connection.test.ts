import { describe, expect, it, vi } from "vitest";
import { SSEServer } from "../src/SSEServer.js";
import { createMocks } from "./helpers.js";

describe("connection establishment", () => {
  it("sets the correct SSE headers", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(res.statusCode).toBe(200);
    expect(res.responseHeaders["Content-Type"]).toBe("text/event-stream");
    expect(res.responseHeaders["Cache-Control"]).toBe("no-cache");
    expect(res.responseHeaders["Connection"]).toBe("keep-alive");
    expect(res.responseHeaders["X-Accel-Buffering"]).toBe("no");
    expect(res.headersFlushed).toBe(true);
    await sse.close();
  });

  it("assigns unique connection IDs", async () => {
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
    expect(connA.id).not.toBe(connB.id);
    expect(sse.connectionCount).toBe(2);
    expect(sse.getConnections()).toHaveLength(2);
    await sse.close();
  });

  it("exposes connections via getConnection()", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(sse.getConnection(conn.id)).toBe(conn);
    expect(sse.getConnection("missing")).toBeUndefined();
    await sse.close();
  });

  it("supports multiple simultaneous connections", async () => {
    const sse = new SSEServer();
    const conns = [];
    const mocks = [];
    for (let i = 0; i < 5; i++) {
      const m = createMocks();
      mocks.push(m);
      conns.push(
        sse.connect(m.req.asIncomingMessage(), m.res.asServerResponse()),
      );
    }
    expect(sse.broadcast({ data: "hi" })).toBe(5);
    for (const m of mocks) {
      expect(m.res.body).toContain("data: hi");
    }
    await sse.close();
  });

  it("supports initial topics via connect options", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse(), {
      topics: ["news"],
    });
    expect(conn.getTopics()).toEqual(["news"]);
    expect(sse.getTopicSubscriberCount("news")).toBe(1);
    await sse.close();
  });

  it("exposes Last-Event-ID in the connection context", async () => {
    const sse = new SSEServer();
    const listener = vi.fn();
    sse.onConnection(listener);
    const { req, res } = createMocks({ "last-event-id": "event-123" });
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(conn.lastEventId).toBe("event-123");
    expect(listener).toHaveBeenCalledOnce();
    const [, context] = listener.mock.calls[0] as unknown as [
      unknown,
      { lastEventId?: string },
    ];
    expect(context.lastEventId).toBe("event-123");
    await sse.close();
  });

  it("rejects connections beyond maxConnections with 503", async () => {
    const sse = new SSEServer({ maxConnections: 1 });
    const a = createMocks();
    sse.connect(a.req.asIncomingMessage(), a.res.asServerResponse());
    const b = createMocks();
    expect(() =>
      sse.connect(b.req.asIncomingMessage(), b.res.asServerResponse()),
    ).toThrow(/connection limit/i);
    expect(b.res.statusCode).toBe(503);
    await sse.close();
  });
});

describe("disconnect handling", () => {
  it("cleans up when the client closes the response", async () => {
    const sse = new SSEServer();
    const onDisconnect = vi.fn();
    sse.on("disconnect", onDisconnect);
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    res.simulateClientClose();
    expect(conn.closed).toBe(true);
    expect(sse.getConnection(conn.id)).toBeUndefined();
    expect(sse.connectionCount).toBe(0);
    expect(onDisconnect).toHaveBeenCalledWith(conn);
    await sse.close();
  });

  it("cleans up when the request aborts", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    req.emit("close");
    expect(conn.closed).toBe(true);
    expect(sse.connectionCount).toBe(0);
    await sse.close();
  });

  it("cleanup is idempotent across duplicate close events", async () => {
    const sse = new SSEServer();
    const onDisconnect = vi.fn();
    sse.on("disconnect", onDisconnect);
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    res.simulateClientClose();
    res.simulateClientClose();
    req.emit("close");
    conn.close();
    expect(onDisconnect).toHaveBeenCalledTimes(1);
    await sse.close();
  });

  it("disconnect(id) closes and returns true; unknown IDs return false", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    expect(sse.disconnect(conn.id)).toBe(true);
    expect(conn.closed).toBe(true);
    expect(sse.disconnect(conn.id)).toBe(false);
    expect(sse.disconnect("missing-id")).toBe(false);
    await sse.close();
  });

  it("connection.close() ends the response", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.close();
    expect(res.writableEnded).toBe(true);
    await sse.close();
  });
});

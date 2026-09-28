import { describe, expect, it } from "vitest";
import { SSEServer } from "../src/SSEServer.js";
import { createMocks, sleep } from "./helpers.js";

describe("heartbeats", () => {
  it("sends heartbeat comments on the configured interval", async () => {
    const sse = new SSEServer({ heartbeatInterval: 20 });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(75);
    const heartbeats = res.chunks.filter((c) => c.startsWith(":"));
    expect(heartbeats.length).toBeGreaterThanOrEqual(2);
    expect(heartbeats[0]).toBe(": heartbeat\n\n");
    await sse.close();
  });

  it("uses a custom heartbeat comment", async () => {
    const sse = new SSEServer({
      heartbeatInterval: 20,
      heartbeatComment: "ping",
    });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(50);
    expect(res.chunks.some((c) => c === ": ping\n\n")).toBe(true);
    await sse.close();
  });

  it("does not send heartbeats when disabled", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(40);
    expect(res.chunks).toHaveLength(0);
    await sse.close();
  });

  it("stops heartbeat timers on close (no writes after shutdown)", async () => {
    const sse = new SSEServer({ heartbeatInterval: 15 });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(40);
    expect(res.chunks.length).toBeGreaterThan(0);
    await sse.close();
    const countAfterClose = res.chunks.length;
    await sleep(50);
    expect(res.chunks.length).toBe(countAfterClose);
  });

  it("heartbeat frames are not exposed as application events", async () => {
    const sse = new SSEServer({ heartbeatInterval: 15 });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(40);
    expect(res.body).not.toContain("event:");
    expect(res.body).not.toContain("data:");
    await sse.close();
  });
});

describe("backpressure / slow clients", () => {
  it("buffers frames while the socket applies backpressure", async () => {
    const sse = new SSEServer();
    const { req, res } = createMocks({}, { writeReturn: false });
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    // First write returns false → socket buffer full, but accepted.
    expect(conn.send({ data: "one" })).toBe(true);
    // Subsequent frames queue up.
    expect(conn.send({ data: "two" })).toBe(true);
    expect(conn.send({ data: "three" })).toBe(true);
    expect(conn.bufferedCount).toBe(2);
    expect(conn.hasBackpressure).toBe(true);
    // Drain flushes the queue (one frame per drain while the socket
    // stays full, mirroring real TCP backpressure behavior).
    res.simulateDrain();
    expect(conn.bufferedCount).toBe(1);
    res.simulateDrain();
    expect(conn.bufferedCount).toBe(0);
    expect(res.body).toContain("data: one");
    expect(res.body).toContain("data: two");
    expect(res.body).toContain("data: three");
    await sse.close();
  });

  it("disconnects slow clients by default when the buffer overflows", async () => {
    const sse = new SSEServer({ maxBufferedEvents: 2 });
    const { req, res } = createMocks({}, { writeReturn: false });
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.send({ data: "one" }); // fills socket buffer
    conn.send({ data: "two" }); // queued (1)
    conn.send({ data: "three" }); // queued (2, at limit)
    const accepted = conn.send({ data: "four" }); // overflow → disconnect
    expect(accepted).toBe(false);
    expect(conn.closed).toBe(true);
    expect(conn.isSlow).toBe(true);
    expect(sse.getConnection(conn.id)).toBeUndefined();
    await sse.close();
  });

  it("drop-oldest strategy discards queued frames instead of disconnecting", async () => {
    const sse = new SSEServer({
      maxBufferedEvents: 1,
      slowClientStrategy: "drop-oldest",
    });
    const { req, res } = createMocks({}, { writeReturn: false });
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.send({ data: "one" });
    conn.send({ data: "two" });
    expect(conn.send({ data: "three" })).toBe(true);
    expect(conn.closed).toBe(false);
    expect(conn.droppedFrames).toBe(1);
    res.simulateDrain();
    // "two" was dropped to make room for "three".
    expect(res.body).toContain("data: one");
    expect(res.body).not.toContain("data: two");
    expect(res.body).toContain("data: three");
    await sse.close();
  });

  it("broadcasts isolate slow clients and still reach healthy ones", async () => {
    const sse = new SSEServer({
      maxBufferedEvents: 1,
      slowClientStrategy: "drop-oldest",
    });
    const slow = createMocks({}, { writeReturn: false });
    const healthy = createMocks();
    sse.connect(slow.req.asIncomingMessage(), slow.res.asServerResponse());
    sse.connect(
      healthy.req.asIncomingMessage(),
      healthy.res.asServerResponse(),
    );
    const count = sse.broadcast({ data: "hi" });
    expect(count).toBe(2);
    expect(healthy.res.body).toContain("data: hi");
    await sse.close();
  });
});

import { describe, expect, it } from "vitest";
import { SSEServer } from "../src/SSEServer.js";
import { createMocks } from "./helpers.js";

describe("event history", () => {
  it("stores broadcast events up to maxEvents", async () => {
    const sse = new SSEServer({
      history: { enabled: true, maxEvents: 2 },
    });
    createMocks();
    const m = createMocks();
    sse.connect(m.req.asIncomingMessage(), m.res.asServerResponse());
    sse.broadcast({ data: "1" });
    sse.broadcast({ data: "2" });
    sse.broadcast({ data: "3" });
    expect(sse.getStats().historySize).toBe(2);
    await sse.close();
  });

  it("replays events after Last-Event-ID on reconnect", async () => {
    const sse = new SSEServer({
      history: { enabled: true, maxEvents: 100 },
      generateEventId: true,
    });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ data: "one" }); // id 1
    sse.broadcast({ data: "two" }); // id 2
    sse.broadcast({ data: "three" }); // id 3

    // Reconnect with Last-Event-ID: 1 → should receive 2 and 3 only.
    const second = createMocks({ "last-event-id": "1" });
    sse.connect(second.req.asIncomingMessage(), second.res.asServerResponse());
    expect(second.res.body).toContain("id: 2\ndata: two");
    expect(second.res.body).toContain("id: 3\ndata: three");
    expect(second.res.body).not.toContain("data: one");
    await sse.close();
  });

  it("replays nothing when the ID is unknown or history is disabled", async () => {
    const sse = new SSEServer({
      history: { enabled: true, maxEvents: 100 },
      generateEventId: true,
    });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ data: "one" });

    const unknown = createMocks({ "last-event-id": "999" });
    sse.connect(
      unknown.req.asIncomingMessage(),
      unknown.res.asServerResponse(),
    );
    expect(unknown.res.body).toBe("");

    const plain = new SSEServer();
    const p = createMocks();
    plain.connect(p.req.asIncomingMessage(), p.res.asServerResponse());
    plain.broadcast({ data: "x" });
    const reconnect = createMocks({ "last-event-id": "1" });
    plain.connect(
      reconnect.req.asIncomingMessage(),
      reconnect.res.asServerResponse(),
    );
    expect(reconnect.res.body).toBe("");
    expect(plain.getStats().historySize).toBe(0);

    await sse.close();
    await plain.close();
  });

  it("assigns IDs for history even when generateEventId is false", async () => {
    const sse = new SSEServer({ history: { enabled: true, maxEvents: 10 } });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ data: "one" });
    // Auto-assigned id "1" for history positioning.
    expect(first.res.body).toContain("id: 1");

    const second = createMocks({ "last-event-id": "1" });
    sse.connect(second.req.asIncomingMessage(), second.res.asServerResponse());
    expect(second.res.body).toBe("");
    await sse.close();
  });

  it("supports non-numeric explicit IDs in replay", async () => {
    const sse = new SSEServer({ history: { enabled: true, maxEvents: 10 } });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ id: "evt-a", data: "a" });
    sse.broadcast({ id: "evt-b", data: "b" });

    const second = createMocks({ "last-event-id": "evt-a" });
    sse.connect(second.req.asIncomingMessage(), second.res.asServerResponse());
    expect(second.res.body).toContain("id: evt-b");
    expect(second.res.body).not.toContain("id: evt-a");
    await sse.close();
  });

  it("supports a custom history store", async () => {
    const stored: { id: string; frame: string }[] = [];
    const customStore = {
      get size() {
        return stored.length;
      },
      add: (entry: { id: string; frame: string }) => {
        stored.push(entry);
      },
      getAfter: (id: string) => {
        const idx = stored.findIndex((e) => e.id === id);
        return idx === -1 ? [] : stored.slice(idx + 1);
      },
      clear: () => {
        stored.length = 0;
      },
    };
    const sse = new SSEServer({
      history: { enabled: true, store: customStore },
      generateEventId: true,
    });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ data: "one" });
    sse.broadcast({ data: "two" });
    expect(stored).toHaveLength(2);

    const second = createMocks({ "last-event-id": "1" });
    sse.connect(second.req.asIncomingMessage(), second.res.asServerResponse());
    expect(second.res.body).toContain("data: two");
    await sse.close();
  });

  it("direct sendTo messages are not replayed (documented global-history rule)", async () => {
    const sse = new SSEServer({
      history: { enabled: true, maxEvents: 10 },
      generateEventId: true,
    });
    const first = createMocks();
    const conn = sse.connect(
      first.req.asIncomingMessage(),
      first.res.asServerResponse(),
    );
    sse.sendTo(conn.id, { data: "private" });
    expect(sse.getStats().historySize).toBe(0);
    await sse.close();
  });
});

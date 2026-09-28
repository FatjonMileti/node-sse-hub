/**
 * Redis-backed SSE example for `sse-kit` (invoice flow, native HTTP).
 *
 * Architecture:
 *
 *   HTTP client
 *       ↓  GET /events (EventSource, auto-reconnects with Last-Event-ID)
 *   sse-kit (SSEServer)
 *       ↓  history persistence (fire-and-forget, live-first)
 *   RedisEventStore (sse-kit/redis)
 *       ↓  documents via RedisModel, ordering via INCR + sorted set,
 *          retries via redis-orm-lite → node-retry-kit
 *   redis-orm-lite
 *       ↓
 *   Redis
 *
 * Run:
 *   1. docker run -p 6379:6379 redis:7
 *   2. npm install sse-kit redis-orm-lite
 *   3. node --loader ts-node/esm examples/redis/server.ts
 *      (or compile first and run the emitted JS)
 *
 * Try it:
 *   curl -N http://localhost:3000/events &
 *   curl -X POST http://localhost:3000/invoices \
 *     -H 'Content-Type: application/json' \
 *     -d '{"id":"INV-123","total":150}'
 *   # Kill the first curl, note the last id, reconnect with it:
 *   curl -N -H 'Last-Event-ID: 1' http://localhost:3000/events
 */
import { createServer } from "node:http";
import { SSEServer } from "../../src/index.js";
import { RedisEventBus, RedisEventStore } from "../../src/redis.js";

const store = new RedisEventStore({
  url: process.env["REDIS_URL"] ?? "redis://localhost:6379",
  keyPrefix: "sse-kit:example",
  maxEvents: 1000,
  retry: {
    retries: 5,
    backoff: "exponential",
    delay: 500,
    maxDelay: 10_000,
    jitter: true,
  },
});

// Optional: share live events with other Node instances via Redis Pub/Sub.
// Omit `bus` entirely for a single-process deployment.
const bus = new RedisEventBus({
  url: process.env["REDIS_URL"] ?? "redis://localhost:6379",
  channel: "sse-kit:example:bus",
});

const sse = new SSEServer({
  heartbeatInterval: 30_000,
  history: { enabled: true, store },
  bus,
  nodeId: process.env["NODE_ID"] ?? "node-1",
});

// Persistence must never take down live streaming.
sse.onStorageError((error) => {
  console.error("[storageError]", error);
});
sse.onBusError((error) => {
  console.error("[busError]", error);
});

const server = createServer((req, res) => {
  if (req.url === "/events" && req.method === "GET") {
    // Authenticate/authorize here in real apps, before connecting.
    const connection = sse.connect(req, res);
    connection.subscribe("invoices");
    return;
  }

  if (req.url === "/invoices" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk: Buffer) => {
      body += chunk.toString();
    });
    req.on("end", () => {
      try {
        const invoice = JSON.parse(body) as {
          id?: string;
          total?: number;
        };
        const count = sse.to("invoices").broadcast({
          event: "invoice-created",
          data: {
            invoiceId: invoice.id ?? "INV-unknown",
            total: invoice.total ?? 0,
          },
        });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ delivered: count }));
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "invalid JSON" }));
      }
    });
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not found");
});

server.listen(3000, () => {
  console.log("SSE+Redis example on http://localhost:3000/events");
});

async function shutdown(signal: string): Promise<void> {
  console.log(`Received ${signal}, shutting down…`);
  await sse.flushHistory();
  await sse.close();
  await store.close();
  server.close();
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

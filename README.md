# node-sse-hub

A lightweight, framework-friendly **Server-Sent Events (SSE)** library for Node.js.

Create SSE streams, send events to individual clients, broadcast to everyone, publish to topics, replay missed events after reconnects, and shut down gracefully — with strong TypeScript types and zero runtime dependencies.

## Why use it?

- **Hub, not sessions** — one `SSEServer` tracks every connection by ID: broadcast with recipient counts, message one client by ID, disconnect by ID. No manual session bookkeeping.
- **Framework-friendly** — works with Express, Fastify, Node's native HTTP server, or anything exposing Node `IncomingMessage` / `ServerResponse`. No framework dependencies.
- **Correct SSE** — spec-compliant framing, multiline data, `retry`, comments, and `Last-Event-ID` handling.
- **Topics** — subscribe connections to rooms and broadcast per-room, with topic-tagged history.
- **Replay** — pluggable async event-history stores (memory built in, Redis adapter included) replay missed events to reconnecting clients.
- **Multi-node ready** — optional Redis Pub/Sub bus fans broadcasts out across Node instances.
- **Reliable** — heartbeat keep-alives, backpressure strategies for slow clients, connection/payload caps, storage and bus error hooks, idempotent cleanup, graceful shutdown.
- **Small** — zero required runtime dependencies, strict TypeScript, ESM + CommonJS builds.

## How is it different from better-sse?

[`better-sse`](https://github.com/MatthewWid/better-sse) is an excellent, mature, session-centric library: you create a session per request, hold the reference, and `push()` to it (plus channels, batching, and stream/iterable piping, with Fetch-API support for edge runtimes). Choose it for per-request streaming pipelines and non-Node runtimes.

`node-sse-hub` takes the opposite, hub-centric shape and targets Node.js servers that need operational control:

| Concern                 | better-sse                                   | node-sse-hub                                                              |
| ----------------------- | -------------------------------------------- | ------------------------------------------------------------------------- |
| Model                   | Sessions you hold and push to                | Central registry; address clients by ID                                   |
| Broadcast result        | Fire-and-forget per session                  | Returns recipient count                                                   |
| Rooms                   | Channels (register sessions)                 | Topics + per-topic broadcast + topic-tagged history                       |
| Missed-event replay     | Exposes/trusts `Last-Event-ID` for app logic | Server-side replay from pluggable stores (memory, Redis, custom)          |
| Persistence             | None built in                                | `RedisEventStore` with idempotent appends, global sequencing              |
| Multi-node live fan-out | Not in scope                                 | Optional Redis Pub/Sub `SSEEventBus`                                      |
| Slow consumers          | Backpressure via streams                     | Bounded per-connection queue with `disconnect` / `drop-oldest` strategies |
| Failure hooks           | Session errors                               | `storageError` / `busError` without breaking live delivery                |
| Runtimes                | Node + Fetch API (Bun, Deno, edge)           | Node.js (`IncomingMessage` / `ServerResponse`)                            |

## Installation

```bash
npm install node-sse-hub
```

Requires Node.js 18+.

## Quick start

```ts
import { SSEServer } from "node-sse-hub";

const sse = new SSEServer({ heartbeatInterval: 30_000 });

// …inside your route handler:
sse.connect(req, res);

// …anywhere else in your app:
sse.broadcast({ event: "ping", data: { ok: true } });
```

## Express example

```ts
import express from "express";
import { SSEServer } from "node-sse-hub";

const sse = new SSEServer({ heartbeatInterval: 30_000 });
const app = express();

app.get("/events", (req, res) => {
  sse.connect(req, res);
});

app.listen(3000);
```

See [`examples/express.ts`](examples/express.ts).

### Express response-lifecycle contract

`connect()` takes over the response socket, so the route must give it a pristine response:

- **Headers must not be sent yet.** Don't set headers, write, or end the response before calling `connect()` — the hub writes the `200` + SSE headers itself.
- **No competing response finishers.** Don't call `next()` on to handlers/middleware that `res.send()`/`res.end()`, and don't end the response after `connect()` returns. Disconnects are detected via request abort / response close / write failures, and cleanup is idempotent.
- **No body parsing needed.** SSE streams are GET requests without bodies; body-parser middleware on the route is unnecessary (harmless, but pointless).
- **Disable compression for the route.** Response compression buffers the stream and defeats SSE flushing — exclude the SSE route from `compression()` (or any buffering proxy config; the hub already sends `X-Accel-Buffering: no` for nginx).
- **One `connect()` per response.** Calling `connect()` twice on the same response throws (`ERR_HTTP_HEADERS_SENT` from the second `writeHead`); no second connection is registered. If you need per-client setup (greeting event, topic subscriptions), do it with the returned connection inside the same handler.
- **Auth first.** Express 4's `req`/`res` are native HTTP objects, so they pass straight through — run your auth middleware (cookies, signed URLs) _before_ the `connect()` call; the hub deliberately has no auth.

## Native Node example

```ts
import { createServer } from "node:http";
import { SSEServer } from "node-sse-hub";

const sse = new SSEServer({ heartbeatInterval: 30_000 });

const server = createServer((req, res) => {
  if (req.url === "/events") {
    sse.connect(req, res);
    return;
  }
  res.writeHead(404).end();
});

server.listen(3000);
```

See [`examples/native-http.ts`](examples/native-http.ts). A Fastify example (using raw `request.raw` / `reply.raw` with `reply.hijack()`) is in [`examples/fastify.ts`](examples/fastify.ts).

## Sending events

```ts
// Object payloads are JSON-serialized by default:
sse.send({
  event: "message",
  data: { message: "Hello" },
});
// → event: message
// → data: {"message":"Hello"}

// Strings are sent as-is:
sse.send({ event: "message", data: "Hello" });
// → event: message
// → data: Hello

// Multiline data is split into multiple `data:` lines automatically:
sse.send({ data: "first line\nsecond line" });
// → data: first line
// → data: second line

// Retry hint (reconnection time in ms):
sse.send({ data: "hi", retry: 5000 });
// → retry: 5000
// → data: hi
```

`send()` is an alias for `broadcast()`.

### Custom serializer

```ts
const sse = new SSEServer({
  serialize(data) {
    return JSON.stringify(data);
  },
});
```

## Broadcasting

```ts
const count = sse.broadcast({
  event: "notification",
  data: { message: "Hello everyone" },
});
console.log(`Delivered to ${count} clients`);
```

`broadcast()` sends to every connected client and returns the number of clients that accepted the event. One slow or broken client never blocks the others.

### Excluding connections (optimistic UI)

When the sender already applied its own change locally, echoing the event back is wasteful — pass `exceptConnectionIds` instead of hand-rolling a `getConnections()` + `sendTo()` loop:

```ts
app.get("/events/:userId", (req, res) => {
  const connection = sse.connect(req, res);
  // Remember which connection belongs to whom:
  userConnections.set(req.params.userId, connection.id);
  sse.send({ event: "connected", data: { userId: req.params.userId } });
});

app.post("/orders/:userId", (req, res) => {
  const senderId = userConnections.get(req.params.userId);
  sse.broadcast(
    { event: "order-updated", data: { orderId: "7" } },
    { exceptConnectionIds: senderId === undefined ? [] : [senderId] },
  );
  res.json({ ok: true });
});
```

Unlike `sendTo`, an excluded broadcast **still writes history and publishes to the bus** — the event is global, only its delivery is scoped (unknown IDs are ignored, and the exclusion list travels in the bus envelope so other nodes honor it too). The returned count excludes skipped connections. One caveat: history is global, so a reconnecting _excluded_ client may receive the event via `Last-Event-ID` replay — keep consumers idempotent.

## Individual connections

Every connection gets a unique ID:

```ts
const connection = sse.connect(req, res);
console.log(connection.id);

sse.sendTo(connection.id, {
  event: "private-message",
  data: { message: "Hello" },
}); // → true if delivered, false if unknown/closed

sse.disconnect(connection.id); // → true if a client was disconnected
sse.getConnection(connection.id); // → SSEConnection | undefined
```

`disconnect()` on an unknown ID safely returns `false` instead of throwing.

## Topics

```ts
const connection = sse.connect(req, res);
connection.subscribe("invoices");
connection.subscribe("notifications");

sse.to("invoices").broadcast({
  event: "invoice-created",
  data: { invoiceId: "123" },
});

connection.unsubscribe("invoices");

sse.getTopicSubscribers("invoices"); // SSEConnection[]
sse.getTopicSubscriberCount("invoices"); // number
sse.getTopics(); // topics with ≥1 subscriber
```

Disconnected clients are removed from all topics automatically. See [`examples/topics.ts`](examples/topics.ts).

## Last-Event-ID

Browsers automatically reconnect dropped `EventSource` streams and send the last received event ID:

```http
Last-Event-ID: event-123
```

Your app can observe it on every connection:

```ts
sse.onConnection((connection, context) => {
  console.log(context.lastEventId); // "event-123" | undefined
});
```

`node-sse-hub` does **not** pretend to replay events it doesn't have: replay only happens when `history.enabled` is `true` (below). Without history, `lastEventId` is exposed so your application can implement its own catch-up logic (e.g. query a database).

## Event replay / history

```ts
const sse = new SSEServer({
  history: { enabled: true, maxEvents: 1000 },
});
```

When a client reconnects with `Last-Event-ID: 50`, the server replays buffered events stored after ID `50`.

How IDs work:

- Explicit IDs (`send({ id: "event-123", … })`) always win.
- With `generateEventId: true`, every ID-less event gets an incrementing numeric ID (`"1"`, `"2"`, …).
- With `history.enabled: true`, ID-less events are **also** auto-assigned incrementing IDs so replay positioning works, even if `generateEventId` is `false`.

Limitations (by design):

- Matching is by **exact string equality** — IDs need not be numeric.
- If the client's ID is **unknown** (never existed or already evicted from the bounded buffer), **nothing is replayed** rather than the whole buffer, to avoid duplicate storms after long disconnections.
- History is a **single global log**: replay is positional, not filtered by topic or original audience. Direct `sendTo()` messages are **not** stored in history (so private messages can't leak to whoever reconnects next), but topic broadcasts are. Apps needing per-topic replay should use separate `SSEServer` instances or filter client-side.
- No external store is bundled (no Redis dependency). Implement `SSEHistoryStore` to persist elsewhere:

```ts
import type { SSEHistoryStore } from "node-sse-hub";

const sse = new SSEServer({
  history: { enabled: true, maxEvents: 1000, store: myStore },
});
```

### Async event stores

For out-of-process backends, implement the async interface instead:

```ts
import type { SSEEventStore, StoredSSEEvent } from "node-sse-hub";

const store: SSEEventStore = {
  append: (event: StoredSSEEvent) => ...,
  getAfter: (lastEventId: string) => ...,
  getRecent: (limit: number) => ...,
  clear: () => ...,
};

const sse = new SSEServer({ history: { enabled: true, store } });
```

The built-in `MemoryEventStore` is the reference implementation:

```ts
const sse = new SSEServer({
  history: {
    enabled: true,
    store: new MemoryEventStore({ maxEvents: 1000 }),
  },
});
```

Two behavioral notes for async stores: persistence runs through a serialized queue (order-preserving) and reconnect replay resolves shortly after `connect()` returns, so events broadcast concurrently with a reconnect can arrive before replayed history — clients should order/dedupe by `id`. `await sse.flushHistory()` waits for all enqueued writes to settle. Failures surface via `sse.onStorageError(...)` and never break live delivery.

## Redis integration

Memory history is simple and single-process. Redis history gives you persistent, shared storage: multiple application instances (or restarts) can access the same event history.

```bash
npm install node-sse-hub redis-orm-lite
```

```ts
import { SSEServer } from "node-sse-hub";
import { RedisEventStore } from "node-sse-hub/redis";

const store = new RedisEventStore({
  url: "redis://localhost:6379",
  keyPrefix: "node-sse-hub", // isolates this app's keys
  maxEvents: 10_000, // oldest-first eviction, managed by the store
  retry: {
    retries: 5,
    backoff: "exponential",
    delay: 500,
    maxDelay: 10_000,
    jitter: true,
  },
});

const sse = new SSEServer({ history: { enabled: true, store } });

sse.onStorageError((error) => {
  console.error("history persistence failed (live delivery unaffected)", error);
});
```

How it works (no ORM knowledge required — the adapter hides it):

- Event documents are stored with `redis-orm-lite`'s `RedisModel` under `<prefix>:events:<eventId>` as whole-document overwrites, so re-appending the same ID is idempotent — retries can never create duplicate logical events.
- Ordering never relies on sorting IDs (UUIDs don't sort chronologically). Each append takes a Redis-side atomic `INCR <prefix>:seq` sequence, and an index sorted set (`<prefix>:index`, member = event ID, score = sequence) makes replay a positional lookup.
- `getAfter(lastEventId)` resolves the anchor's sequence, then reads everything scored after it. Unknown anchors replay nothing, same as memory history.
- `data` payloads must be JSON-serializable (they round-trip through Redis).
- `close()` on the server intentionally leaves a Redis store intact (it may be shared); call `store.close()` yourself on shutdown, and `store.clear()` only when you really mean to wipe history.

Consistency model (broadcast-first, persist-after): `broadcast()` delivers to live clients synchronously and returns the recipient count, then persists asynchronously. A Redis outage therefore never fails or delays live delivery — the trade-off is a small crash window where a delivered event isn't yet persisted. If you need write-through instead, `await store.append(...)` yourself before broadcasting, or `await sse.flushHistory()` at shutdown.

## Retry integration

Retries come from `redis-orm-lite`, which builds on `node-retry-kit` — `node-sse-hub` never re-implements retry logic and you don't need `node-retry-kit` installed directly:

```bash
npm install node-sse-hub redis-orm-lite   # node-retry-kit arrives transitively
```

Pass a retry policy to `RedisEventStore` and it is forwarded (per-operation, never global) to every Redis round-trip:

```ts
new RedisEventStore({
  retry: {
    retries: 5, // retries after the first attempt; 0/absent = single attempt
    backoff: "exponential", // or "fixed"
    delay: 500, // base delay in ms
    maxDelay: 10_000, // cap in ms
    jitter: true, // avoid thundering herds
    timeout: 2000, // per-attempt timeout in ms (optional)
    reads: true, // retry reads (default true)
    writes: true, // retry writes — safe: whole-doc SET/DEL are idempotent
  },
});
```

Only transient failures retry (connection resets, timeouts, `TRYAGAIN`/`LOADING`/`BUSY` server states, classified by `redis-orm-lite`'s `isTransientRedisError`). Permanent errors (wrong password, `WRONGTYPE`, bad commands) and aborts never retry. Without a `retry` policy every command runs exactly once.

## Distributed systems: persistence ≠ live broadcast

Important limitation: Redis history alone does **not** make live broadcasting distributed. If a client is connected to Node A and an event is broadcast via Node B's `sse.broadcast(...)`, Node A's clients won't see it — each server only knows its own sockets.

For multi-node fan-out, attach a bus:

```ts
import { RedisEventBus } from "node-sse-hub/redis";

const sse = new SSEServer({
  bus: new RedisEventBus({ channel: "my-app:bus" }),
  nodeId: "node-a", // defaults to a random UUID
});
```

Every broadcast is then also published to the bus; each subscribed node delivers it to its local clients (loop-safe via the envelope's `origin`, topic-aware for `to(...)` broadcasts). Remote events are recorded in each node's local history. Bus failures surface via `sse.onBusError(...)` and never break local delivery.

```text
              Redis Pub/Sub
                    │
        ┌───────────┴───────────┐
        ↓                       ↓
     Node A                  Node B
        ↓                       ↓
   SSE clients              SSE clients
```

The core package only defines the `SSEEventBus` interface (`publish`/`subscribe`/`close`) — the Redis transport is optional, and custom transports (NATS, Postgres `LISTEN`, …) can implement the same interface. Without a bus, use sticky sessions so `Last-Event-ID` reconnects land on the node holding the relevant history (or share one Redis store, since history lookups work from any node).

> **Packaging note:** `node-sse-hub/redis` uses static imports of the
> `redis-orm-lite` public API, so it loads under bundlers, Vitest, and
> CommonJS (`require("node-sse-hub/redis")`) everywhere. Pure-Node ESM
> (`import … from "node-sse-hub/redis"`) additionally requires a
> `redis-orm-lite` build whose ESM output uses file extensions —
> `redis-orm-lite@1.1.0`'s ESM entry uses extensionless relative imports
> and therefore only resolves under bundlers/CJS today. Until that is
> fixed upstream, prefer `require("node-sse-hub/redis")` (or a bundler) when
> running on plain Node ESM.

## Heartbeats

```ts
const sse = new SSEServer({ heartbeatInterval: 30_000 });
```

Every interval the server sends an SSE comment (`: heartbeat`) to each client, keeping proxies/load balancers from treating idle connections as dead. Heartbeats are comments — never exposed as application events — and timers are cleaned up on `close()` (the timer is also `unref`'d so it won't hold the process open).

Testing heartbeats without waiting out production intervals: assert the resolved config, and exercise delivery with a short-interval instance.

```ts
// No 15s waits — assert the effective configuration directly:
expect(sse.getOptions().heartbeatInterval).toBe(15_000);

// And verify delivery mechanics with a fast instance:
const fast = new SSEServer({ heartbeatInterval: 20 });
fast.connect(req, res);
await sleep(75);
const heartbeats = res.chunks.filter((c) => c.startsWith(":"));
expect(heartbeats.length).toBeGreaterThanOrEqual(2);
```

`getOptions()` returns the full effective configuration after defaults are applied (`heartbeatComment`, `historyEnabled`, caps, `hasBus`, `nodeId`, …) — handy for tests and startup logging. A fresh object is returned on each call. `getStats()` keeps reporting live state (`connections`, `topics`, `historySize`, `closed`).

## Connection lifecycle

```ts
sse.on("connection", (connection, context) => {
  /* … */
});
sse.on("disconnect", (connection) => {
  /* … */
});
sse.on("error", (error, connection) => {
  /* … */
});

// Convenience aliases:
sse.onConnection((connection, context) => {
  /* … */
});
sse.onDisconnect((connection) => {
  /* … */
});
sse.onError((error, connection) => {
  /* … */
});
```

Disconnects are detected via request abort, response close, write failures, and server shutdown. Cleanup is idempotent — duplicate close events fire `disconnect` exactly once.

## Graceful shutdown

```ts
await sse.close();
```

This stops accepting new connections, stops heartbeat timers, closes all active connections, clears topic subscriptions and history, and resolves only after cleanup is complete. Repeated calls are safe.

Typical usage:

```ts
process.on("SIGTERM", async () => {
  await sse.close();
  server.close();
});
```

## Configuration

```ts
const sse = new SSEServer({
  generateEventId: false, // auto-assign numeric IDs to ID-less events
  heartbeatInterval: 0, // ms; 0 disables heartbeats
  heartbeatComment: "heartbeat", // text of the `: …` keep-alive comment
  history: { enabled: false, maxEvents: 100 }, // + optional custom `store`
  maxBufferedEvents: 100, // per-connection queue limit under backpressure
  slowClientStrategy: "disconnect", // "disconnect" | "drop-oldest"
  maxConnections: 10_000, // new connections beyond this get HTTP 503
  maxTopicsPerConnection: 100, // subscription cap per connection
  maxEventBytes: 1_048_576, // max serialized frame size (1 MiB)
  serialize: (data) => JSON.stringify(data),
  bus: undefined, // optional SSEEventBus for multi-node fan-out
  nodeId: undefined, // stable bus node ID (defaults to random UUID)
});
```

Read the effective configuration back with `sse.getOptions()` (see [Heartbeats](#heartbeats)).

## Installation matrix

| You want                      | Install                                                                                                                     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| SSE only                      | `npm install node-sse-hub`                                                                                                  |
| SSE + Redis history           | `npm install node-sse-hub redis-orm-lite`                                                                                   |
| SSE + Redis history + retries | `npm install node-sse-hub redis-orm-lite` (`node-retry-kit` arrives transitively; configure via the store's `retry` option) |
| Multi-node live fan-out       | same as above + `new RedisEventBus(...)` (Redis Pub/Sub)                                                                    |

## TypeScript usage

```ts
import type {
  SSEEvent,
  SSEServerOptions,
  SSEServerResolvedOptions,
  BroadcastOptions,
  SSEConnectionContext,
  SSEHistoryStore,
  HeartbeatOptions,
  HistoryOptions,
  Topic,
} from "node-sse-hub";

const event: SSEEvent<{ invoiceId: string }> = {
  id: "event-123",
  event: "invoice-created",
  data: { invoiceId: "123" },
  retry: 5000,
};
```

CommonJS consumers (`module: node16`, no `"type": "module"`) are supported: `require("node-sse-hub")` resolves to the `.cjs` build with matching `.d.cts` types via conditional exports — no `resolution-mode` attributes or suppressions needed.

## API reference

| Method                                          | Description                                                                      |
| ----------------------------------------------- | -------------------------------------------------------------------------------- |
| `new SSEServer(options?)`                       | Create a server instance                                                         |
| `sse.connect(req, res, opts?)`                  | Accept a client; returns `SSEConnection`                                         |
| `sse.broadcast(event)` / `sse.send(event)`      | Send to all clients; returns recipient count                                     |
| `sse.broadcast(event, { exceptConnectionIds })` | Send to all-but-excluded (history + bus still recorded); returns count           |
| `sse.sendTo(id, event)`                         | Send to one client; returns `boolean`                                            |
| `sse.to(topic).broadcast(event)`                | Send to topic subscribers; returns count                                         |
| `sse.disconnect(id)`                            | Close a client; returns `boolean`                                                |
| `sse.getConnection(id)`                         | Look up a client                                                                 |
| `sse.getConnections()`                          | All connected clients                                                            |
| `sse.getTopics()`                               | Topics with ≥1 subscriber                                                        |
| `sse.getTopicSubscribers(topic)`                | Subscribed connections                                                           |
| `sse.getTopicSubscriberCount(topic)`            | Subscriber count                                                                 |
| `sse.getStats()`                                | `{ connections, topics, historySize, closed }`                                   |
| `sse.getOptions()`                              | Effective config after defaults (`heartbeatInterval`, caps, `hasBus`, …)         |
| `sse.on / once / off`                           | Typed `connection` / `disconnect` / `error` / `storageError` / `busError` events |
| `sse.onConnection / onDisconnect / onError`     | Convenience listener aliases                                                     |
| `sse.onStorageError / onBusError`               | Async persistence / bus failure hooks (live delivery unaffected)                 |
| `sse.getEventStore()`                           | The configured history store                                                     |
| `await sse.flushHistory()`                      | Wait for enqueued history writes to settle                                       |
| `await sse.close()`                             | Graceful shutdown (idempotent)                                                   |
| `connection.send(event)`                        | Send to this client                                                              |
| `connection.subscribe / unsubscribe(topic)`     | Manage topic membership                                                          |
| `connection.getTopics()`                        | This client's topics                                                             |
| `connection.close()`                            | Close this client (idempotent)                                                   |

Error classes: `SSEError` (base), `ConnectionNotFoundError`, `SSEClosedError`, `TopicNotFoundError`. Normal lifecycle misses (unknown IDs, empty topics) return `false`/`0`/`[]` rather than throwing.

`node-sse-hub/redis` adds: `RedisEventStore` (+ `RedisEventStoreOptions`), `RedisEventBus` (+ `RedisEventBusOptions`), re-exported `RedisRetryOptions` / `OperationOptions` types. Core adds: `MemoryEventStore`, `SSEEventStore`, `StoredSSEEvent`, `SSEEventBus`, `SSEBusEnvelope`, `GetAfterOptions`.

## Performance considerations

- Events are formatted **once per broadcast**, then fanned out — per-client work is a single socket write.
- Each connection has an independent bounded buffer, so one slow client never blocks others.
- Under backpressure (`response.write()` returns `false`), frames queue up to `maxBufferedEvents`, then the `slowClientStrategy` applies: `"disconnect"` (default) closes the slow consumer; `"drop-oldest"` discards the oldest queued frames (counted in `connection.droppedFrames`) so live data keeps flowing.
- History is bounded (`maxEvents`, oldest evicted). Monitor `getStats().historySize`.
- For very high fan-out, prefer fewer, larger events over many tiny ones, and consider one `SSEServer` per concern to keep topic/history cardinality bounded.

## Limitations

- Single-process only: history and connections live in memory. Multi-server deployments need sticky sessions (so `Last-Event-ID` reconnects land where history lives) or a shared `SSEHistoryStore`.
- History replay is positional over a global log (not per-topic); direct messages are excluded from history.
- `EventSource` supports GET-only streams without custom headers — authentication typically happens via cookies, signed URLs, or a pre-handshake; `node-sse-hub` leaves auth to the host app.
- Browser `EventSource` does not let you set `Last-Event-ID` manually on first connect; the browser manages it from received `id:` fields.

## Browser example

```ts
const events = new EventSource("/events");

events.addEventListener("invoice-created", (event) => {
  const invoice = JSON.parse(event.data);
  console.log(invoice);
});

events.onerror = () => {
  console.log("disconnected — browser will retry automatically");
};
```

## Security notes

Authentication/authorization must be handled by the host application (middleware, signed URLs, cookies) **before** calling `connect()` — the core package deliberately has no auth. Defaults guard common abuses: bounded history, bounded per-client buffers, connection/topic/payload caps. Tune `maxConnections`, `maxBufferedEvents`, `maxTopicsPerConnection`, and `maxEventBytes` for your deployment.

## License

MIT — see [LICENSE](LICENSE).

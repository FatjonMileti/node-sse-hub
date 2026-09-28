# sse-kit

A lightweight, framework-friendly **Server-Sent Events (SSE)** library for Node.js.

Create SSE streams, send events to individual clients, broadcast to everyone, publish to topics, replay missed events after reconnects, and shut down gracefully — with strong TypeScript types and zero runtime dependencies.

## Why use it?

- **Framework-friendly** — works with Express, Fastify, Node's native HTTP server, or anything exposing Node `IncomingMessage` / `ServerResponse`. No framework dependencies.
- **Correct SSE** — spec-compliant framing, multiline data, `retry`, comments, and `Last-Event-ID` handling.
- **Topics** — subscribe connections to rooms and broadcast per-room.
- **Replay** — optional in-memory event history replays missed events to reconnecting clients (with a pluggable store interface).
- **Reliable** — heartbeat keep-alives, backpressure handling for slow clients, idempotent cleanup, graceful shutdown.
- **Small** — zero runtime dependencies, strict TypeScript, ESM + CommonJS builds.

## Installation

```bash
npm install sse-kit
```

Requires Node.js 18+.

## Quick start

```ts
import { SSEServer } from "sse-kit";

const sse = new SSEServer({ heartbeatInterval: 30_000 });

// …inside your route handler:
sse.connect(req, res);

// …anywhere else in your app:
sse.broadcast({ event: "ping", data: { ok: true } });
```

## Express example

```ts
import express from "express";
import { SSEServer } from "sse-kit";

const sse = new SSEServer({ heartbeatInterval: 30_000 });
const app = express();

app.get("/events", (req, res) => {
  sse.connect(req, res);
});

app.listen(3000);
```

See [`examples/express.ts`](examples/express.ts).

## Native Node example

```ts
import { createServer } from "node:http";
import { SSEServer } from "sse-kit";

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

`sse-kit` does **not** pretend to replay events it doesn't have: replay only happens when `history.enabled` is `true` (below). Without history, `lastEventId` is exposed so your application can implement its own catch-up logic (e.g. query a database).

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
import type { SSEHistoryStore } from "sse-kit";

const sse = new SSEServer({
  history: { enabled: true, maxEvents: 1000, store: myStore },
});
```

## Heartbeats

```ts
const sse = new SSEServer({ heartbeatInterval: 30_000 });
```

Every interval the server sends an SSE comment (`: heartbeat`) to each client, keeping proxies/load balancers from treating idle connections as dead. Heartbeats are comments — never exposed as application events — and timers are cleaned up on `close()` (the timer is also `unref`'d so it won't hold the process open).

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
});
```

## TypeScript usage

```ts
import type {
  SSEEvent,
  SSEServerOptions,
  SSEConnectionContext,
  SSEHistoryStore,
  HeartbeatOptions,
  HistoryOptions,
  Topic,
} from "sse-kit";

const event: SSEEvent<{ invoiceId: string }> = {
  id: "event-123",
  event: "invoice-created",
  data: { invoiceId: "123" },
  retry: 5000,
};
```

## API reference

| Method                                      | Description                                        |
| ------------------------------------------- | -------------------------------------------------- |
| `new SSEServer(options?)`                   | Create a server instance                           |
| `sse.connect(req, res, opts?)`              | Accept a client; returns `SSEConnection`           |
| `sse.broadcast(event)` / `sse.send(event)`  | Send to all clients; returns recipient count       |
| `sse.sendTo(id, event)`                     | Send to one client; returns `boolean`              |
| `sse.to(topic).broadcast(event)`            | Send to topic subscribers; returns count           |
| `sse.disconnect(id)`                        | Close a client; returns `boolean`                  |
| `sse.getConnection(id)`                     | Look up a client                                   |
| `sse.getConnections()`                      | All connected clients                              |
| `sse.getTopics()`                           | Topics with ≥1 subscriber                          |
| `sse.getTopicSubscribers(topic)`            | Subscribed connections                             |
| `sse.getTopicSubscriberCount(topic)`        | Subscriber count                                   |
| `sse.getStats()`                            | `{ connections, topics, historySize, closed }`     |
| `sse.on / once / off`                       | Typed `connection` / `disconnect` / `error` events |
| `sse.onConnection / onDisconnect / onError` | Convenience listener aliases                       |
| `await sse.close()`                         | Graceful shutdown (idempotent)                     |
| `connection.send(event)`                    | Send to this client                                |
| `connection.subscribe / unsubscribe(topic)` | Manage topic membership                            |
| `connection.getTopics()`                    | This client's topics                               |
| `connection.close()`                        | Close this client (idempotent)                     |

Error classes: `SSEError` (base), `ConnectionNotFoundError`, `SSEClosedError`, `TopicNotFoundError`. Normal lifecycle misses (unknown IDs, empty topics) return `false`/`0`/`[]` rather than throwing.

## Performance considerations

- Events are formatted **once per broadcast**, then fanned out — per-client work is a single socket write.
- Each connection has an independent bounded buffer, so one slow client never blocks others.
- Under backpressure (`response.write()` returns `false`), frames queue up to `maxBufferedEvents`, then the `slowClientStrategy` applies: `"disconnect"` (default) closes the slow consumer; `"drop-oldest"` discards the oldest queued frames (counted in `connection.droppedFrames`) so live data keeps flowing.
- History is bounded (`maxEvents`, oldest evicted). Monitor `getStats().historySize`.
- For very high fan-out, prefer fewer, larger events over many tiny ones, and consider one `SSEServer` per concern to keep topic/history cardinality bounded.

## Limitations

- Single-process only: history and connections live in memory. Multi-server deployments need sticky sessions (so `Last-Event-ID` reconnects land where history lives) or a shared `SSEHistoryStore`.
- History replay is positional over a global log (not per-topic); direct messages are excluded from history.
- `EventSource` supports GET-only streams without custom headers — authentication typically happens via cookies, signed URLs, or a pre-handshake; `sse-kit` leaves auth to the host app.
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

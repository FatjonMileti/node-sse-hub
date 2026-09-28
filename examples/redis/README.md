# Redis example for `node-sse-hub`

Invoice-style SSE service with Redis-backed history (and optional
multi-node live fan-out), using only native Node.js HTTP plus:

- `node-sse-hub` — the SSE server
- `node-sse-hub/redis` — `RedisEventStore` + `RedisEventBus` adapters
- `redis-orm-lite` — Redis persistence (documents) and retry plumbing
- `node-retry-kit` — pulled in transitively by `redis-orm-lite`

## Run

```bash
docker run -p 6379:6379 redis:7
npm install node-sse-hub redis-orm-lite
# from the node-sse-hub repo:
npx tsx examples/redis/server.ts
```

## Try it

```bash
# 1. Open a stream (note the event ids):
curl -N http://localhost:3000/events

# 2. Publish an invoice (in another terminal):
curl -X POST http://localhost:3000/invoices \
  -H 'Content-Type: application/json' \
  -d '{"id":"INV-123","total":150}'

# 3. Kill the stream, then reconnect with the last seen id
#    to receive only what you missed:
curl -N -H 'Last-Event-ID: 1' http://localhost:3000/events
```

## What to observe

- Live clients receive `invoice-created` immediately, even if Redis is
  slow (broadcast-first, persist-after).
- A reconnecting client replays missed invoices from Redis history.
- If Redis goes down, publishing keeps working; failures surface on
  `sse.onStorageError(...)` instead of crashing the server.
- Run a second copy with `NODE_ID=node-2` (and a different port) to see
  `RedisEventBus` fan events out across processes.

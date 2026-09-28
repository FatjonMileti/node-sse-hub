# Redis example for `sse-kit`

Invoice-style SSE service with Redis-backed history (and optional
multi-node live fan-out), using only native Node.js HTTP plus:

- `sse-kit` — the SSE server
- `sse-kit/redis` — `RedisEventStore` + `RedisEventBus` adapters
- `redis-orm-lite` — Redis persistence (documents) and retry plumbing
- `node-retry-kit` — pulled in transitively by `redis-orm-lite`

## Run

```bash
docker run -p 6379:6379 redis:7
npm install sse-kit redis-orm-lite
# from the sse-kit repo:
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

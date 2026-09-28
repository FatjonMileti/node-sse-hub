/**
 * Fastify example for `node-sse-hub` — no Fastify dependency required by the
 * core package; we only use Fastify's raw Node req/res objects.
 *
 * Run with:  npx tsx examples/fastify.ts
 * (Install fastify to try this file.)
 */
import Fastify from "fastify";
import { SSEServer } from "../src/index.js";

const sse = new SSEServer({ heartbeatInterval: 30_000 });
const app = Fastify();

app.get("/events", (request, reply) => {
  // Fastify wraps Node's primitives — pass the raw objects through.
  // Hijack the reply so Fastify doesn't try to send its own response.
  reply.hijack();
  sse.connect(request.raw, reply.raw);
});

app.post("/publish", (request) => {
  const count = sse.broadcast({
    event: "notification",
    data: (request.body ?? {}) as Record<string, unknown>,
  });
  return { delivered: count };
});

await app.listen({ port: 3000 });
console.log("SSE server listening on http://localhost:3000/events");

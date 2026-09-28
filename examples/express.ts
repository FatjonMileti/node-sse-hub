/**
 * Express example for `node-sse-hub`.
 *
 * Run with:  npx tsx examples/express.ts
 * (Express is only a dev-time example dependency pattern — the core
 * package never depends on it. Install express to try this file.)
 */
import express from "express";
import { SSEServer } from "../src/index.js";

const sse = new SSEServer({
  heartbeatInterval: 30_000,
  history: { enabled: true, maxEvents: 100 },
});

const app = express();
app.use(express.json());

// Open an SSE stream. Authenticate first (e.g. auth middleware) —
// node-sse-hub intentionally leaves auth to the host application.
app.get("/events", (req, res) => {
  sse.connect(req, res);
});

// Publish an event to every connected client.
app.post("/publish", (req, res) => {
  const count = sse.broadcast({
    event: "notification",
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    data: req.body,
  });
  res.json({ delivered: count });
});

// Graceful shutdown.
process.on("SIGTERM", () => {
  void (async () => {
    await sse.close();
    server.close();
  })();
});

const server = app.listen(3000, () => {
  console.log("SSE server listening on http://localhost:3000/events");
});

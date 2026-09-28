/**
 * Native Node.js HTTP example for `sse-kit` — no framework required.
 *
 * Run with:  node --loader ts-node/esm examples/native-http.ts
 * (or compile first and run the emitted JS).
 */
import { createServer } from "node:http";
import { SSEServer } from "../src/index.js";

const sse = new SSEServer({ heartbeatInterval: 30_000 });

const server = createServer((req, res) => {
  if (req.url === "/events") {
    // Authenticate/authorize here before connecting in real apps.
    sse.connect(req, res);
    return;
  }
  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not found");
});

server.listen(3000, () => {
  console.log("SSE server listening on http://localhost:3000/events");
});

// Push an event every 5 seconds.
const timer = setInterval(() => {
  sse.broadcast({ event: "tick", data: { at: new Date().toISOString() } });
}, 5_000);
timer.unref();

// Graceful shutdown.
async function shutdown(signal: string): Promise<void> {
  console.log(`Received ${signal}, shutting down…`);
  clearInterval(timer);
  await sse.close();
  server.close();
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

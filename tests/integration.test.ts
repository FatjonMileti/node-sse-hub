import { createServer, request, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { SSEServer } from "../src/SSEServer.js";
import { sleep } from "./helpers.js";

const servers: Server[] = [];
const kits: SSEServer[] = [];

afterEach(async () => {
  for (const sse of kits.splice(0)) {
    await sse.close();
  }
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => {
            resolve();
          });
        }),
    ),
  );
});

function listen(sse: SSEServer): Promise<{ server: Server; port: number }> {
  kits.push(sse);
  const server = createServer((req, res) => {
    if (req.url === "/events") {
      sse.connect(req, res);
    } else {
      res.writeHead(404).end();
    }
  });
  servers.push(server);
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      resolve({ server, port });
    });
  });
}

interface CollectedStream {
  chunks: string[];
  close: () => void;
  waitFor: (predicate: () => boolean, timeoutMs?: number) => Promise<void>;
}

/** Open a real SSE stream against the test server. */
function openStream(
  port: number,
  headers: Record<string, string> = {},
): Promise<CollectedStream> {
  return new Promise((resolve, reject) => {
    const req = request(
      { port, host: "127.0.0.1", path: "/events", headers },
      (res) => {
        const chunks: string[] = [];
        res.on("data", (d: Buffer) => chunks.push(d.toString()));
        const stream: CollectedStream = {
          chunks,
          close: () => {
            res.destroy();
          },
          waitFor: async (predicate, timeoutMs = 2000) => {
            const start = Date.now();
            while (!predicate()) {
              if (Date.now() - start > timeoutMs) {
                throw new Error(
                  `Timed out waiting for stream data. Got: ${JSON.stringify(chunks)}`,
                );
              }
              await sleep(10);
            }
          },
        };
        resolve(stream);
      },
    );
    req.on("error", reject);
    req.end();
  });
}

describe("native HTTP integration", () => {
  it("serves SSE with correct headers over a real socket", async () => {
    const sse = new SSEServer();
    const { port } = await listen(sse);
    const stream = await openStream(port);
    sse.broadcast({ event: "message", data: { message: "Hello" } });
    await stream.waitFor(() => stream.chunks.join("").includes("Hello"));
    expect(stream.chunks.join("")).toBe(
      'event: message\ndata: {"message":"Hello"}\n\n',
    );
    stream.close();
  });

  it("delivers string data and multiline payloads end-to-end", async () => {
    const sse = new SSEServer();
    const { port } = await listen(sse);
    const stream = await openStream(port);
    sse.broadcast({ event: "message", data: "Hello" });
    sse.broadcast({ data: "first line\nsecond line" });
    await stream.waitFor(() => stream.chunks.join("").includes("second line"));
    const body = stream.chunks.join("");
    expect(body).toContain("event: message\ndata: Hello\n\n");
    expect(body).toContain("data: first line\ndata: second line\n\n");
    stream.close();
  });

  it("supports Last-Event-ID replay over real reconnects", async () => {
    const sse = new SSEServer({
      history: { enabled: true, maxEvents: 100 },
      generateEventId: true,
    });
    const { port } = await listen(sse);

    const first = await openStream(port);
    sse.broadcast({ data: "one" });
    sse.broadcast({ data: "two" });
    sse.broadcast({ data: "three" });
    await first.waitFor(() => first.chunks.join("").includes("three"));
    first.close();
    await sleep(50); // allow server-side cleanup of the dead socket

    const second = await openStream(port, { "Last-Event-ID": "2" });
    await second.waitFor(() => second.chunks.join("").includes("three"));
    const body = second.chunks.join("");
    expect(body).toContain("id: 3");
    expect(body).not.toContain("data: one");
    expect(body).not.toContain("data: two");
    second.close();
  });

  it("detects real client disconnects and cleans up", async () => {
    const sse = new SSEServer();
    const { port } = await listen(sse);
    const stream = await openStream(port);
    expect(sse.connectionCount).toBe(1);
    stream.close();
    const start = Date.now();
    while (sse.connectionCount !== 0 && Date.now() - start < 2000) {
      await sleep(10);
    }
    expect(sse.connectionCount).toBe(0);
  });

  it("delivers heartbeats over a real socket", async () => {
    const sse = new SSEServer({ heartbeatInterval: 20 });
    const { port } = await listen(sse);
    const stream = await openStream(port);
    await stream.waitFor(
      () => stream.chunks.join("").includes(": heartbeat"),
      2000,
    );
    stream.close();
  });
});

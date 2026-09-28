import { execFile } from "node:child_process";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MemoryEventStore } from "../src/store.js";
import { SSEServer } from "../src/SSEServer.js";
import { createMocks, sleep } from "./helpers.js";

const rootDir = fileURLToPath(new URL("..", import.meta.url));

describe("MemoryEventStore", () => {
  it("appends and replays everything after an ID", async () => {
    const store = new MemoryEventStore({ maxEvents: 10 });
    await store.append({ id: "1", data: "a", createdAt: 1 });
    await store.append({ id: "2", data: "b", createdAt: 2 });
    await store.append({ id: "3", data: "c", createdAt: 3 });
    expect(store.size).toBe(3);
    expect((await store.getAfter("1")).map((e) => e.id)).toEqual(["2", "3"]);
    expect(await store.getAfter("3")).toEqual([]);
  });

  it("returns [] for unknown IDs and evicts oldest beyond maxEvents", async () => {
    const store = new MemoryEventStore({ maxEvents: 2 });
    await store.append({ id: "1", data: "a", createdAt: 1 });
    await store.append({ id: "2", data: "b", createdAt: 2 });
    await store.append({ id: "3", data: "c", createdAt: 3 });
    expect(store.size).toBe(2);
    expect(await store.getAfter("missing")).toEqual([]);
    expect(await store.getAfter("1")).toEqual([]);
    expect((await store.getAfter("2")).map((e) => e.id)).toEqual(["3"]);
  });

  it("supports topic filtering, limits, and getRecent/clear", async () => {
    const store = new MemoryEventStore({ maxEvents: 10 });
    await store.append({ id: "1", topic: "a", data: 1, createdAt: 1 });
    await store.append({ id: "2", topic: "b", data: 2, createdAt: 2 });
    await store.append({ id: "3", topic: "a", data: 3, createdAt: 3 });
    expect(
      (await store.getAfter("1", { topic: "a" })).map((e) => e.id),
    ).toEqual(["3"]);
    expect((await store.getAfter("1", { limit: 1 })).map((e) => e.id)).toEqual([
      "2",
    ]);
    expect((await store.getRecent(2)).map((e) => e.id)).toEqual(["2", "3"]);
    await store.clear();
    expect(store.size).toBe(0);
  });

  it("validates options", async () => {
    expect(() => new MemoryEventStore({ maxEvents: 0 })).toThrow();
    const store = new MemoryEventStore();
    await expect(store.getRecent(-1)).rejects.toThrow();
  });
});

describe("server with an async event store", () => {
  it("persists broadcasts and replays them to reconnecting clients", async () => {
    const store = new MemoryEventStore({ maxEvents: 100 });
    const sse = new SSEServer({
      history: { enabled: true, store },
      generateEventId: true,
    });
    const first = createMocks();
    sse.connect(first.req.asIncomingMessage(), first.res.asServerResponse());
    sse.broadcast({ data: "one" });
    sse.broadcast({ data: "two" });
    await sse.flushHistory();
    expect(store.size).toBe(2);

    const second = createMocks({ "last-event-id": "1" });
    sse.connect(second.req.asIncomingMessage(), second.res.asServerResponse());
    await sleep(10); // async replay resolves after connect() returns
    expect(second.res.body).toContain("id: 2\ndata: two");
    expect(second.res.body).not.toContain("data: one");
    await sse.close();
  });

  it("tags topic broadcasts and exposes the store", async () => {
    const store = new MemoryEventStore({ maxEvents: 100 });
    const sse = new SSEServer({
      history: { enabled: true, store },
      generateEventId: true,
    });
    expect(sse.getEventStore()).toBe(store);
    const { req, res } = createMocks();
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    conn.subscribe("invoices");
    sse.to("invoices").broadcast({ event: "created", data: { id: 1 } });
    await sse.flushHistory();
    const recent = await store.getRecent(5);
    expect(recent).toHaveLength(1);
    expect(recent[0]).toMatchObject({ topic: "invoices", event: "created" });
    expect(sse.getStats().historySize).toBe(1);
    await sse.close();
  });

  it("keeps broadcasting when persistence fails, via storageError", async () => {
    const failing = new MemoryEventStore();
    failing.append = async () => {
      throw new Error("disk on fire");
    };
    const sse = new SSEServer({ history: { enabled: true, store: failing } });
    const errors: Error[] = [];
    sse.onStorageError((error) => {
      errors.push(error);
    });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    // Live delivery is unaffected by the storage failure.
    expect(sse.broadcast({ data: "live" })).toBe(1);
    expect(res.body).toContain("data: live");
    await sse.flushHistory();
    expect(errors).toHaveLength(1);
    expect(errors[0]?.message).toBe("disk on fire");
    await sse.close();
  });

  it("surfaces replay failures via storageError without dropping the client", async () => {
    const failing = new MemoryEventStore();
    failing.getAfter = async () => {
      throw new Error("cannot read");
    };
    const sse = new SSEServer({ history: { enabled: true, store: failing } });
    const errors: Error[] = [];
    sse.on("storageError", (error) => {
      errors.push(error);
    });
    const { req, res } = createMocks({ "last-event-id": "1" });
    const conn = sse.connect(req.asIncomingMessage(), res.asServerResponse());
    await sleep(10);
    expect(conn.closed).toBe(false);
    expect(sse.getConnection(conn.id)).toBe(conn);
    expect(errors).toHaveLength(1);
    await sse.close();
  });

  it("does not clear async stores on close (they may be shared)", async () => {
    const store = new MemoryEventStore({ maxEvents: 100 });
    const sse = new SSEServer({
      history: { enabled: true, store },
      generateEventId: true,
    });
    const { req, res } = createMocks();
    sse.connect(req.asIncomingMessage(), res.asServerResponse());
    sse.broadcast({ data: "x" });
    await sse.flushHistory();
    await sse.close();
    expect(store.size).toBe(1);
  });
});

describe("core isolation from optional dependencies", () => {
  const BLOCKED = ["redis-orm-lite", "node-retry-kit"];

  it("core source graph never references optional deps or the redis entry", () => {
    const read = (name: string): string =>
      readFileSync(join(rootDir, "src", name), "utf8");
    const entryImports = (source: string): string[] =>
      [...source.matchAll(/from\s+["']([^"']+)["']/g)].map((m) => m[1] ?? "");
    const visited = new Set<string>(["index.ts"]);
    const queue = ["index.ts"];
    while (queue.length > 0) {
      const file = queue.pop() as string;
      for (const spec of entryImports(read(file))) {
        for (const blocked of BLOCKED) {
          expect(spec).not.toContain(blocked);
        }
        expect(spec).not.toBe("./redis.js");
        const local = spec.match(/^\.\/(.+)\.js$/);
        if (local?.[1] && !visited.has(`${local[1]}.ts`)) {
          visited.add(`${local[1]}.ts`);
          queue.push(`${local[1]}.ts`);
        }
      }
    }
    // Sanity: the traversal actually covered the core modules.
    expect([...visited].sort()).toEqual(
      expect.arrayContaining([
        "SSEServer.ts",
        "SSEConnection.ts",
        "SSEEvent.ts",
        "TopicManager.ts",
        "store.ts",
        "types.ts",
        "errors.ts",
      ]),
    );
  });

  it("built core files contain no optional-dependency references", () => {
    const dist = join(rootDir, "dist");
    if (!existsSync(dist)) {
      console.warn("skipping: dist not built (run npm run build first)");
      return;
    }
    // Only files reachable from the core entry point. dist/redis.js is
    // the optional subpath entry and legitimately references the peer.
    const coreFiles = [
      "index.js",
      "SSEServer.js",
      "SSEConnection.js",
      "SSEEvent.js",
      "TopicManager.js",
      "types.js",
      "errors.js",
      "store.js",
    ];
    for (const file of coreFiles) {
      const content = readFileSync(join(dist, file), "utf8");
      for (const blocked of BLOCKED) {
        expect(content, file).not.toContain(blocked);
      }
    }
    for (const file of readdirSync(join(dist, "cjs")).filter((f) =>
      f.endsWith(".cjs"),
    )) {
      if (file === "redis.cjs") continue; // optional subpath entry
      const content = readFileSync(join(dist, "cjs", file), "utf8");
      for (const blocked of BLOCKED) {
        expect(content, file).not.toContain(blocked);
      }
    }
  });

  it("core runs at runtime with optional deps blocked", async () => {
    const dist = join(rootDir, "dist");
    if (!existsSync(join(dist, "index.js"))) {
      console.warn("skipping: dist not built (run npm run build first)");
      return;
    }
    const loader = join(
      rootDir,
      "tests",
      "fixtures",
      "block-optional-deps-loader.mjs",
    );
    const smoke = join(rootDir, "tests", "fixtures", "no-deps-smoke.mjs");
    const output = await new Promise<string>((resolve, reject) => {
      execFile(
        process.execPath,
        ["--loader", loader, smoke, dist],
        { cwd: rootDir },
        (error, stdout, stderr) => {
          if (error) {
            reject(new Error(`smoke failed: ${stderr || stdout}`));
            return;
          }
          resolve(stdout);
        },
      );
    });
    expect(output).toContain("NO_DEPS_SMOKE_OK");
  }, 30_000);
});

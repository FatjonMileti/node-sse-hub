/**
 * Runtime smoke test used by isolation.test.ts: runs the BUILT core
 * package (ESM + CJS) in a process where `redis-orm-lite` and
 * `node-retry-kit` are blocked by the loader fixture. Any static or
 * dynamic import of those packages fails the run.
 *
 * Usage: node --loader <loader> tests/fixtures/no-deps-smoke.mjs <distDir>
 */
/* global process, console */
import { EventEmitter } from "node:events";
import { createRequire } from "node:module";

const distDir = process.argv[2];
if (!distDir) {
  console.error("usage: no-deps-smoke.mjs <distDir>");
  process.exit(2);
}

class MockRes extends EventEmitter {
  constructor() {
    super();
    this.chunks = [];
    this.writableEnded = false;
  }
  writeHead() {}
  flushHeaders() {}
  write(chunk) {
    this.chunks.push(chunk);
    return true;
  }
  end() {
    this.writableEnded = true;
  }
}

class MockReq extends EventEmitter {
  constructor() {
    super();
    this.headers = {};
  }
}

// ESM entry.
const esm = await import(`${distDir}/index.js`);
const sse = new esm.SSEServer({ history: { enabled: true, maxEvents: 10 } });
const res = new MockRes();
sse.connect(new MockReq(), res);
const count = sse.broadcast({ event: "hello", data: { n: 1 } });
if (count !== 1) throw new Error(`ESM broadcast count: ${count}`);
if (!res.chunks.join("").includes("hello")) {
  throw new Error("ESM broadcast payload missing");
}
await sse.close();

// CJS entry.
const require = createRequire(import.meta.url);
const cjs = require(`${distDir}/cjs/index.cjs`);
const sse2 = new cjs.SSEServer();
const res2 = new MockRes();
sse2.connect(new MockReq(), res2);
if (sse2.broadcast({ data: "hi" }) !== 1) throw new Error("CJS broadcast failed");
await sse2.close();

console.log("NO_DEPS_SMOKE_OK");

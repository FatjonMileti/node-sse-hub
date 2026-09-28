/**
 * Shared test helpers: lightweight in-memory mocks of Node's
 * `IncomingMessage` / `ServerResponse` plus utilities for real-HTTP
 * integration tests.
 */
import { EventEmitter } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";

export class MockRequest extends EventEmitter {
  headers: Record<string, string | string[] | undefined> = {};
  url = "/events";
  method = "GET";

  constructor(headers: Record<string, string> = {}) {
    super();
    for (const [key, value] of Object.entries(headers)) {
      this.headers[key.toLowerCase()] = value;
    }
  }

  asIncomingMessage(): IncomingMessage {
    return this as unknown as IncomingMessage;
  }
}

export interface WriteBehavior {
  /** Return value of `write()`. Return `false` to simulate backpressure. */
  writeReturn?: boolean | ((chunk: string) => boolean);
  /** Throw from `write()` to simulate a failed socket. */
  throwOnWrite?: boolean;
}

export class MockResponse extends EventEmitter {
  chunks: string[] = [];
  statusCode: number | undefined;
  responseHeaders: Record<string, string | number | string[]> = {};
  writableEnded = false;
  headersFlushed = false;
  writeBehavior: WriteBehavior = {};

  writeHead(status: number, headers?: Record<string, string>): this {
    this.statusCode = status;
    if (headers) {
      for (const [key, value] of Object.entries(headers)) {
        this.responseHeaders[key] = value;
      }
    }
    return this;
  }

  flushHeaders(): void {
    this.headersFlushed = true;
  }

  write(chunk: string): boolean {
    if (this.writeBehavior.throwOnWrite) {
      throw new Error("mock socket write failure");
    }
    this.chunks.push(chunk);
    const ret = this.writeBehavior.writeReturn;
    if (typeof ret === "function") return ret(chunk);
    return ret ?? true;
  }

  end(): this {
    this.writableEnded = true;
    return this;
  }

  get body(): string {
    return this.chunks.join("");
  }

  asServerResponse(): ServerResponse {
    return this as unknown as ServerResponse;
  }

  /** Simulate the client going away (triggers `close` listeners). */
  simulateClientClose(): void {
    this.emit("close");
  }

  /** Simulate the socket becoming writable again. */
  simulateDrain(): void {
    this.emit("drain");
  }
}

export function createMocks(
  requestHeaders: Record<string, string> = {},
  writeBehavior: WriteBehavior = {},
): { req: MockRequest; res: MockResponse } {
  const req = new MockRequest(requestHeaders);
  const res = new MockResponse();
  res.writeBehavior = writeBehavior;
  return { req, res };
}

/** Small sleep helper for timer-based tests. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * A single Server-Sent Events connection: wraps a Node.js `ServerResponse`,
 * owns the per-connection write buffer used for backpressure handling, and
 * tracks topic subscriptions.
 */

import { randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { SSEError } from "./errors.js";
import { formatSSEFrame } from "./SSEEvent.js";
import type { TopicManager } from "./TopicManager.js";
import { assertValidTopic } from "./TopicManager.js";
import type {
  SSEConnectionContext,
  SSEEvent,
  SSESerializer,
  SlowClientStrategy,
  Topic,
} from "./types.js";
import { DEFAULT_SERIALIZE } from "./SSEEvent.js";

/** Options used to construct an {@link SSEConnection}. */
export interface SSEConnectionOptions {
  req: IncomingMessage;
  res: ServerResponse;
  topics: TopicManager;
  context?: SSEConnectionContext;
  initialTopics?: string[] | undefined;
  serialize?: SSESerializer;
  maxBufferedEvents?: number;
  slowClientStrategy?: SlowClientStrategy;
  maxTopicsPerConnection?: number;
  /**
   * Called exactly once when the connection is cleaned up, so the owning
   * server can remove it from its registry. Cleanup is idempotent — the
   * callback fires only on the first close.
   */
  onCleanup?: (connection: SSEConnection) => void;
}

/**
 * Represents one connected SSE client.
 *
 * Backpressure behavior: frames are written directly while the socket
 * accepts them. When `response.write()` returns `false` (kernel buffer
 * full), subsequent frames are queued in memory (bounded by
 * `maxBufferedEvents`) and flushed on the response's `drain` event. When
 * the queue is full, the configured `slowClientStrategy` applies:
 * `"disconnect"` closes the slow client, `"drop-oldest"` discards the
 * oldest queued frames. Broadcasts never block on a single slow client —
 * each connection is handled independently.
 */
export class SSEConnection {
  /** Unique connection ID (UUID v4). */
  readonly id: string = randomUUID();

  /** When the connection was established. */
  readonly createdAt: Date = new Date();

  /** The `Last-Event-ID` header value sent by the client, if any. */
  readonly lastEventId: string | undefined;

  readonly #res: ServerResponse;
  readonly #topics: TopicManager;
  readonly #serialize: SSESerializer;
  readonly #maxBufferedEvents: number;
  readonly #slowClientStrategy: SlowClientStrategy;
  readonly #maxTopicsPerConnection: number;
  readonly #onCleanup: ((connection: SSEConnection) => void) | undefined;

  readonly #queue: string[] = [];
  #awaitingDrain = false;
  #closed = false;
  #slow = false;
  #onDrain = (): void => {
    this.#awaitingDrain = false;
    this.#flush();
  };

  /** Number of frames dropped due to `drop-oldest` backpressure handling. */
  #droppedFrames = 0;

  constructor(options: SSEConnectionOptions) {
    this.#res = options.res;
    this.#topics = options.topics;
    this.lastEventId = options.context?.lastEventId;
    this.#serialize = options.serialize ?? DEFAULT_SERIALIZE;
    this.#maxBufferedEvents = options.maxBufferedEvents ?? 100;
    this.#slowClientStrategy = options.slowClientStrategy ?? "disconnect";
    this.#maxTopicsPerConnection = options.maxTopicsPerConnection ?? 100;
    this.#onCleanup = options.onCleanup;

    this.#res.on("drain", this.#onDrain);

    for (const topic of options.initialTopics ?? []) {
      this.subscribe(topic);
    }
  }

  /** Whether the connection has been closed (idempotent flag). */
  get closed(): boolean {
    return this.#closed;
  }

  /** Whether the connection is currently experiencing backpressure. */
  get hasBackpressure(): boolean {
    return this.#awaitingDrain || this.#queue.length > 0;
  }

  /** Number of frames currently buffered for this connection. */
  get bufferedCount(): number {
    return this.#queue.length;
  }

  /** Number of frames dropped via the `drop-oldest` strategy. */
  get droppedFrames(): number {
    return this.#droppedFrames;
  }

  /** Whether this connection was flagged as a slow consumer. */
  get isSlow(): boolean {
    return this.#slow;
  }

  /** Topics this connection is currently subscribed to. */
  getTopics(): Topic[] {
    return this.#topics.getTopicsFor(this.id);
  }

  /**
   * Subscribe to a topic.
   * @throws {@link SSEError} for invalid topics or when the per-connection
   * topic limit is exceeded.
   */
  subscribe(topic: string): void {
    if (this.#closed) return;
    assertValidTopic(topic);
    const current = this.#topics.getTopicsFor(this.id);
    if (
      !current.includes(topic) &&
      current.length >= this.#maxTopicsPerConnection
    ) {
      throw new SSEError(
        `Connection ${this.id} exceeds the limit of ${this.#maxTopicsPerConnection} topic subscriptions.`,
      );
    }
    this.#topics.add(topic, this.id);
  }

  /**
   * Unsubscribe from a topic.
   * @returns `true` if a subscription was removed.
   */
  unsubscribe(topic: string): boolean {
    return this.#topics.remove(topic, this.id);
  }

  /**
   * Send an event to this client.
   * @returns `true` if the frame was accepted (written or buffered),
   * `false` if the connection is closed or was disconnected as a slow client.
   */
  send<T>(event: SSEEvent<T>): boolean {
    if (this.#closed) return false;
    const frame = formatSSEFrame(event, { serialize: this.#serialize });
    return this.writeFrame(frame);
  }

  /**
   * Write a pre-formatted SSE frame (or heartbeat comment) to this client,
   * applying backpressure handling. Used by the server for broadcasts so
   * each event is formatted exactly once.
   *
   * @returns `true` if the frame was accepted, `false` otherwise.
   */
  writeFrame(frame: string): boolean {
    if (this.#closed) return false;

    if (this.#awaitingDrain || this.#queue.length > 0) {
      return this.#enqueue(frame);
    }

    let accepted: boolean;
    try {
      accepted = this.#res.write(frame);
    } catch {
      this.close();
      return false;
    }

    if (!accepted) {
      this.#awaitingDrain = true;
    }
    return true;
  }

  #enqueue(frame: string): boolean {
    if (this.#queue.length >= this.#maxBufferedEvents) {
      if (this.#slowClientStrategy === "drop-oldest") {
        this.#queue.shift();
        this.#droppedFrames += 1;
        this.#slow = true;
        this.#queue.push(frame);
        return true;
      }
      // "disconnect": predictable cleanup for slow consumers.
      this.#slow = true;
      this.close();
      return false;
    }
    this.#queue.push(frame);
    return true;
  }

  #flush(): void {
    if (this.#closed) return;
    while (this.#queue.length > 0 && !this.#awaitingDrain) {
      const frame = this.#queue.shift();
      if (frame === undefined) return;
      let accepted: boolean;
      try {
        accepted = this.#res.write(frame);
      } catch {
        this.close();
        return;
      }
      if (!accepted) {
        this.#awaitingDrain = true;
      }
    }
  }

  /**
   * Close this connection and release its resources. Safe to call multiple
   * times — only the first call performs cleanup.
   */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;

    this.#res.off("drain", this.#onDrain);
    this.#queue.length = 0;
    this.#awaitingDrain = false;
    this.#topics.removeConnection(this.id);

    try {
      if (!this.#res.writableEnded) {
        this.#res.end();
      }
    } catch {
      // Ignore errors during teardown — the socket may already be gone.
    }

    this.#onCleanup?.(this);
  }
}

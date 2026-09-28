/**
 * Pluggable event-history abstractions for `node-sse-hub`.
 *
 * The {@link SSEServer} depends only on the {@link SSEEventStore}
 * interface — never on Redis or any other infrastructure. The default
 * path stays fully in-memory; persistence adapters (Redis, database,
 * …) implement this interface in optional subpath entries such as
 * `node-sse-hub/redis`.
 */

import { SSEError } from "./errors.js";
import type { SSEEvent } from "./types.js";

/**
 * A single persisted event: everything needed to replay it later.
 *
 * Only replay-relevant fields are stored — never connections, sockets,
 * requests, or responses.
 *
 * `sequence` provides deterministic ordering independent of the event
 * `id` format (UUIDs do not sort chronologically). Stores that span
 * multiple processes (e.g. Redis) should assign globally ordered
 * sequences; single-process stores may use insertion order.
 */
export interface StoredSSEEvent {
  /** Unique event ID (also the `Last-Event-ID` cursor). */
  id: string;
  /** Monotonic ordering key. Assigned by the server or the store. */
  sequence?: number;
  /** Named event, if any. */
  event?: string;
  /** Payload. Must be JSON-serializable for out-of-process stores. */
  data: unknown;
  /** Reconnection-time hint in ms, if any. */
  retry?: number;
  /** Topic the event was broadcast to, if any. */
  topic?: string;
  /** Unix timestamp (ms) of when the event was stored. */
  createdAt: number;
}

/** Options accepted by `getAfter`. */
export interface GetAfterOptions {
  topic?: string | undefined;
  limit?: number | undefined;
}

/**
 * Async history storage contract. All methods are async so local and
 * remote (Redis, database) backends share one interface.
 *
 * Ordering guarantee: `getAfter` / `getRecent` return events in
 * ascending `sequence` (then insertion) order.
 */
export interface SSEEventStore {
  /** Persist an event. Re-appending the same `id` must be idempotent. */
  append(event: StoredSSEEvent): Promise<void>;
  /**
   * Events stored after `lastEventId` (exclusive, exact ID match),
   * oldest first. Unknown IDs yield `[]` (never the whole buffer).
   */
  getAfter(
    lastEventId: string,
    options?: GetAfterOptions,
  ): Promise<StoredSSEEvent[]>;
  /** Most recent events, oldest first, capped at `limit`. */
  getRecent(limit: number): Promise<StoredSSEEvent[]>;
  /** Remove all stored events. */
  clear(): Promise<void>;
}

/**
 * Envelope exchanged over an {@link SSEEventBus}. `origin` identifies
 * the publishing node so receivers can avoid re-publishing (loops).
 */
export interface SSEBusEnvelope {
  /** Unique ID of the publishing node. */
  origin: string;
  /** Topic for topic-scoped broadcasts, if any. */
  topic?: string;
  /** The application event. */
  event: SSEEvent;
}

/**
 * Distributed fan-out abstraction (extension point).
 *
 * Persistence alone does NOT make live broadcasting distributed: an
 * event broadcast on Node B never reaches clients on Node A unless the
 * nodes share a bus. A Redis Pub/Sub implementation lives in the
 * optional `node-sse-hub/redis` entry point.
 */
export interface SSEEventBus {
  /** Publish an envelope to every subscribed node. */
  publish(envelope: SSEBusEnvelope): Promise<void>;
  /**
   * Subscribe to envelopes from other nodes.
   * @returns An unsubscribe function.
   */
  subscribe(handler: (envelope: SSEBusEnvelope) => void): Promise<() => void>;
  /** Release bus resources (subscriptions, connections). */
  close(): Promise<void>;
}

/** Options for {@link MemoryEventStore}. */
export interface MemoryEventStoreOptions {
  /** Maximum stored events; oldest evicted first. Default `100`. */
  maxEvents?: number;
}

/** Type guard: is this the legacy synchronous history store? */
export function isLegacyHistoryStore(store: unknown): store is {
  add(entry: { id: string; frame: string }): void;
  getAfter(lastEventId: string): { id: string; frame: string }[];
  readonly size: number;
  clear(): void;
} {
  if (store === null || typeof store !== "object") return false;
  const candidate = store as Record<string, unknown>;
  return (
    typeof candidate["add"] === "function" &&
    typeof candidate["append"] !== "function"
  );
}

/**
 * Default in-memory {@link SSEEventStore}. Works without Redis or any
 * other infrastructure.
 *
 * Semantics mirror the legacy synchronous store: exact-ID-match replay,
 * unknown IDs replay nothing, bounded buffer with oldest-first eviction.
 */
// Methods stay `async` (without `await`) to satisfy the SSEEventStore
// contract; the work itself is synchronous by design (in-memory).
/* eslint-disable @typescript-eslint/require-await */
export class MemoryEventStore implements SSEEventStore {
  readonly #entries: StoredSSEEvent[] = [];
  readonly #maxEvents: number;

  constructor(options: MemoryEventStoreOptions = {}) {
    const maxEvents = options.maxEvents ?? 100;
    if (!Number.isInteger(maxEvents) || maxEvents <= 0) {
      throw new SSEError("`maxEvents` must be a positive integer.");
    }
    this.#maxEvents = maxEvents;
  }

  /** Maximum number of stored events. */
  get maxEvents(): number {
    return this.#maxEvents;
  }

  /** Current number of stored events. */
  get size(): number {
    return this.#entries.length;
  }

  async append(event: StoredSSEEvent): Promise<void> {
    this.#entries.push({ ...event });
    while (this.#entries.length > this.#maxEvents) {
      this.#entries.shift();
    }
  }

  async getAfter(
    lastEventId: string,
    options: GetAfterOptions = {},
  ): Promise<StoredSSEEvent[]> {
    const index = this.#entries.findIndex((e) => e.id === lastEventId);
    if (index === -1) return [];
    let result = this.#entries.slice(index + 1);
    if (options.topic !== undefined) {
      result = result.filter((e) => e.topic === options.topic);
    }
    if (options.limit !== undefined) {
      result = result.slice(0, options.limit);
    }
    return result.map((e) => ({ ...e }));
  }

  async getRecent(limit: number): Promise<StoredSSEEvent[]> {
    if (!Number.isInteger(limit) || limit < 0) {
      throw new SSEError("`limit` must be a non-negative integer.");
    }
    return this.#entries.slice(-limit).map((e) => ({ ...e }));
  }

  async clear(): Promise<void> {
    this.#entries.length = 0;
  }
}
/* eslint-enable @typescript-eslint/require-await */

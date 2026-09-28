/**
 * Optional Redis integrations for `sse-kit`.
 *
 * Import from the `sse-kit/redis` subpath — never from the core entry —
 * so applications that only need in-memory SSE never load Redis code:
 *
 * ```ts
 * import { RedisEventStore } from "sse-kit/redis";
 * ```
 *
 * Requires the `redis-orm-lite` package (optional peer dependency):
 *
 * ```bash
 * npm install sse-kit redis-orm-lite
 * ```
 *
 * Retry behavior (backoff, jitter, transient-error classification) is
 * provided by `redis-orm-lite`, which itself builds on `node-retry-kit`.
 * `sse-kit` never re-implements retry logic.
 */

import { connectRedis, executeRedisCommand, RedisModel } from "redis-orm-lite";
import type { OperationOptions, RedisRetryOptions } from "redis-orm-lite";
import { SSEError } from "./errors.js";
import type {
  SSEBusEnvelope,
  SSEEventBus,
  SSEEventStore,
  StoredSSEEvent,
} from "./store.js";

export type { RedisRetryOptions, OperationOptions };

/**
 * The raw Redis client type, derived from `redis-orm-lite`'s public
 * `connectRedis` return type — no direct `redis` import needed.
 */
type RawRedisClient = Awaited<ReturnType<typeof connectRedis>>;

/** Document shape stored in Redis (key: `<modelName>:<eventId>`). */
interface RedisEventDoc {
  id: string;
  sequence: number;
  event?: string;
  data: unknown;
  retry?: number;
  topic?: string;
  createdAt: number;
}

/** Options for {@link RedisEventStore}. */
export interface RedisEventStoreOptions {
  /**
   * Redis connection URL. Default `"redis://localhost:6379"`.
   * Passed to `redis-orm-lite`'s `connectRedis` (lazy, on first use).
   */
  url?: string;
  /**
   * Prefix for all keys (`<prefix>:events:*`, `<prefix>:seq`,
   * `<prefix>:index`). Default `"sse-kit"`. Use a unique prefix per
   * test run / tenant to isolate histories.
   */
  keyPrefix?: string;
  /**
   * Maximum stored events; oldest evicted first. Default `1000`.
   * The store manages its own bound — independent of the server's
   * `history.maxEvents` (which only applies to the default store).
   */
  maxEvents?: number;
  /**
   * Retry policy forwarded to every Redis round-trip (per-operation
   * override — never touches the global `redis-orm-lite` config).
   * Retry stays disabled unless `retries > 0`, exactly like
   * `redis-orm-lite` itself.
   *
   * @example
   * ```ts
   * new RedisEventStore({
   *   retry: {
   *     retries: 5,
   *     backoff: "exponential",
   *     delay: 500,
   *     maxDelay: 10_000,
   *     jitter: true,
   *   },
   * });
   * ```
   */
  retry?: RedisRetryOptions;
}

const DEFAULT_REDIS_URL = "redis://localhost:6379";
const DEFAULT_KEY_PREFIX = "sse-kit";
const DEFAULT_MAX_EVENTS = 1000;

/**
 * Redis-backed {@link SSEEventStore}, built on `redis-orm-lite`.
 *
 * Design:
 * - Event documents are stored with `RedisModel` under
 *   `<prefix>:events:<eventId>` (whole-document `SET`, idempotent by ID).
 * - Global ordering across Node instances comes from a Redis-side atomic
 *   counter (`INCR <prefix>:seq`); the ID itself is never used for
 *   sorting because UUIDs do not sort chronologically.
 * - An index sorted set (`<prefix>:index`, member = event ID,
 *   score = sequence) makes `getAfter` / `getRecent` positional lookups.
 * - Every round-trip flows through `redis-orm-lite`'s
 *   `executeRedisCommand`, so transient network failures are retried
 *   exactly when `retry.retries > 0` (powered by `node-retry-kit`
 *   underneath). Permanent errors (auth, WRONGTYPE, …) never retry.
 *
 * Connection ownership: the store manages its own connection via
 * `connectRedis` (lazy, on first use) and reuses the returned client for
 * index/counter operations, while `RedisModel` uses the same global
 * client internally. If your application also uses `redis-orm-lite`
 * directly, connect the store first and use the same URL so both share
 * one connection.
 */
export class RedisEventStore implements SSEEventStore {
  readonly #url: string;
  readonly #prefix: string;
  readonly #maxEvents: number;
  readonly #retry: RedisRetryOptions | undefined;
  readonly #modelName: string;
  readonly #model: RedisModel<RedisEventDoc>;
  #raw: RawRedisClient | undefined;

  constructor(options: RedisEventStoreOptions = {}) {
    this.#url = options.url ?? DEFAULT_REDIS_URL;
    this.#prefix = options.keyPrefix ?? DEFAULT_KEY_PREFIX;
    const maxEvents = options.maxEvents ?? DEFAULT_MAX_EVENTS;
    if (!Number.isInteger(maxEvents) || maxEvents <= 0) {
      throw new SSEError("`maxEvents` must be a positive integer.");
    }
    this.#maxEvents = maxEvents;
    this.#retry = options.retry;
    this.#modelName = `${this.#prefix}:events`;
    this.#model = new RedisModel<RedisEventDoc>(this.#modelName);
  }

  /** Redis URL this store connects to. */
  get url(): string {
    return this.#url;
  }

  /** Key prefix isolating this store's keys. */
  get keyPrefix(): string {
    return this.#prefix;
  }

  /** Maximum stored events before oldest-first eviction. */
  get maxEvents(): number {
    return this.#maxEvents;
  }

  get #counterKey(): string {
    return `${this.#prefix}:seq`;
  }

  get #indexKey(): string {
    return `${this.#prefix}:index`;
  }

  /**
   * Connect to Redis now. Optional — the first store operation connects
   * lazily. Safe to call repeatedly.
   */
  async connect(): Promise<void> {
    if (this.#raw !== undefined) return;
    this.#raw = await connectRedis(this.#url);
  }

  /**
   * Disconnect this store's Redis client. Only call this when no other
   * code (other stores, buses, application queries sharing the
   * `redis-orm-lite` global client) still needs the connection.
   */
  async close(): Promise<void> {
    const raw = this.#raw;
    this.#raw = undefined;
    if (raw === undefined) return;
    try {
      await raw.quit();
    } catch {
      // Already closed — nothing to release.
    }
  }

  #operationOptions(): OperationOptions | undefined {
    return this.#retry === undefined ? undefined : { retry: this.#retry };
  }

  async #client(): Promise<RawRedisClient> {
    await this.connect();
    const raw = this.#raw;
    if (raw === undefined) {
      throw new SSEError("Redis client not initialized.");
    }
    return raw;
  }

  /**
   * Persist an event. Idempotent by event ID: re-appending the same ID
   * overwrites the document and refreshes its index entry instead of
   * creating a duplicate logical event (the sequence advances, so a
   * re-appended event sorts at its newest position).
   */
  async append(event: StoredSSEEvent): Promise<void> {
    if (typeof event.id !== "string" || event.id.length === 0) {
      throw new SSEError("Stored event must have a non-empty string `id`.");
    }
    const client = await this.#client();
    const op = this.#operationOptions();
    const sequence = await executeRedisCommand(
      "incr",
      () => client.incr(this.#counterKey),
      op,
    );
    const doc: RedisEventDoc = {
      id: event.id,
      sequence,
      data: event.data,
      createdAt: event.createdAt,
    };
    if (event.event !== undefined) doc.event = event.event;
    if (event.retry !== undefined) doc.retry = event.retry;
    if (event.topic !== undefined) doc.topic = event.topic;
    await this.#model.create(doc, op);
    await executeRedisCommand(
      "zadd",
      () => client.zAdd(this.#indexKey, [{ score: sequence, value: event.id }]),
      op,
    );
    await this.#trim(client, op);
  }

  /** Evict entries beyond `maxEvents`, oldest first. */
  async #trim(
    client: RawRedisClient,
    op: OperationOptions | undefined,
  ): Promise<void> {
    const count = await executeRedisCommand(
      "zcard",
      () => client.zCard(this.#indexKey),
      op,
    );
    const overflow = count - this.#maxEvents;
    if (overflow <= 0) return;
    // Collect evicted IDs before trimming so their documents can be
    // deleted too. A crash between index-trim and doc-delete only leaves
    // orphaned documents, which are invisible to reads (all reads go
    // through the index).
    const evicted = await executeRedisCommand(
      "zrange",
      () => client.zRange(this.#indexKey, 0, overflow - 1),
      op,
    );
    await executeRedisCommand(
      "zremrangebyrank",
      () => client.zRemRangeByRank(this.#indexKey, 0, overflow - 1),
      op,
    );
    for (const id of evicted) {
      await executeRedisCommand(
        "del",
        () => client.del(`${this.#modelName}:${id}`),
        op,
      );
    }
  }

  async getAfter(
    lastEventId: string,
    options: { topic?: string; limit?: number } = {},
  ): Promise<StoredSSEEvent[]> {
    const op = this.#operationOptions();
    const anchor = await this.#model.findById(lastEventId, op);
    if (anchor === null || typeof anchor.sequence !== "number") return [];
    const client = await this.#client();
    const ids = await executeRedisCommand(
      "zrangebyscore",
      () => client.zRangeByScore(this.#indexKey, `(${anchor.sequence}`, "+inf"),
      op,
    );
    const docs = await Promise.all(
      ids.map((id) => this.#model.findById(id, op)),
    );
    let events = docs
      .filter((doc): doc is RedisEventDoc => doc !== null)
      .sort((a, b) => a.sequence - b.sequence)
      .map(toStoredEvent);
    if (options.topic !== undefined) {
      events = events.filter((e) => e.topic === options.topic);
    }
    if (options.limit !== undefined) {
      events = events.slice(0, options.limit);
    }
    return events;
  }

  async getRecent(limit: number): Promise<StoredSSEEvent[]> {
    if (!Number.isInteger(limit) || limit < 0) {
      throw new SSEError("`limit` must be a non-negative integer.");
    }
    if (limit === 0) return [];
    const client = await this.#client();
    const op = this.#operationOptions();
    const ids = await executeRedisCommand(
      "zrange",
      () => client.zRange(this.#indexKey, 0, limit - 1, { REV: true }),
      op,
    );
    const docs = await Promise.all(
      ids.map((id) => this.#model.findById(id, op)),
    );
    return docs
      .filter((doc): doc is RedisEventDoc => doc !== null)
      .sort((a, b) => a.sequence - b.sequence)
      .map(toStoredEvent);
  }

  async clear(): Promise<void> {
    const client = await this.#client();
    const op = this.#operationOptions();
    const pattern = `${this.#modelName}:*`;
    let cursor = 0;
    do {
      const result = await executeRedisCommand(
        "scan",
        () => client.scan(cursor, { MATCH: pattern, COUNT: 100 }),
        op,
      );
      cursor = result.cursor;
      for (const key of result.keys) {
        await executeRedisCommand("del", () => client.del(key), op);
      }
    } while (cursor !== 0);
    await executeRedisCommand("del", () => client.del(this.#indexKey), op);
    await executeRedisCommand("del", () => client.del(this.#counterKey), op);
  }
}

function toStoredEvent(doc: RedisEventDoc): StoredSSEEvent {
  const event: StoredSSEEvent = {
    id: doc.id,
    sequence: doc.sequence,
    data: doc.data,
    createdAt: doc.createdAt,
  };
  if (doc.event !== undefined) event.event = doc.event;
  if (doc.retry !== undefined) event.retry = doc.retry;
  if (doc.topic !== undefined) event.topic = doc.topic;
  return event;
}

/** Options for {@link RedisEventBus}. */
export interface RedisEventBusOptions {
  /** Redis connection URL. Default `"redis://localhost:6379"`. */
  url?: string;
  /** Pub/Sub channel. Default `"sse-kit:bus"`. */
  channel?: string;
}

const DEFAULT_BUS_CHANNEL = "sse-kit:bus";

function isBusEnvelope(value: unknown): value is SSEBusEnvelope {
  if (value === null || typeof value !== "object") return false;
  const candidate = value as Record<string, unknown>;
  if (typeof candidate["origin"] !== "string") return false;
  const event = candidate["event"];
  return event !== null && typeof event === "object";
}

/**
 * Redis Pub/Sub {@link SSEEventBus}: nodes publish envelopes on a
 * channel; every subscribed node delivers them to its local clients.
 *
 * Loop prevention is handled by the server via `envelope.origin`, not
 * here — this class is a thin transport. Malformed messages are
 * ignored, never thrown.
 */
export class RedisEventBus implements SSEEventBus {
  readonly #url: string;
  readonly #channel: string;
  #raw: RawRedisClient | undefined;
  #subscriber: RawRedisClient | undefined;

  constructor(options: RedisEventBusOptions = {}) {
    this.#url = options.url ?? DEFAULT_REDIS_URL;
    this.#channel = options.channel ?? DEFAULT_BUS_CHANNEL;
  }

  /** Pub/Sub channel in use. */
  get channel(): string {
    return this.#channel;
  }

  /**
   * Connect the publisher now. Optional — the first `publish` or
   * `subscribe` connects lazily. Safe to call repeatedly.
   */
  async connect(): Promise<void> {
    await this.#publisher();
  }

  async #publisher(): Promise<RawRedisClient> {
    if (this.#raw === undefined) {
      this.#raw = await connectRedis(this.#url);
    }
    return this.#raw;
  }

  async publish(envelope: SSEBusEnvelope): Promise<void> {
    const client = await this.#publisher();
    await executeRedisCommand("publish", () =>
      client.publish(this.#channel, JSON.stringify(envelope)),
    );
  }

  async subscribe(
    handler: (envelope: SSEBusEnvelope) => void,
  ): Promise<() => void> {
    const client = await this.#publisher();
    await this.#releaseSubscriber();
    const subscriber = client.duplicate();
    await subscriber.connect();
    await subscriber.subscribe(this.#channel, (message: string) => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(message) as unknown;
      } catch {
        return;
      }
      if (isBusEnvelope(parsed)) handler(parsed);
    });
    this.#subscriber = subscriber;
    return () => {
      void this.#releaseSubscriber().catch(() => {
        // Unsubscribe is best-effort during teardown.
      });
    };
  }

  async #releaseSubscriber(): Promise<void> {
    const subscriber = this.#subscriber;
    this.#subscriber = undefined;
    if (subscriber === undefined) return;
    try {
      await subscriber.unsubscribe(this.#channel);
    } finally {
      try {
        await subscriber.quit();
      } catch {
        // Already gone — nothing to release.
      }
    }
  }

  /**
   * Release the subscription (and its dedicated connection) as well as
   * this bus's publisher client, so a process using only the bus can
   * exit cleanly. If other code shares `redis-orm-lite`'s global client
   * (e.g. a `RedisEventStore` connected afterwards, or direct model
   * use), close this bus last — each instance only quits the client it
   * opened itself.
   */
  async close(): Promise<void> {
    await this.#releaseSubscriber();
    const raw = this.#raw;
    this.#raw = undefined;
    if (raw === undefined) return;
    try {
      await raw.quit();
    } catch {
      // Already closed — nothing to release.
    }
  }
}

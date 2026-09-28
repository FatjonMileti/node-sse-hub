/**
 * Public TypeScript types for `node-sse-hub`.
 *
 * The core package only depends on Node.js HTTP primitives so it can be
 * used with Express, Fastify, Node's native HTTP server, or any other
 * framework that exposes `IncomingMessage` / `ServerResponse` objects.
 */

import type { IncomingMessage, ServerResponse } from "node:http";
import type { SSEConnection } from "./SSEConnection.js";
import type { SSEEventBus, SSEEventStore } from "./store.js";

export type { IncomingMessage, ServerResponse };

/** A topic (room) name. Must be a non-empty string. */
export type Topic = string;

/**
 * An event to send over SSE.
 *
 * @example
 * ```ts
 * sse.broadcast({ event: "invoice-created", data: { invoiceId: "123" } });
 * ```
 */
export interface SSEEvent<T = unknown> {
  /** Optional event ID used for `Last-Event-ID` reconnection. */
  id?: string;
  /** Optional named event (maps to `EventSource.addEventListener(name, …)`). */
  event?: string;
  /** Payload. Objects are JSON-serialized by default, strings sent as-is. */
  data: T;
  /** Optional reconnection time hint (milliseconds) sent as `retry:`. */
  retry?: number;
}

/** Serializes non-string `data` payloads. Defaults to `JSON.stringify`. */
export type SSESerializer = (data: unknown) => string;

/** Strategy applied when a slow client's buffer exceeds `maxBufferedEvents`. */
export type SlowClientStrategy = "disconnect" | "drop-oldest";

/**
 * A single stored history entry: the assigned event ID plus the exact
 * pre-formatted SSE frame, so replay is byte-identical to the original send.
 */
export interface SSEHistoryEntry {
  id: string;
  frame: string;
}

/**
 * Pluggable history storage. The default implementation keeps the last
 * `maxEvents` entries in memory. Implement this interface to persist
 * history elsewhere (file, Redis, …).
 */
export interface SSEHistoryStore {
  /** Append an entry, evicting oldest entries beyond capacity. */
  add(entry: SSEHistoryEntry): void;
  /** Entries stored after `lastEventId` (exclusive), in insertion order. */
  getAfter(lastEventId: string): SSEHistoryEntry[];
  /** Current number of stored entries. */
  readonly size: number;
  /** Remove all stored entries. */
  clear(): void;
}

/** Options controlling optional event history / replay. */
export interface HistoryOptions {
  /** Enable history storage and `Last-Event-ID` replay. Default `false`. */
  enabled?: boolean;
  /**
   * Maximum stored events (used only when the server creates the default
   * synchronous in-memory store). Custom stores manage their own bounds.
   * Default `100`.
   */
  maxEvents?: number;
  /**
   * Custom storage backend. Accepts either the legacy synchronous
   * {@link SSEHistoryStore} (pre-formatted frames) or the async
   * {@link SSEEventStore} (structured events, e.g. `MemoryEventStore`
   * or the Redis adapter from `node-sse-hub/redis`). Defaults to an
   * in-memory synchronous store.
   */
  store?: SSEHistoryStore | SSEEventStore;
}

/** Options controlling automatic heartbeat (keep-alive) comments. */
export interface HeartbeatOptions {
  /** Interval in milliseconds. `0` or omitted disables heartbeats. */
  interval?: number;
  /** Comment text sent as `: <comment>`. Default `"heartbeat"`. */
  comment?: string;
}

/** Context passed to `connection` listeners when a client connects. */
export interface SSEConnectionContext {
  /**
   * Value of the client's `Last-Event-ID` request header, if present.
   * Use it to detect reconnections. Note: automatic replay of missed
   * events only happens when `history.enabled` is `true`.
   */
  lastEventId?: string;
}

/** Options accepted by {@link SSEServer.connect}. */
export interface ConnectOptions {
  /** Topics to subscribe the new connection to immediately. */
  topics?: Topic[];
}

/** Options accepted by the {@link SSEServer} constructor. */
export interface SSEServerOptions {
  /**
   * Automatically assign incrementing numeric IDs (`"1"`, `"2"`, …) to
   * events that do not carry an explicit `id`. Default `false`.
   *
   * Note: when `history.enabled` is `true`, IDs are always assigned
   * (using the same counter) so replay positioning works, even if this
   * flag is `false`. This is documented in the README.
   */
  generateEventId?: boolean;
  /**
   * Heartbeat interval in milliseconds. Heartbeats are SSE comments
   * (`: heartbeat`) that keep proxies/load balancers from treating the
   * connection as idle. `0` (default) disables heartbeats.
   */
  heartbeatInterval?: number;
  /** Text used for heartbeat comments. Default `"heartbeat"`. */
  heartbeatComment?: string;
  /** Optional event history / replay configuration. */
  history?: HistoryOptions;
  /**
   * Maximum queued frames per connection while the socket applies
   * backpressure. Default `100`. See `slowClientStrategy`.
   */
  maxBufferedEvents?: number;
  /**
   * What to do when a connection's buffer exceeds `maxBufferedEvents`:
   * - `"disconnect"` (default): close the slow connection.
   * - `"drop-oldest"`: discard the oldest queued frames to make room.
   */
  slowClientStrategy?: SlowClientStrategy;
  /**
   * Maximum number of simultaneous connections. New connections beyond
   * this limit receive HTTP 503 and an error is thrown. Default `10000`.
   */
  maxConnections?: number;
  /**
   * Maximum number of topic subscriptions per connection.
   * Default `100`. Prevents unbounded topic growth from one client.
   */
  maxTopicsPerConnection?: number;
  /**
   * Maximum serialized SSE frame size in bytes. Larger frames are
   * rejected with an `SSEError`. Default `1_048_576` (1 MiB).
   */
  maxEventBytes?: number;
  /** Custom serializer for non-string `data`. Defaults to `JSON.stringify`. */
  serialize?: SSESerializer;
  /**
   * Optional distributed event bus (e.g. the Redis adapter from
   * `node-sse-hub/redis`). When set, broadcasts are also published to the bus
   * and envelopes received from other nodes are delivered locally.
   * Core package has no bus implementation — this is an extension point.
   */
  bus?: SSEEventBus;
  /**
   * Stable ID for this node on the bus (used for loop prevention).
   * Defaults to a random UUID.
   */
  nodeId?: string;
}

/** Listener for new connections: `(connection, context) => void`. */
export type ConnectionListener = (
  connection: SSEConnection,
  context: SSEConnectionContext,
) => void;

/** Listener for disconnections: `(connection) => void`. */
export type DisconnectListener = (connection: SSEConnection) => void;

/** Listener for errors: `(error, connection?) => void`. */
export type ErrorListener = (error: Error, connection?: SSEConnection) => void;

/** Listener for async history-store failures: `(error) => void`. */
export type StorageErrorListener = (error: Error) => void;

/** Listener for event-bus failures: `(error) => void`. */
export type BusErrorListener = (error: Error) => void;

/** Scoped broadcaster returned by `sse.to(topic)`. */
export interface TopicBroadcaster {
  /** Broadcast an event to all subscribers of the topic. Returns recipient count. */
  broadcast<T>(event: SSEEvent<T>): number;
}

/** Snapshot of server state, useful for health checks and debugging. */
export interface SSEServerStats {
  connections: number;
  topics: number;
  historySize: number;
  closed: boolean;
}

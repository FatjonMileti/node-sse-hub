/**
 * The main entry point of `node-sse-hub`: creates, tracks, and cleans up SSE
 * connections, and routes events to individual clients, all clients, or
 * topic subscribers.
 *
 * History storage and inter-node fan-out are both pluggable: the server
 * depends on the {@link SSEEventStore} / {@link SSEEventBus} abstractions
 * and never on Redis or any other infrastructure.
 */

import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";
import { SSEConnection } from "./SSEConnection.js";
import { SSEClosedError, SSEError } from "./errors.js";
import {
  formatHeartbeat,
  formatSSEFrame,
  InMemoryHistoryStore,
  parseLastEventId,
} from "./SSEEvent.js";
import {
  isLegacyHistoryStore,
  type SSEBusEnvelope,
  type SSEEventBus,
  type SSEEventStore,
  type StoredSSEEvent,
} from "./store.js";
import { TopicManager } from "./TopicManager.js";
import type {
  BroadcastOptions,
  BusErrorListener,
  ConnectOptions,
  ConnectionListener,
  DisconnectListener,
  ErrorListener,
  SSEConnectionContext,
  SSEEvent,
  SSEHistoryStore,
  SSESerializer,
  SSEServerOptions,
  SSEServerResolvedOptions,
  SSEServerStats,
  SlowClientStrategy,
  StorageErrorListener,
  Topic,
  TopicBroadcaster,
} from "./types.js";

const DEFAULT_MAX_BUFFERED_EVENTS = 100;
const DEFAULT_MAX_CONNECTIONS = 10_000;
const DEFAULT_MAX_TOPICS_PER_CONNECTION = 100;
const DEFAULT_MAX_EVENT_BYTES = 1_048_576; // 1 MiB
const DEFAULT_MAX_HISTORY_EVENTS = 100;
const DEFAULT_HEARTBEAT_COMMENT = "heartbeat";

export type SSEServerEventMap = {
  connection: [connection: SSEConnection, context: SSEConnectionContext];
  disconnect: [connection: SSEConnection];
  error: [error: Error, connection?: SSEConnection];
  storageError: [error: Error];
  busError: [error: Error];
};

function asError(error: unknown): Error {
  return error instanceof Error ? error : new SSEError(String(error));
}

/**
 * Framework-agnostic SSE server. Works with Express, Fastify, Node's
 * native HTTP server, or anything exposing Node `IncomingMessage` /
 * `ServerResponse` objects.
 *
 * Authentication/authorization is intentionally out of scope — handle it
 * in your framework (middleware, hooks, …) before calling `connect()`.
 *
 * @example
 * ```ts
 * import { SSEServer } from "node-sse-hub";
 *
 * const sse = new SSEServer({ heartbeatInterval: 30_000 });
 *
 * app.get("/events", (req, res) => {
 *   sse.connect(req, res);
 * });
 *
 * sse.broadcast({ event: "ping", data: { ok: true } });
 * ```
 */
export class SSEServer extends EventEmitter {
  readonly #connections = new Map<string, SSEConnection>();
  readonly #topics = new TopicManager();
  readonly #history: SSEHistoryStore | SSEEventStore;
  readonly #asyncStore: SSEEventStore | undefined;
  readonly #historyEnabled: boolean;
  readonly #bus: SSEEventBus | undefined;
  readonly #nodeId: string;

  readonly #generateEventId: boolean;
  readonly #heartbeatInterval: number;
  readonly #heartbeatComment: string;
  readonly #maxBufferedEvents: number;
  readonly #slowClientStrategy: SlowClientStrategy;
  readonly #maxConnections: number;
  readonly #maxTopicsPerConnection: number;
  readonly #maxEventBytes: number;
  readonly #serialize: SSESerializer;

  #nextEventId = 1;
  #nextSequence = 1;
  #heartbeatTimer: NodeJS.Timeout | undefined;
  #closed = false;
  #closePromise: Promise<void> | undefined;
  #persistQueue: Promise<void> = Promise.resolve();
  #unsubscribeBus: (() => void) | undefined;

  constructor(options: SSEServerOptions = {}) {
    super();
    this.#generateEventId = options.generateEventId ?? false;
    this.#heartbeatInterval = options.heartbeatInterval ?? 0;
    this.#heartbeatComment =
      options.heartbeatComment ?? DEFAULT_HEARTBEAT_COMMENT;
    this.#maxBufferedEvents =
      options.maxBufferedEvents ?? DEFAULT_MAX_BUFFERED_EVENTS;
    this.#slowClientStrategy = options.slowClientStrategy ?? "disconnect";
    this.#maxConnections = options.maxConnections ?? DEFAULT_MAX_CONNECTIONS;
    this.#maxTopicsPerConnection =
      options.maxTopicsPerConnection ?? DEFAULT_MAX_TOPICS_PER_CONNECTION;
    this.#maxEventBytes = options.maxEventBytes ?? DEFAULT_MAX_EVENT_BYTES;
    this.#serialize =
      options.serialize ?? ((data: unknown) => JSON.stringify(data));

    validatePositiveInt(this.#maxBufferedEvents, "maxBufferedEvents");
    validatePositiveInt(this.#maxConnections, "maxConnections");
    validatePositiveInt(this.#maxTopicsPerConnection, "maxTopicsPerConnection");
    validatePositiveInt(this.#maxEventBytes, "maxEventBytes");
    if (
      !Number.isInteger(this.#heartbeatInterval) ||
      this.#heartbeatInterval < 0
    ) {
      throw new SSEError("`heartbeatInterval` must be a non-negative integer.");
    }

    const historyOptions = options.history ?? {};
    this.#historyEnabled = historyOptions.enabled ?? false;
    const maxEvents = historyOptions.maxEvents ?? DEFAULT_MAX_HISTORY_EVENTS;
    if (this.#historyEnabled && historyOptions.store !== undefined) {
      this.#history = historyOptions.store;
    } else {
      this.#history = new InMemoryHistoryStore(maxEvents);
    }
    this.#asyncStore = isLegacyHistoryStore(this.#history)
      ? undefined
      : this.#history;

    this.#bus = options.bus;
    this.#nodeId = options.nodeId ?? randomUUID();
    if (this.#bus !== undefined) {
      const bus = this.#bus;
      void bus
        .subscribe((envelope) => {
          this.#ingestRemote(envelope);
        })
        .then((unsubscribe) => {
          this.#unsubscribeBus = unsubscribe;
        })
        .catch((error: unknown) => {
          this.emit("busError", asError(error));
        });
    }

    if (this.#heartbeatInterval > 0) {
      this.#startHeartbeat();
    }
  }

  // ------------------------------------------------------------------ setup

  /** Whether {@link close} has been called. */
  get closed(): boolean {
    return this.#closed;
  }

  /** Number of currently connected clients. */
  get connectionCount(): number {
    return this.#connections.size;
  }

  /** This node's ID on the event bus (for loop prevention). */
  get nodeId(): string {
    return this.#nodeId;
  }

  /**
   * The configured history store (legacy sync or async), regardless of
   * whether history is enabled.
   */
  getEventStore(): SSEHistoryStore | SSEEventStore {
    return this.#history;
  }

  /**
   * Resolve when every history write enqueued so far has settled.
   * Useful in tests and for write-through guarantees before shutdown.
   * Never rejects — persist failures surface via `"storageError"`.
   */
  flushHistory(): Promise<void> {
    return this.#persistQueue;
  }

  // -------------------------------------------------------------- lifecycle

  /**
   * Accept an SSE connection. Sets the required SSE headers, registers
   * disconnect detection, replays missed events when history is enabled
   * and the client sent `Last-Event-ID`, and emits `"connection"`.
   *
   * Replay from synchronous stores happens inline; replay from async
   * stores (Redis, …) resolves shortly after `connect()` returns. Events
   * broadcast concurrently with a reconnect may arrive before replayed
   * history — clients should order/dedupe by event `id`.
   *
   * @returns The new connection (with a unique `connection.id`).
   * @throws {@link SSEClosedError} if the server is closed.
   * @throws {@link SSEError} when the connection limit is exceeded.
   */
  connect(
    req: IncomingMessage,
    res: ServerResponse,
    options: ConnectOptions = {},
  ): SSEConnection {
    if (this.#closed) {
      throw new SSEClosedError("connect");
    }
    if (this.#connections.size >= this.#maxConnections) {
      try {
        res.writeHead(503, { "Content-Type": "text/plain" });
        res.end("Service Unavailable: too many SSE connections");
      } catch {
        // Ignore — the socket may already be gone.
      }
      throw new SSEError(
        `SSE connection limit reached (${this.#maxConnections}).`,
      );
    }

    const lastEventId = parseLastEventId(req.headers);
    const context: SSEConnectionContext = {};
    if (lastEventId !== undefined) context.lastEventId = lastEventId;

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    if (typeof res.flushHeaders === "function") res.flushHeaders();

    const connection = new SSEConnection({
      req,
      res,
      topics: this.#topics,
      context,
      initialTopics: options.topics,
      serialize: this.#serialize,
      maxBufferedEvents: this.#maxBufferedEvents,
      slowClientStrategy: this.#slowClientStrategy,
      maxTopicsPerConnection: this.#maxTopicsPerConnection,
      onCleanup: (conn) => {
        this.#removeConnection(conn);
      },
    });

    this.#connections.set(connection.id, connection);

    // Reconnection may race with an in-flight close of either side.
    const cleanup = (): void => {
      connection.close();
    };
    req.on("close", cleanup);
    res.on("close", cleanup);

    // Replay missed events when the client is reconnecting.
    if (lastEventId !== undefined && this.#historyEnabled) {
      if (this.#asyncStore !== undefined) {
        void this.#replayFromStore(connection, lastEventId);
      } else if (isLegacyHistoryStore(this.#history)) {
        for (const entry of this.#history.getAfter(lastEventId)) {
          if (connection.closed) break;
          connection.writeFrame(entry.frame);
        }
      }
    }

    this.emit("connection", connection, context);
    return connection;
  }

  #removeConnection(connection: SSEConnection): void {
    if (!this.#connections.has(connection.id)) return;
    this.#connections.delete(connection.id);
    this.#topics.removeConnection(connection.id);
    this.emit("disconnect", connection);
  }

  // ---------------------------------------------------------------- events

  /**
   * Assign an event ID according to the server configuration.
   * Returns the ID to use, or `undefined` when no ID applies.
   *
   * IDs are always assigned when history is enabled (so replay
   * positioning works), and when `generateEventId` is `true`. Explicit
   * IDs always win and advance the counter past numeric collisions.
   */
  #assignId<T>(event: SSEEvent<T>, forHistory: boolean): string | undefined {
    if (event.id !== undefined) {
      const numeric = Number(event.id);
      if (
        Number.isInteger(numeric) &&
        numeric >= this.#nextEventId &&
        event.id.trim() !== "" &&
        String(numeric) === event.id
      ) {
        this.#nextEventId = numeric + 1;
      }
      return event.id;
    }
    if (this.#generateEventId || (forHistory && this.#historyEnabled)) {
      const id = String(this.#nextEventId);
      this.#nextEventId += 1;
      return id;
    }
    return undefined;
  }

  /**
   * Format an event (assigning an ID when configured) and enforce the
   * `maxEventBytes` limit. Returns the ID used, the wire frame, and the
   * ID-stamped event (for history persistence and bus fan-out).
   */
  #prepare<T>(
    event: SSEEvent<T>,
    forHistory: boolean,
  ): { id: string | undefined; frame: string; stamped: SSEEvent<T> } {
    const id = this.#assignId(event, forHistory);
    const stamped =
      id === undefined || event.id !== undefined ? event : { ...event, id };
    const frame = formatSSEFrame(stamped, { serialize: this.#serialize });
    if (Buffer.byteLength(frame, "utf8") > this.#maxEventBytes) {
      throw new SSEError(
        `SSE frame exceeds the limit of ${this.#maxEventBytes} bytes.`,
      );
    }
    return { id, frame, stamped };
  }

  #deliver(connection: SSEConnection, frame: string): boolean {
    try {
      return connection.writeFrame(frame);
    } catch (error) {
      this.emit("error", asError(error), connection);
      connection.close();
      return false;
    }
  }

  /**
   * Record an event in history. Synchronous legacy stores are written
   * inline; async stores are appended through a serialized queue so
   * persistence preserves broadcast order. Storage failures never break
   * live delivery — they surface via `"storageError"`.
   */
  #recordHistory<T>(
    id: string | undefined,
    frame: string,
    stamped: SSEEvent<T>,
    topic?: string,
  ): void {
    if (!this.#historyEnabled || id === undefined) return;
    const asyncStore = this.#asyncStore;
    if (asyncStore === undefined) {
      if (isLegacyHistoryStore(this.#history)) {
        this.#history.add({ id, frame });
      }
      return;
    }
    const stored: StoredSSEEvent = {
      id,
      sequence: this.#nextSequence,
      data: stamped.data,
      createdAt: Date.now(),
    };
    this.#nextSequence += 1;
    if (stamped.event !== undefined) stored.event = stamped.event;
    if (stamped.retry !== undefined) stored.retry = stamped.retry;
    if (topic !== undefined) stored.topic = topic;
    this.#persistQueue = this.#persistQueue.then(async () => {
      try {
        await asyncStore.append(stored);
      } catch (error) {
        this.emit("storageError", asError(error));
      }
    });
  }

  /** Replay missed events from an async store to a reconnecting client. */
  async #replayFromStore(
    connection: SSEConnection,
    lastEventId: string,
  ): Promise<void> {
    const store = this.#asyncStore;
    if (store === undefined) return;
    let events: StoredSSEEvent[];
    try {
      events = await store.getAfter(lastEventId);
    } catch (error) {
      this.emit("storageError", asError(error));
      return;
    }
    for (const stored of events) {
      if (connection.closed || this.#closed) break;
      const toSend: SSEEvent = { data: stored.data };
      toSend.id = stored.id;
      if (stored.event !== undefined) toSend.event = stored.event;
      if (stored.retry !== undefined) toSend.retry = stored.retry;
      let frame: string;
      try {
        frame = formatSSEFrame(toSend, { serialize: this.#serialize });
      } catch (error) {
        this.emit("storageError", asError(error));
        continue;
      }
      this.#deliver(connection, frame);
    }
  }

  /** Publish an envelope to the bus without breaking live delivery. */
  #publishToBus(envelope: SSEBusEnvelope): void {
    if (this.#bus === undefined || this.#closed) return;
    void this.#bus.publish(envelope).catch((error: unknown) => {
      this.emit("busError", asError(error));
    });
  }

  /**
   * Deliver an envelope received from another node: fan out locally and
   * record in local history, but never re-publish (loop prevention via
   * the envelope's `origin`).
   */
  #ingestRemote(envelope: SSEBusEnvelope): void {
    if (this.#closed || envelope.origin === this.#nodeId) return;
    let prepared: {
      id: string | undefined;
      frame: string;
      stamped: SSEEvent;
    };
    try {
      prepared = this.#prepare(envelope.event, true);
    } catch (error) {
      this.emit("busError", asError(error));
      return;
    }
    const { id, frame, stamped } = prepared;
    const excluded =
      envelope.exceptConnectionIds !== undefined
        ? new Set(envelope.exceptConnectionIds)
        : undefined;
    if (envelope.topic !== undefined) {
      for (const connectionId of this.#topics.getSubscribers(envelope.topic)) {
        if (excluded?.has(connectionId)) continue;
        const connection = this.#connections.get(connectionId);
        if (connection !== undefined) this.#deliver(connection, frame);
      }
    } else {
      for (const connection of [...this.#connections.values()]) {
        if (excluded?.has(connection.id)) continue;
        this.#deliver(connection, frame);
      }
    }
    this.#recordHistory(id, frame, stamped, envelope.topic);
  }

  /**
   * Broadcast an event to **all** connected clients.
   *
   * Pass `{ exceptConnectionIds }` for optimistic-UI fan-out: the
   * listed connections are skipped locally *and* on every node reached
   * over the bus, while history recording and bus publishing proceed
   * exactly as for a full broadcast. Unknown IDs are ignored.
   *
   * Consistency model: live delivery happens first and synchronously;
   * history persistence follows asynchronously. A storage failure never
   * fails the broadcast — it surfaces via `"storageError"`.
   *
   * @returns Number of clients the event was accepted by (skipped
   * connections are not counted).
   */
  broadcast<T>(event: SSEEvent<T>, options: BroadcastOptions = {}): number {
    if (this.#closed) return 0;
    const excluded =
      options.exceptConnectionIds !== undefined
        ? new Set(options.exceptConnectionIds)
        : undefined;
    const { id, frame, stamped } = this.#prepare(event, true);
    let count = 0;
    for (const connection of [...this.#connections.values()]) {
      if (excluded?.has(connection.id)) continue;
      if (this.#deliver(connection, frame)) count += 1;
    }
    this.#recordHistory(id, frame, stamped);
    this.#publishToBus({
      origin: this.#nodeId,
      event: stamped,
      ...(excluded !== undefined ? { exceptConnectionIds: [...excluded] } : {}),
    });
    return count;
  }

  /** Alias for {@link broadcast} (also accepts `BroadcastOptions`). */
  send<T>(event: SSEEvent<T>, options: BroadcastOptions = {}): number {
    return this.broadcast(event, options);
  }

  /**
   * Send an event to a single connection.
   * @returns `true` if the connection exists and accepted the event.
   *
   * Note: direct messages are intentionally **not** stored in history
   * and are **not** published to the bus (history is a global log —
   * storing private messages there would replay them to whoever
   * reconnects next). Documented in the README.
   */
  sendTo<T>(connectionId: string, event: SSEEvent<T>): boolean {
    const connection = this.#connections.get(connectionId);
    if (connection === undefined) return false;
    if (this.#closed) return false;
    const { frame } = this.#prepare(event, false);
    return this.#deliver(connection, frame);
  }

  /**
   * Return a scoped broadcaster for a topic. Topic broadcasts are stored
   * in the (global, positional — not per-topic) history like broadcasts,
   * tagged with the topic when the store supports it.
   */
  to(topic: Topic): TopicBroadcaster {
    return {
      broadcast: <T>(event: SSEEvent<T>): number => {
        if (this.#closed) return 0;
        const { id, frame, stamped } = this.#prepare(event, true);
        let count = 0;
        for (const connectionId of this.#topics.getSubscribers(topic)) {
          const connection = this.#connections.get(connectionId);
          if (connection !== undefined && this.#deliver(connection, frame)) {
            count += 1;
          }
        }
        this.#recordHistory(id, frame, stamped, topic);
        this.#publishToBus({ origin: this.#nodeId, topic, event: stamped });
        return count;
      },
    };
  }

  // ------------------------------------------------------------- connections

  /** Look up a connection by ID. Returns `undefined` when unknown. */
  getConnection(connectionId: string): SSEConnection | undefined {
    return this.#connections.get(connectionId);
  }

  /** All currently connected clients. */
  getConnections(): SSEConnection[] {
    return [...this.#connections.values()];
  }

  /**
   * Disconnect a client by ID. Safe for unknown IDs (returns `false`)
   * and idempotent.
   */
  disconnect(connectionId: string): boolean {
    const connection = this.#connections.get(connectionId);
    if (connection === undefined) return false;
    connection.close();
    return true;
  }

  // ------------------------------------------------------------------ topics

  /** Topics that currently have at least one subscriber. */
  getTopics(): Topic[] {
    return this.#topics.topics();
  }

  /** Connections subscribed to a topic (empty array when none). */
  getTopicSubscribers(topic: Topic): SSEConnection[] {
    const result: SSEConnection[] = [];
    for (const id of this.#topics.getSubscribers(topic)) {
      const connection = this.#connections.get(id);
      if (connection !== undefined) result.push(connection);
    }
    return result;
  }

  /** Number of subscribers of a topic. */
  getTopicSubscriberCount(topic: Topic): number {
    return this.#topics.getSubscribers(topic).length;
  }

  // ------------------------------------------------------------------ stats

  /**
   * The effective configuration after defaults are applied. Useful for
   * asserting setup in tests (e.g. `getOptions().heartbeatInterval`)
   * without waiting out real timers, and for logging startup config.
   * Returns a fresh object on every call.
   */
  getOptions(): SSEServerResolvedOptions {
    return {
      generateEventId: this.#generateEventId,
      heartbeatInterval: this.#heartbeatInterval,
      heartbeatComment: this.#heartbeatComment,
      historyEnabled: this.#historyEnabled,
      maxBufferedEvents: this.#maxBufferedEvents,
      slowClientStrategy: this.#slowClientStrategy,
      maxConnections: this.#maxConnections,
      maxTopicsPerConnection: this.#maxTopicsPerConnection,
      maxEventBytes: this.#maxEventBytes,
      hasBus: this.#bus !== undefined,
      nodeId: this.#nodeId,
    };
  }

  /** Snapshot of current server state. */
  getStats(): SSEServerStats {
    let historySize = 0;
    if (this.#historyEnabled) {
      if (isLegacyHistoryStore(this.#history)) {
        historySize = this.#history.size;
      } else {
        const size = (this.#history as { size?: unknown }).size;
        historySize = typeof size === "number" ? size : 0;
      }
    }
    return {
      connections: this.#connections.size,
      topics: this.#topics.size,
      historySize,
      closed: this.#closed,
    };
  }

  // --------------------------------------------------------------- heartbeat

  #startHeartbeat(): void {
    this.#heartbeatTimer = setInterval(() => {
      if (this.#closed) return;
      const frame = formatHeartbeat(this.#heartbeatComment);
      for (const connection of [...this.#connections.values()]) {
        try {
          connection.writeFrame(frame);
        } catch (error) {
          this.emit("error", asError(error), connection);
          connection.close();
        }
      }
    }, this.#heartbeatInterval);
    // Don't hold the process open for heartbeats alone.
    if (typeof this.#heartbeatTimer.unref === "function") {
      this.#heartbeatTimer.unref();
    }
  }

  #stopHeartbeat(): void {
    if (this.#heartbeatTimer !== undefined) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = undefined;
    }
  }

  // -------------------------------------------------------- graceful shutdown

  /**
   * Gracefully shut down: stops accepting new connections, stops the
   * heartbeat timer, closes all active connections, clears topic
   * subscriptions, releases the bus, and resolves only after cleanup is
   * complete. Safe to call repeatedly.
   *
   * Local (synchronous) history is cleared; async/shared stores
   * (Redis, …) are intentionally left intact so other nodes keep their
   * history. Awaiting {@link flushHistory} before `close()` guarantees
   * all enqueued writes have settled.
   */
  close(): Promise<void> {
    if (this.#closePromise !== undefined) return this.#closePromise;
    this.#closed = true;
    this.#stopHeartbeat();
    const connections = [...this.#connections.values()];
    for (const connection of connections) {
      try {
        connection.close();
      } catch (error) {
        this.emit("error", asError(error), connection);
      }
    }
    this.#connections.clear();
    this.#topics.clear();
    if (this.#asyncStore === undefined && isLegacyHistoryStore(this.#history)) {
      this.#history.clear();
    }
    const bus = this.#bus;
    const unsubscribe = this.#unsubscribeBus;
    this.#closePromise = (async (): Promise<void> => {
      await this.#persistQueue;
      if (bus !== undefined) {
        try {
          unsubscribe?.();
        } catch (error) {
          this.emit("busError", asError(error));
        }
        try {
          await bus.close();
        } catch (error) {
          this.emit("busError", asError(error));
        }
      }
    })();
    return this.#closePromise;
  }

  // ------------------------------------------------------------------ events

  /** Typed `on` overloads for server lifecycle events. */
  on(event: "connection", listener: ConnectionListener): this;
  on(event: "disconnect", listener: DisconnectListener): this;
  on(event: "error", listener: ErrorListener): this;
  on(event: "storageError", listener: StorageErrorListener): this;
  on(event: "busError", listener: BusErrorListener): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  /** Typed `once` overloads for server lifecycle events. */
  once(event: "connection", listener: ConnectionListener): this;
  once(event: "disconnect", listener: DisconnectListener): this;
  once(event: "error", listener: ErrorListener): this;
  once(event: "storageError", listener: StorageErrorListener): this;
  once(event: "busError", listener: BusErrorListener): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  /** Typed `off` overloads for server lifecycle events. */
  off(event: "connection", listener: ConnectionListener): this;
  off(event: "disconnect", listener: DisconnectListener): this;
  off(event: "error", listener: ErrorListener): this;
  off(event: "storageError", listener: StorageErrorListener): this;
  off(event: "busError", listener: BusErrorListener): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  off(event: string, listener: (...args: any[]) => void): this {
    return super.off(event, listener);
  }

  /**
   * Convenience alias for `on("connection", listener)` — receives the
   * new connection plus a context containing `lastEventId`.
   */
  onConnection(listener: ConnectionListener): this {
    return this.on("connection", listener);
  }

  /** Convenience alias for `on("disconnect", listener)`. */
  onDisconnect(listener: DisconnectListener): this {
    return this.on("disconnect", listener);
  }

  /** Convenience alias for `on("error", listener)`. */
  onError(listener: ErrorListener): this {
    return this.on("error", listener);
  }

  /**
   * Convenience alias for `on("storageError", listener)` — fires when
   * an async history-store write or replay fails. Live delivery is
   * never affected.
   */
  onStorageError(listener: StorageErrorListener): this {
    return this.on("storageError", listener);
  }

  /** Convenience alias for `on("busError", listener)`. */
  onBusError(listener: BusErrorListener): this {
    return this.on("busError", listener);
  }
}

function validatePositiveInt(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new SSEError(`\`${name}\` must be a positive integer.`);
  }
}

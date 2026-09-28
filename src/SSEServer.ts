/**
 * The main entry point of `sse-kit`: creates, tracks, and cleans up SSE
 * connections, and routes events to individual clients, all clients, or
 * topic subscribers.
 */

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
import { TopicManager } from "./TopicManager.js";
import type {
  ConnectOptions,
  ConnectionListener,
  DisconnectListener,
  ErrorListener,
  SSEConnectionContext,
  SSEEvent,
  SSEHistoryStore,
  SSESerializer,
  SSEServerOptions,
  SSEServerStats,
  SlowClientStrategy,
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
};

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
 * import { SSEServer } from "sse-kit";
 *
 * const sse = new SSEServer({ heartbeatInterval: 30_000 });
 *
 * app.get("/events", (req, res) => {
 *   sse.connect(req, res);
 * });
 *
 * sse.broadcast({ event: "ping", data: "hello" });
 * ```
 */
export class SSEServer extends EventEmitter {
  readonly #connections = new Map<string, SSEConnection>();
  readonly #topics = new TopicManager();
  readonly #history: SSEHistoryStore;
  readonly #historyEnabled: boolean;

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
  #heartbeatTimer: NodeJS.Timeout | undefined;
  #closed = false;
  #closePromise: Promise<void> | undefined;

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

  // -------------------------------------------------------------- lifecycle

  /**
   * Accept an SSE connection. Sets the required SSE headers, registers
   * disconnect detection, replays missed events when history is enabled
   * and the client sent `Last-Event-ID`, and emits `"connection"`.
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

    // Replay missed events (history entries already carry assigned IDs).
    if (lastEventId !== undefined && this.#historyEnabled) {
      for (const entry of this.#history.getAfter(lastEventId)) {
        if (connection.closed) break;
        connection.writeFrame(entry.frame);
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
   * `maxEventBytes` limit. Returns the ID used plus the wire frame.
   */
  #prepare<T>(
    event: SSEEvent<T>,
    forHistory: boolean,
  ): { id: string | undefined; frame: string } {
    const id = this.#assignId(event, forHistory);
    const frame =
      id === undefined || event.id !== undefined
        ? formatSSEFrame(event, { serialize: this.#serialize })
        : formatSSEFrame({ ...event, id }, { serialize: this.#serialize });
    if (Buffer.byteLength(frame, "utf8") > this.#maxEventBytes) {
      throw new SSEError(
        `SSE frame exceeds the limit of ${this.#maxEventBytes} bytes.`,
      );
    }
    return { id, frame };
  }

  #deliver(connection: SSEConnection, frame: string): boolean {
    try {
      return connection.writeFrame(frame);
    } catch (error) {
      this.emit(
        "error",
        error instanceof Error ? error : new SSEError(String(error)),
        connection,
      );
      connection.close();
      return false;
    }
  }

  /**
   * Broadcast an event to **all** connected clients.
   * @returns Number of clients the event was accepted by.
   */
  broadcast<T>(event: SSEEvent<T>): number {
    if (this.#closed) return 0;
    const { id, frame } = this.#prepare(event, true);
    if (this.#historyEnabled && id !== undefined) {
      this.#history.add({ id, frame });
    }
    let count = 0;
    for (const connection of [...this.#connections.values()]) {
      if (this.#deliver(connection, frame)) count += 1;
    }
    return count;
  }

  /** Alias for {@link broadcast}. */
  send<T>(event: SSEEvent<T>): number {
    return this.broadcast(event);
  }

  /**
   * Send an event to a single connection.
   * @returns `true` if the connection exists and accepted the event.
   *
   * Note: direct messages are intentionally **not** stored in history
   * (history is a global log — storing private messages there would
   * replay them to whoever reconnects next). Documented in the README.
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
   * in the (global, positional — not per-topic) history like broadcasts.
   */
  to(topic: Topic): TopicBroadcaster {
    return {
      broadcast: <T>(event: SSEEvent<T>): number => {
        if (this.#closed) return 0;
        const { id, frame } = this.#prepare(event, true);
        if (this.#historyEnabled && id !== undefined) {
          this.#history.add({ id, frame });
        }
        let count = 0;
        for (const connectionId of this.#topics.getSubscribers(topic)) {
          const connection = this.#connections.get(connectionId);
          if (connection !== undefined && this.#deliver(connection, frame)) {
            count += 1;
          }
        }
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

  /** Snapshot of current server state. */
  getStats(): SSEServerStats {
    return {
      connections: this.#connections.size,
      topics: this.#topics.size,
      historySize: this.#historyEnabled ? this.#history.size : 0,
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
          this.emit(
            "error",
            error instanceof Error ? error : new SSEError(String(error)),
            connection,
          );
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
   * subscriptions and history, and releases resources. Resolves only
   * after cleanup is complete. Safe to call repeatedly.
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
        this.emit(
          "error",
          error instanceof Error ? error : new SSEError(String(error)),
          connection,
        );
      }
    }
    this.#connections.clear();
    this.#topics.clear();
    this.#history.clear();
    this.#closePromise = Promise.resolve();
    return this.#closePromise;
  }

  // ------------------------------------------------------------------ events

  /** Typed `on` overloads for server lifecycle events. */
  on(event: "connection", listener: ConnectionListener): this;
  on(event: "disconnect", listener: DisconnectListener): this;
  on(event: "error", listener: ErrorListener): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  /** Typed `once` overloads for server lifecycle events. */
  once(event: "connection", listener: ConnectionListener): this;
  once(event: "disconnect", listener: DisconnectListener): this;
  once(event: "error", listener: ErrorListener): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  /** Typed `off` overloads for server lifecycle events. */
  off(event: "connection", listener: ConnectionListener): this;
  off(event: "disconnect", listener: DisconnectListener): this;
  off(event: "error", listener: ErrorListener): this;
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
}

function validatePositiveInt(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new SSEError(`\`${name}\` must be a positive integer.`);
  }
}

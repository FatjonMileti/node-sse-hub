/**
 * `node-sse-hub` — a lightweight, framework-friendly Server-Sent Events (SSE)
 * library for Node.js.
 *
 * @example
 * ```ts
 * import { SSEServer } from "node-sse-hub";
 *
 * const sse = new SSEServer({ heartbeatInterval: 30_000 });
 *
 * // Express / Fastify / native http: pass the raw req & res through.
 * sse.connect(req, res);
 *
 * sse.broadcast({ event: "ping", data: { ok: true } });
 * ```
 */

export { SSEServer } from "./SSEServer.js";
export { SSEConnection } from "./SSEConnection.js";
export {
  formatSSEFrame,
  formatDataLines,
  formatHeartbeat,
  parseLastEventId,
  InMemoryHistoryStore,
  DEFAULT_SERIALIZE,
  HEARTBEAT_COMMENT,
} from "./SSEEvent.js";
export { MemoryEventStore, isLegacyHistoryStore } from "./store.js";
export type {
  StoredSSEEvent,
  SSEEventStore,
  SSEEventBus,
  SSEBusEnvelope,
  GetAfterOptions,
  MemoryEventStoreOptions,
} from "./store.js";
export { TopicManager, MAX_TOPIC_LENGTH } from "./TopicManager.js";
export {
  SSEError,
  ConnectionNotFoundError,
  SSEClosedError,
  TopicNotFoundError,
} from "./errors.js";
export type {
  Topic,
  SSEEvent,
  SSESerializer,
  SlowClientStrategy,
  SSEHistoryEntry,
  SSEHistoryStore,
  HistoryOptions,
  HeartbeatOptions,
  SSEConnectionContext,
  ConnectOptions,
  SSEServerOptions,
  SSEServerResolvedOptions,
  BroadcastOptions,
  ConnectionListener,
  DisconnectListener,
  ErrorListener,
  StorageErrorListener,
  BusErrorListener,
  TopicBroadcaster,
  SSEServerStats,
} from "./types.js";
export type { SSEConnectionOptions } from "./SSEConnection.js";
export type { FormatOptions } from "./SSEEvent.js";

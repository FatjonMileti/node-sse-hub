/**
 * SSE frame formatting and parsing helpers.
 *
 * Follows the HTML Standard's
 * [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
 * event-stream format:
 *
 * ```text
 * id: <id>
 * event: <name>
 * data: <line 1>
 * data: <line 2>
 * retry: <milliseconds>
 *
 * ```
 */

import { SSEError } from "./errors.js";
import type { SSEEvent, SSEHistoryEntry, SSESerializer } from "./types.js";

export const DEFAULT_SERIALIZE: SSESerializer = (data: unknown) =>
  JSON.stringify(data);

/** Default heartbeat comment text. */
export const HEARTBEAT_COMMENT = "heartbeat";

/** Format a heartbeat/keep-alive SSE comment frame. */
export function formatHeartbeat(comment: string = HEARTBEAT_COMMENT): string {
  const sanitized = comment.replace(/[\r\n]/g, " ").trim() || HEARTBEAT_COMMENT;
  return `: ${sanitized}\n\n`;
}

function assertNoNewlines(value: string, field: "id" | "event"): void {
  if (value.includes("\n") || value.includes("\r")) {
    throw new SSEError(
      `Invalid SSE ${field} ${JSON.stringify(value)}: must not contain CR or LF characters.`,
    );
  }
}

/**
 * Serialize an event payload to its multi-line `data:` representation.
 * Strings are sent as-is; any other value goes through `serialize`.
 * `undefined` produces no `data:` lines (valid for `retry:`-only frames).
 */
export function formatDataLines(
  data: unknown,
  serialize: SSESerializer = DEFAULT_SERIALIZE,
): string {
  if (data === undefined) return "";
  const text = typeof data === "string" ? data : serialize(data);
  if (typeof text !== "string") {
    throw new SSEError("Custom SSE serializer must return a string.");
  }
  return text
    .split(/\r\n|\r|\n/)
    .map((line) => `data: ${line}\n`)
    .join("");
}

/** Options for {@link formatSSEFrame}. */
export interface FormatOptions {
  serialize?: SSESerializer;
}

/**
 * Format an {@link SSEEvent} into a wire-ready SSE frame (ends with a
 * blank line). Throws {@link SSEError} on malformed fields.
 */
export function formatSSEFrame<T>(
  event: SSEEvent<T>,
  options: FormatOptions = {},
): string {
  // Validate at runtime (typed as unknown first so the checks are
  // meaningful for plain-JavaScript callers too).
  const candidate: unknown = event;
  if (
    candidate === null ||
    typeof candidate !== "object" ||
    Array.isArray(candidate)
  ) {
    throw new SSEError("SSE event must be an object with a `data` property.");
  }
  if (!("data" in candidate)) {
    throw new SSEError("SSE event must have a `data` property.");
  }
  const evt = candidate as SSEEvent<T>;

  const serialize = options.serialize ?? DEFAULT_SERIALIZE;
  let frame = "";

  if (evt.id !== undefined) {
    if (typeof evt.id !== "string" || evt.id.length === 0) {
      throw new SSEError("SSE event `id` must be a non-empty string.");
    }
    assertNoNewlines(evt.id, "id");
    frame += `id: ${evt.id}\n`;
  }

  if (evt.event !== undefined) {
    if (typeof evt.event !== "string" || evt.event.length === 0) {
      throw new SSEError("SSE event `event` must be a non-empty string.");
    }
    assertNoNewlines(evt.event, "event");
    frame += `event: ${evt.event}\n`;
  }

  if (evt.retry !== undefined) {
    if (!Number.isInteger(evt.retry) || evt.retry < 0) {
      throw new SSEError("SSE event `retry` must be a non-negative integer.");
    }
    frame += `retry: ${evt.retry}\n`;
  }

  frame += formatDataLines(evt.data, serialize);
  frame += "\n";
  return frame;
}

/**
 * Parse the `Last-Event-ID` header value from a headers object.
 * Returns `undefined` when absent or empty.
 */
export function parseLastEventId(
  headers: Record<string, unknown> | undefined,
): string | undefined {
  if (headers === undefined) return undefined;
  const raw = headers["last-event-id"];
  if (typeof raw !== "string") return undefined;
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Simple in-memory history store: keeps the last `maxEvents` entries and
 * replays entries stored after a given ID (exclusive, exact string match).
 *
 * Limitations (documented):
 * - IDs are matched by exact string equality; there is no numeric range
 *   logic, so IDs need not be numeric or ordered — but replay only works
 *   when the client's `Last-Event-ID` is still present in the buffer.
 * - If the ID is unknown (never existed or already evicted), replay
 *   returns an empty list rather than the whole buffer, to avoid
 *   duplicate storms after long disconnections.
 * - The buffer is global to the server instance: replay is positional,
 *   not filtered by topic or original audience. Applications needing
 *   per-topic replay should use separate server instances or filter
 *   client-side.
 */
export class InMemoryHistoryStore {
  readonly #entries: SSEHistoryEntry[] = [];
  readonly #maxEvents: number;

  constructor(maxEvents = 100) {
    if (!Number.isInteger(maxEvents) || maxEvents <= 0) {
      throw new SSEError("History `maxEvents` must be a positive integer.");
    }
    this.#maxEvents = maxEvents;
  }

  get size(): number {
    return this.#entries.length;
  }

  get maxEvents(): number {
    return this.#maxEvents;
  }

  add(entry: SSEHistoryEntry): void {
    this.#entries.push(entry);
    while (this.#entries.length > this.#maxEvents) {
      this.#entries.shift();
    }
  }

  getAfter(lastEventId: string): SSEHistoryEntry[] {
    const index = this.#entries.findIndex((e) => e.id === lastEventId);
    if (index === -1) return [];
    return this.#entries.slice(index + 1);
  }

  clear(): void {
    this.#entries.length = 0;
  }
}

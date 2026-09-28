/**
 * Minimal topic subscription registry: maps topic names to sets of
 * connection IDs. The {@link SSEServer} owns an instance; connections
 * mutate it via `subscribe` / `unsubscribe`.
 */

import { SSEError, TopicNotFoundError } from "./errors.js";
import type { Topic } from "./types.js";

export const MAX_TOPIC_LENGTH = 256;

export function assertValidTopic(topic: string): void {
  if (typeof topic !== "string" || topic.trim().length === 0) {
    throw new SSEError("SSE topic must be a non-empty string.");
  }
  if (topic.length > MAX_TOPIC_LENGTH) {
    throw new SSEError(
      `SSE topic must be at most ${MAX_TOPIC_LENGTH} characters.`,
    );
  }
  if (/[\r\n]/.test(topic)) {
    throw new SSEError("SSE topic must not contain CR or LF characters.");
  }
}

/** Tracks which connection IDs are subscribed to which topics. */
export class TopicManager {
  readonly #topics = new Map<Topic, Set<string>>();

  /** Subscribe a connection ID to a topic. */
  add(topic: Topic, connectionId: string): void {
    assertValidTopic(topic);
    let members = this.#topics.get(topic);
    if (members === undefined) {
      members = new Set<string>();
      this.#topics.set(topic, members);
    }
    members.add(connectionId);
  }

  /**
   * Unsubscribe a connection ID from a topic.
   * @returns `true` if a subscription was removed.
   */
  remove(topic: Topic, connectionId: string): boolean {
    const members = this.#topics.get(topic);
    if (members === undefined) return false;
    const removed = members.delete(connectionId);
    if (members.size === 0) this.#topics.delete(topic);
    return removed;
  }

  /** Remove a connection ID from every topic it belongs to. */
  removeConnection(connectionId: string): void {
    for (const [topic, members] of this.#topics) {
      members.delete(connectionId);
      if (members.size === 0) this.#topics.delete(topic);
    }
  }

  /** Connection IDs subscribed to `topic` (empty array when none). */
  getSubscribers(topic: Topic): string[] {
    const members = this.#topics.get(topic);
    return members === undefined ? [] : [...members];
  }

  /**
   * Connection IDs subscribed to `topic`.
   * @throws {@link TopicNotFoundError} when nobody is subscribed.
   */
  requireSubscribers(topic: Topic): string[] {
    const subscribers = this.getSubscribers(topic);
    if (subscribers.length === 0) throw new TopicNotFoundError(topic);
    return subscribers;
  }

  /** Topics a connection ID is currently subscribed to. */
  getTopicsFor(connectionId: string): Topic[] {
    const topics: Topic[] = [];
    for (const [topic, members] of this.#topics) {
      if (members.has(connectionId)) topics.push(topic);
    }
    return topics;
  }

  /** Whether the topic currently has at least one subscriber. */
  has(topic: Topic): boolean {
    return this.#topics.has(topic);
  }

  /** Number of topics with at least one subscriber. */
  get size(): number {
    return this.#topics.size;
  }

  /** All topics with at least one subscriber. */
  topics(): Topic[] {
    return [...this.#topics.keys()];
  }

  /** Drop every subscription. */
  clear(): void {
    this.#topics.clear();
  }
}

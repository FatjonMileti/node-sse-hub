/**
 * Error classes for `node-sse-hub`. Only a small set is provided — normal
 * lifecycle operations (e.g. disconnecting an unknown ID) return `false`
 * instead of throwing.
 */

/** Base class for all `node-sse-hub` errors. */
export class SSEError extends Error {
  override name = "SSEError";

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    // Maintain a proper prototype chain when targeting ES2022+ with
    // older transpile setups.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when an operation targets a connection ID that does not exist. */
export class ConnectionNotFoundError extends SSEError {
  override name = "ConnectionNotFoundError";

  constructor(connectionId: string) {
    super(`SSE connection not found: "${connectionId}"`);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when connecting or sending after the server has been closed. */
export class SSEClosedError extends SSEError {
  override name = "SSEClosedError";

  constructor(operation = "operation") {
    super(`SSE server is closed; cannot perform ${operation}.`);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when an operation targets a topic that has no subscribers. */
export class TopicNotFoundError extends SSEError {
  override name = "TopicNotFoundError";

  constructor(topic: string) {
    super(`SSE topic has no subscribers: "${topic}"`);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

import { describe, expect, it } from "vitest";
import {
  DEFAULT_SERIALIZE,
  formatDataLines,
  formatHeartbeat,
  formatSSEFrame,
  InMemoryHistoryStore,
  parseLastEventId,
} from "../src/SSEEvent.js";
import { SSEError } from "../src/errors.js";

describe("formatSSEFrame", () => {
  it("formats a named event with a JSON object payload", () => {
    const frame = formatSSEFrame({
      event: "message",
      data: { message: "Hello" },
    });
    expect(frame).toBe('event: message\ndata: {"message":"Hello"}\n\n');
  });

  it("formats string data as-is", () => {
    expect(formatSSEFrame({ event: "message", data: "Hello" })).toBe(
      "event: message\ndata: Hello\n\n",
    );
  });

  it("formats an event without a name (data-only frame)", () => {
    expect(formatSSEFrame({ data: "hi" })).toBe("data: hi\n\n");
  });

  it("includes explicit IDs", () => {
    const frame = formatSSEFrame({
      id: "event-123",
      event: "invoice-created",
      data: { invoiceId: "123" },
    });
    expect(frame).toBe(
      'id: event-123\nevent: invoice-created\ndata: {"invoiceId":"123"}\n\n',
    );
  });

  it("includes the retry field", () => {
    expect(formatSSEFrame({ data: "x", retry: 3000 })).toBe(
      "retry: 3000\ndata: x\n\n",
    );
  });

  it("orders fields as id, event, retry, data", () => {
    const frame = formatSSEFrame({
      id: "1",
      event: "e",
      retry: 100,
      data: "d",
    });
    expect(frame).toBe("id: 1\nevent: e\nretry: 100\ndata: d\n\n");
  });

  it("splits multiline string data into multiple data lines", () => {
    expect(formatSSEFrame({ data: "first line\nsecond line" })).toBe(
      "data: first line\ndata: second line\n\n",
    );
  });

  it("handles CRLF and CR line endings", () => {
    expect(formatSSEFrame({ data: "a\r\nb\rc" })).toBe(
      "data: a\ndata: b\ndata: c\n\n",
    );
  });

  it("splits multiline serialized JSON across data lines", () => {
    const frame = formatSSEFrame(
      { data: { a: 1 } },
      { serialize: () => "line1\nline2" },
    );
    expect(frame).toBe("data: line1\ndata: line2\n\n");
  });

  it("supports a custom serializer", () => {
    const frame = formatSSEFrame(
      { event: "e", data: { a: 1 } },
      { serialize: (d) => `custom:${JSON.stringify(d)}` },
    );
    expect(frame).toContain("data: custom:");
  });

  it("throws for non-object events", () => {
    expect(() => formatSSEFrame("nope" as unknown as { data: string })).toThrow(
      SSEError,
    );
    expect(() => formatSSEFrame(null as never)).toThrow(SSEError);
  });

  it("throws when data is missing", () => {
    expect(() => formatSSEFrame({} as { data: string })).toThrow(SSEError);
  });

  it("throws for IDs containing newlines", () => {
    expect(() => formatSSEFrame({ id: "a\nb", data: "x" })).toThrow(SSEError);
    expect(() => formatSSEFrame({ event: "a\rb", data: "x" })).toThrow(
      SSEError,
    );
  });

  it("throws for invalid retry values", () => {
    expect(() => formatSSEFrame({ data: "x", retry: -1 })).toThrow(SSEError);
    expect(() => formatSSEFrame({ data: "x", retry: 1.5 })).toThrow(SSEError);
  });

  it("throws when a custom serializer does not return a string", () => {
    expect(() =>
      formatSSEFrame({ data: { a: 1 } }, { serialize: () => 42 as never }),
    ).toThrow(SSEError);
  });

  it("allows undefined data (retry-only frames)", () => {
    expect(formatSSEFrame({ retry: 100, data: undefined })).toBe(
      "retry: 100\n\n",
    );
  });
});

describe("formatDataLines", () => {
  it("returns empty string for undefined data", () => {
    expect(formatDataLines(undefined)).toBe("");
  });

  it("uses the default JSON serializer for objects", () => {
    expect(formatDataLines({ a: 1 }, DEFAULT_SERIALIZE)).toBe(
      'data: {"a":1}\n',
    );
  });
});

describe("formatHeartbeat", () => {
  it("formats the default heartbeat comment", () => {
    expect(formatHeartbeat()).toBe(": heartbeat\n\n");
  });

  it("formats a custom comment and strips newlines", () => {
    expect(formatHeartbeat("ping")).toBe(": ping\n\n");
    expect(formatHeartbeat("a\nb")).toBe(": a b\n\n");
  });
});

describe("parseLastEventId", () => {
  it("extracts the header value", () => {
    expect(parseLastEventId({ "last-event-id": "42" })).toBe("42");
  });

  it("returns undefined when absent, empty, or non-string", () => {
    expect(parseLastEventId(undefined)).toBeUndefined();
    expect(parseLastEventId({})).toBeUndefined();
    expect(parseLastEventId({ "last-event-id": "   " })).toBeUndefined();
    expect(parseLastEventId({ "last-event-id": 42 })).toBeUndefined();
  });
});

describe("InMemoryHistoryStore", () => {
  it("stores entries and replays everything after an ID", () => {
    const store = new InMemoryHistoryStore(10);
    store.add({ id: "1", frame: "a" });
    store.add({ id: "2", frame: "b" });
    store.add({ id: "3", frame: "c" });
    expect(store.size).toBe(3);
    expect(store.getAfter("1").map((e) => e.id)).toEqual(["2", "3"]);
    expect(store.getAfter("3")).toEqual([]);
  });

  it("returns nothing for unknown IDs", () => {
    const store = new InMemoryHistoryStore(10);
    store.add({ id: "1", frame: "a" });
    expect(store.getAfter("missing")).toEqual([]);
  });

  it("evicts the oldest entries beyond maxEvents", () => {
    const store = new InMemoryHistoryStore(2);
    store.add({ id: "1", frame: "a" });
    store.add({ id: "2", frame: "b" });
    store.add({ id: "3", frame: "c" });
    expect(store.size).toBe(2);
    // "1" was evicted, so replay from it yields nothing.
    expect(store.getAfter("1")).toEqual([]);
    expect(store.getAfter("2").map((e) => e.id)).toEqual(["3"]);
  });

  it("works with non-numeric IDs via exact match", () => {
    const store = new InMemoryHistoryStore(10);
    store.add({ id: "evt-a", frame: "a" });
    store.add({ id: "evt-b", frame: "b" });
    expect(store.getAfter("evt-a").map((e) => e.id)).toEqual(["evt-b"]);
  });

  it("validates maxEvents and supports clear()", () => {
    expect(() => new InMemoryHistoryStore(0)).toThrow(SSEError);
    const store = new InMemoryHistoryStore(5);
    store.add({ id: "1", frame: "a" });
    store.clear();
    expect(store.size).toBe(0);
  });
});

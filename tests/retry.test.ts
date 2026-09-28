/**
 * Retry tests against the REAL stack (`node-retry-kit` + the
 * `redis-orm-lite` retry machinery) with injected flaky operations —
 * no Redis server needed because `executeRedisCommand` takes the
 * operation as a function.
 *
 * The `RedisEventStore` forwards its `retry` option into exactly this
 * machinery (verified in `tests/redis-store.test.ts`), so these tests
 * prove the behavior users get: transient failures retry, permanent
 * ones don't, and exhaustion surfaces the original error.
 */
import { describe, expect, it } from "vitest";
import { executeRedisCommand, isTransientRedisError } from "redis-orm-lite";
import { retry, RetryError } from "node-retry-kit";

function transientError(message = "Connection reset by peer"): Error {
  const error = new Error(message) as Error & { code: string };
  error.code = "ECONNRESET";
  return error;
}

function permanentError(): Error {
  const error = new Error(
    "WRONGPASS invalid username-password pair",
  ) as Error & {
    code: string;
  };
  error.code = "WRONGPASS";
  return error;
}

describe("transient-error classification", () => {
  it("retries network/timeout failures, not auth/data errors", () => {
    expect(isTransientRedisError(transientError())).toBe(true);
    expect(isTransientRedisError(new Error("Operation timed out"))).toBe(true);
    expect(isTransientRedisError(permanentError())).toBe(false);
    expect(
      isTransientRedisError(new Error("WRONGTYPE wrong kind of value")),
    ).toBe(false);
    expect(
      isTransientRedisError(new DOMException("aborted", "AbortError")),
    ).toBe(false);
    expect(isTransientRedisError(null)).toBe(false);
  });
});

describe("executeRedisCommand with a flaky operation", () => {
  it("retries transient failures and eventually succeeds", async () => {
    let attempts = 0;
    const result = await executeRedisCommand(
      "set",
      async () => {
        attempts += 1;
        if (attempts < 3) throw transientError();
        return "OK";
      },
      { retries: 3, delay: 1 },
    );
    expect(result).toBe("OK");
    expect(attempts).toBe(3);
  });

  it("does not retry permanent failures", async () => {
    let attempts = 0;
    await expect(
      executeRedisCommand(
        "set",
        async () => {
          attempts += 1;
          throw permanentError();
        },
        { retries: 3, delay: 1 },
      ),
    ).rejects.toThrow("WRONGPASS");
    expect(attempts).toBe(1);
  });

  it("gives up after retries are exhausted, rethrowing the last error", async () => {
    let attempts = 0;
    const failure = transientError("ECONNREFUSED");
    await expect(
      executeRedisCommand(
        "get",
        async () => {
          attempts += 1;
          throw failure;
        },
        { retries: 2, delay: 1 },
      ),
    ).rejects.toBe(failure);
    expect(attempts).toBe(3); // 1 initial + 2 retries
  });

  it("honors the reads:false / writes:false gates", async () => {
    let reads = 0;
    await expect(
      executeRedisCommand(
        "get",
        async () => {
          reads += 1;
          throw transientError();
        },
        { retries: 3, delay: 1, reads: false },
      ),
    ).rejects.toThrow();
    expect(reads).toBe(1);

    let writes = 0;
    await expect(
      executeRedisCommand(
        "set",
        async () => {
          writes += 1;
          throw transientError();
        },
        { retries: 3, delay: 1, writes: false },
      ),
    ).rejects.toThrow();
    expect(writes).toBe(1);
  });

  it("runs exactly once when retries are not configured", async () => {
    let attempts = 0;
    await expect(
      executeRedisCommand("set", async () => {
        attempts += 1;
        throw transientError();
      }),
    ).rejects.toThrow();
    expect(attempts).toBe(1);
  });
});

describe("node-retry-kit primitives (used by the stack above)", () => {
  it("backs off between attempts and counts them", async () => {
    const delays: number[] = [];
    let attempts = 0;
    await retry(
      async () => {
        attempts += 1;
        if (attempts < 3) throw transientError();
        return "done";
      },
      {
        retries: 5,
        backoff: "exponential",
        delay: 1,
        maxDelay: 5,
        jitter: false,
        onRetry: (_error, context) => {
          delays.push(context.delay);
        },
      },
    );
    expect(attempts).toBe(3);
    expect(delays.length).toBe(2);
  });

  it("supports wrapErrors for programmatic exhaustion handling", async () => {
    const error = await retry(
      async () => {
        throw transientError();
      },
      { retries: 1, delay: 1, wrapErrors: true },
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RetryError);
    expect((error as RetryError).attempts).toBe(2);
    expect((error as RetryError).cause).toBeInstanceOf(Error);
  });
});

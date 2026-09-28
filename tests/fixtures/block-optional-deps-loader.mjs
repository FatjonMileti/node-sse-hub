/**
 * ESM loader fixture for isolation tests: makes `redis-orm-lite` and
 * `node-retry-kit` unresolvable so tests can prove the `sse-kit` core
 * never loads them.
 */
export async function resolve(specifier, context, next) {
  if (specifier === "redis-orm-lite" || specifier === "node-retry-kit") {
    throw new Error(
      `[isolation-fixture] blocked optional dependency: ${specifier}`,
    );
  }
  return next(specifier, context);
}

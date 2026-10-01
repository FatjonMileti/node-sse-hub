const hub = require("./lib/index.js");

const resolved = hub.create();
if (resolved.heartbeatInterval !== 15_000) {
  console.error(
    `CJS fixture FAILED: expected heartbeatInterval 15000, got ${String(
      resolved.heartbeatInterval,
    )}`,
  );
  process.exitCode = 1;
} else {
  console.log(
    `CJS fixture runtime OK (heartbeatInterval ${String(resolved.heartbeatInterval)})`,
  );
}

#!/usr/bin/env bash
# Regression check: a CommonJS consumer (module: node16, no "type": "module")
# must typecheck against the PACKED tarball with zero suppressions, and the
# emitted CJS must run against the .cjs build.
#
# Usage: npm run verify:cjs   (packs first, then runs this script)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARBALL="$(ls -t "$ROOT"/node-sse-hub-*.tgz | head -n 1)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cp -r "$ROOT/fixtures/cjs-consumer/." "$TMP/"
mkdir -p "$TMP/node_modules/node-sse-hub"
tar -xzf "$TARBALL" --strip-components=1 -C "$TMP/node_modules/node-sse-hub"
# Node built-in types for the fixture (the packed tarball intentionally
# contains no @types; real consumers bring their own @types/node).
mkdir -p "$TMP/node_modules/@types"
ln -s "$ROOT/node_modules/@types/node" "$TMP/node_modules/@types/node"

echo "--- tsc (typecheck, no emit) ---"
"$ROOT/node_modules/.bin/tsc" -p "$TMP/tsconfig.json" --noEmit
echo "typecheck OK"

echo "--- tsc (emit) + node (require .cjs build) ---"
"$ROOT/node_modules/.bin/tsc" -p "$TMP/tsconfig.json"
node "$TMP/run.js"

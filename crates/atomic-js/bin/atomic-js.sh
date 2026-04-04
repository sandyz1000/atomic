#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="$SCRIPT_DIR/../../../target/release/atomic-js"
if [ ! -f "$BINARY" ]; then
  echo "atomic-js binary not found. Run: npm run build" >&2
  exit 1
fi
exec "$BINARY" "$@"

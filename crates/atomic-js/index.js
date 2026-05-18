// Load the native Node.js module built by napi-rs.
// The .node binary is produced by one of:
//   npm run build                             → atomic_js.node (next to this file)
//   cargo build --release -p atomic-js       → ../../target/release/atomic_js.node
//   cargo build -p atomic-js                 → ../../target/debug/atomic_js.node

"use strict";

const { join } = require("path");

let nativeBinding;

function tryLoad(p) {
  try {
    nativeBinding = require(p);
    return true;
  } catch (_) {
    return false;
  }
}

// Priority: napi CLI co-located build → release cargo build → debug cargo build
const localPath = join(__dirname, "atomic_js.node");
const releasePath = join(
  __dirname,
  "..",
  "..",
  "target",
  "release",
  "atomic_js.node",
);
const debugPath = join(
  __dirname,
  "..",
  "..",
  "target",
  "debug",
  "atomic_js.node",
);

if (!tryLoad(localPath) && !tryLoad(releasePath) && !tryLoad(debugPath)) {
  throw new Error(
    "Failed to load @atomic-compute/js native module (atomic_js.node).\n\n" +
      "Build it first with one of:\n" +
      "  cd crates/atomic-js && npm run build     # recommended (napi CLI)\n" +
      "  cargo build --release -p atomic-js       # release build via cargo\n" +
      "  cargo build -p atomic-js                 # debug build via cargo\n\n" +
      "Tried:\n" +
      "  " +
      localPath +
      "\n" +
      "  " +
      releasePath +
      "\n" +
      "  " +
      debugPath,
  );
}

// Re-export with user-facing names matching the TypeScript declarations.
module.exports = {
  Context: nativeBinding.JsContext,
  RDD: nativeBinding.JsRdd,
};

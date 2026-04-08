// Load the native Node.js module built by napi-rs.
// After `npm run build`, the .node file lands next to this file.
// After `cargo build --release -p atomic-js`, it lands in ../../target/release/.

const { join } = require('path');

let nativeBinding;

function tryLoad(p) {
  try {
    nativeBinding = require(p);
    return true;
  } catch (_) {
    return false;
  }
}

// 1. Release build (npm run build / cargo build --release)
const releasePath = join(__dirname, '..', '..', 'target', 'release', 'atomic_js.node');
// 2. Debug build
const debugPath = join(__dirname, '..', '..', 'target', 'debug', 'atomic_js.node');
// 3. napi-rs CLI build (co-located .node)
const localPath = join(__dirname, 'atomic_js.node');

if (!tryLoad(localPath) && !tryLoad(releasePath) && !tryLoad(debugPath)) {
  throw new Error(
    'Failed to load atomic_js native module.\n' +
    'Build it first:\n' +
    '  cargo build --release -p atomic-js\n' +
    'or:\n' +
    '  cd crates/atomic-js && npm run build'
  );
}

module.exports = nativeBinding;

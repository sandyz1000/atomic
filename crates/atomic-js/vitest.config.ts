import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Native napi-rs addons are not thread-safe across worker threads: each
    // Worker has its own napi_env and class constructor registry, so a class
    // registered in one env isn't found in another. Use forked processes
    // (one env per process) so every test file gets a clean, fully-initialized
    // native binding without cross-env constructor lookup failures.
    pool: "forks",
  },
});

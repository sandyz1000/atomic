# Audit: Over-engineering & bloat in the Atomic Rust workspace

Scope: over-engineering / dead code / unused flexibility / speculative features only.
Not a correctness, security, or performance audit. Line numbers are approximate
(marker line `anchor` given where exact starts differ). Verified by direct file reads
across all listed crates.

## Ranked findings (biggest cuts first)

1. **delete:** entire gRPC stack — `tonic`, `prost`, `tonic-build` are dead weight.
   - Wire transport is hand-rolled rkyv frames over raw TCP (`atomic-data/src/distributed/transport.rs`, `wire.rs`; `atomic-compute/src/executor.rs`). No `.proto` files exist anywhere (`proto/` is absent; both read attempts ENOENT). There is no `build.rs`. Nothing emits or consumes protobuf.
   - Worse: `tonic-build` (a build-time codegen tool) is declared as a *runtime* dependency in `atomic-compute/Cargo.toml` `[dependencies]` — it has nothing to generate and shouldn't even compile in this position.
   - Cut: `tonic`, `prost`, `tonic-build` from `crates/atomic-compute/Cargo.toml`, `crates/atomic-data/Cargo.toml`, and `[workspace.dependencies]` of root `Cargo.toml`. Keep `hyper`/`http-body-util`/`http` — those back the shuffle/metrics/register HTTP servers (real).
   - `hyper`/`http` also appear in `atomic-data` deps and root workspace deps; keep where host.

2. **delete:** duplicate `hosts` module compiled twice.
   - `crates/atomic-data/src/hosts.rs` and `crates/atomic-compute/src/hosts.rs` are byte-identical (same `Hosts{master,slaves}`, same `OnceCell` loader reading `~/hosts.conf`). Only the atomic-compute copy is referenced (`crate::hosts::Hosts::get()` in `crates/atomic-compute/src/context/mod.rs`). The atomic-data copy is a shadowed clone.
   - Cut: remove `atomic-data/src/hosts.rs` (and `pub mod hosts` in its lib.rs) and the now-unused `HostError`/`hosts` artifacts. Also this is a legacy Spark homologue — `Config::from_env` already resolves workers itself; see #7.

3. **shrink:** `Config::from_env` + env-var `ConfigError` variants are legacy parallel config with live code reaching for `hosts.conf`.
   - The project's stated direction (lib.rs + context doc comments) is explicit-constructor `Config` (`local`/`distributed_driver`/`worker`). `from_env` re-reads a parallel set of `ATOMIC_*` env vars *and* falls back to the `hosts.conf` file. `ConfigError::MissingSlavePort` / `MissingSlavePort`+`SLAVE_DEPLOYMENT` / `MissingLocalIp` exist only for this path.
   - Kept only for Python/JS bindings — so the env-var loader and the whole `hosts.conf` read should be scoped behind the binding crates, not living in the core. At minimum relocate the hosts-file resolution out of core into `atomic-py`/`atomic-js`; core `Config` shouldn't know about a file format.
   - `crates/atomic-compute/src/env/mod.rs` (lines ~220-330), `context/mod.rs` (lines ~105-125).

4. **stdlib:** `randomize_in_place` reinvents `slice::shuffle()`.
   - `crates/atomic-utils/src/sys_env.rs` lines ~10-19 hand-rolls an in-place shuffle loop. `rand` already ships this exact primitive: `use rand::seq::SliceRandom; slice.shuffle(&mut rng)`. Replace body with `.shuffle(&mut rng)` (note: the hand-rolled version also skips the last element index, the stdlib does it correctly).
   - Bonus: `randomize_in_place` is not even re-exported at the crate root (`lib.rs` only exports `bounded_double`, `bpq`, `random::BernoulliSampler`, `sys_env::{clean_up_work_dir, get_dynamic_port}`) — likely dead as well as redundant.

5. **native:/shrink:** `get_dynamic_port` — hand-rolled ephemeral port range.
   - `crates/atomic-utils/src/sys_env.rs` lines ~22-25 picks a random port in `49152..65535`. Native pattern is binding to port 0 and reading `local_addr`. It's used in tests to acquire a free port; the stdlib/native idiom (`TcpListener::bind("0.0.0.0:0")` → `.local_addr()`) removes the randomness and the whole helper.

6. **delete:/shrink:** dead `cfg_avro!` macro.
   - `crates/atomic-data/src/lib.rs` exports `cfg_avro!`/`cfg_not_avro!`-style macro, but `atomic-data`'s `Cargo.toml` features are `python`, `js`, `scripted`, `kafka` — there is **no `avro` feature** in atomic-data, so the macro can never gate anything at this layer. The avro feature actually lives in `atomic-sql`/`atomic-py`/`atomic-js`. Dead: delete the `cfg_avro!` definition in atomic-data.

7. **yagni:/shrink:** oversized Spark-port sampling module.
   - `crates/atomic-utils/src/random.rs` (~300 lines) is a faithful port of Spark's samplers: `PoissonSampler`, `BernoulliCellSampler`, `GapSamplingReplacement` (`poisson_ge1`, `advance`), `sample_fraction` + `poisson_bounds`/`binomial_bounds` private modules, `get_default_rng` (hardcoded seed `0xcafe_f00d...`). Only `BernoulliSampler` is re-exported at the crate root and consumed externally; the gap-sampling machinery is a micro-optimization from Spark's exceedingly old RNG and the rest (`RandomSampler`, `get_default_rng`, `sample_fraction`, `BernoulliCellSampler`) has no confirmed caller in the workspace.
   - Ask: does any RDD `sample` path actually exercise `sample_fraction`/gap sampling? If not, cut `PoissonSampler` + `GapSamplingReplacement` + `sample_fraction` + bounds modules (~150 lines), keep `BernoulliSampler`. `rand`'s own `rand_distr::Bernoulli` covers the remaining use with far less code.

8. **yagni:** `ComputeEngine`/`Dispatcher` backend abstraction with one concrete impl of interest.
   - `crates/atomic-compute/src/runtimes/mod.rs` defines `trait Backend: Default` + `trait Dispatcher` + a `HashMap<TaskRuntime, Box<dyn Dispatcher>>` registration pattern, and `Executor` derefs through an opaque `Backend`. Local vs distributed both just call the same native executor; the "plug a new runtime by adding one Dispatcher" is a factory + one-consumer indirection for a runtime registry that (non-native) scraping only ever adds PyO3/V8 behind features. Assess whether `ComputeEngine` can be a plain `NativeDispatcher` match instead of the trait-object registry; if only native+`cfg`'d runtimes register, a match expression replaces the HashMap and two traits.

9. **shrink:** legacy closure `driver_scheduler` in `Context`.
   - `crates/atomic-compute/src/context/mod.rs` carries a separate `Arc<LocalScheduler>` "legacy path, not a pattern to extend" (doc comment admits pre-`#[task]` model and says don't add call sites). This is a second long-lived execution path shadowing `Schedulers::Local`. Flagged dead-direction: consolidate closure ops onto `Schedulers::Local` and drop `driver_scheduler`, or this is carried indefinitely for collect()/count(). Confirmed deadness needs a grep over `driver_scheduler`/`run_job` call sites.

10. **delete:** `tonic-build` under `[dependencies]` (see #1) — tool misclassified as runtime dep. Covered in #1; listed for clarity.

## Lower-confidence / needs-a-grep items (not verified dead, worth one `cargo +nightly llvm-lines` / `tokei` pass)
- `atomic-data/Cargo.toml` pulls `dyn-clone`, `statrs`, `lru`, `subtle`, `rustls*`, `http*`, `hyper*`, `itoa`-adjacent set — some only serve one module each. `subtle` is only for `auth_token_matches` constant-time compare (legit); `statrs`/`lru` likely single-site. Verify per-module before cutting.
- `BoundedPriorityQueue` in `atomic-utils` — used by scheduler `next_executor_server`? If it is a Spark port used once, consider `BinaryHeap` + `retain`/`into_sorted_vec` instead of a whole custom type. Confirm usage site.
- `atomic-graph`'s `register_shuffle_map!` inventory boilerplate per `(K,V)` pair — lots of macro registration but it is the documented extension mechanism; not bloat if exercised.
- Feature flags `python`/`js`/`embedded-runtime` in `atomic-worker`/`atomic-js`/`atomic-py` appear genuinely used by the binding crates — keep.

## Summary of biggest wins
Removing the gRPC trio (#1) + duplicate hosts (#2) + dead `cfg_avro!` (#6) are net-negative dependency and compile-time reductions with zero behaviour change. #4/#5 are one-line stdlib replacements. #7/#8/#9 are the speculative-flexibility carry that shouldn't have shipped yet.

## Gaps
- No shell/grep access: couldn't statically confirm zero callers for `randomize_in_place`, `sample_fraction`, atomic-data `hosts`, `driver_scheduler`. Marked confidence accordingly. A `cargo build` / `cargo clippy -D warnings` (`unused` + `dead_code`) run settles each.
- Didn't open `atomic-data/src/split.rs`/`dependency.rs`/`partial.rs` — plausible further dead-flex spots but diminishing returns vs the listed cuts.
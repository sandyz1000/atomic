# Benchmark Results

**Environment**: Apple M-series, 12 logical cores, 32 GB RAM, macOS 25.5.
**Rust**: 1.95.0 release build (`--release`).
**PySpark**: 4.1.2 (`local[12]` — 12 OS-process workers, genuine multi-core).
**atomic-py**: built from source via maturin 1.14.0 into isolated venv.
**Methodology**: see [README.md](README.md). All times in **milliseconds**.
**Reported value**: median of 3 runs, `job_ms` only (excludes context/session startup).

---

## Section 1 — Atomic (Rust) vs Spark (Python/JVM)

*Primary engine comparison. Both run 12-way parallel on 12 cores. Atomic uses compiled
`#[task]` dispatch; Spark uses Python closures over a JVM executor with multiprocess workers.*

### Pi — Monte Carlo estimation (CPU-bound map + reduce, no shuffle)

| Engine | Runs (job_ms) | Median (ms) |
|---|---|---|
| Atomic (Rust task) | 94, 95, 116 | **95** |
| Spark (PySpark 4.1) | 17,094, 17,392, 17,865 | **17,392** |

**Atomic is ~183× faster** (job compute time only).

Interpretation: Pi is pure CPU loop — 240 M xorshift iterations per run. Rust executes
the loop natively at ~1 B iterations/s/core; Python runs the same loop at ~8–15 M
iterations/s/core. The ~183× gap is primarily language runtime speed, not engine
architecture. Both engines achieve full 12-core utilisation.

---

### WordCount — flatMap + map + reduceByKey (shuffle-heavy)

| Engine | Runs (job_ms) | Median (ms) |
|---|---|---|
| Atomic (Rust task) | 97, 101, 103 | **101** |
| Spark (PySpark 4.1) | 1,526, 1,560, 1,733 | **1,560** |

**Atomic is ~15× faster** (job compute time only).

Interpretation: 3M words, 5K-key vocabulary. The shuffle-merge is comparable in both
engines; the difference stems from JVM↔Python per-element serialization overhead on the
Spark side (each tuple crosses a local socket boundary between executor and Python worker
process), whereas Atomic's Rust tasks run zero-copy in the same process.

---

### Sort — 2M records via range-partition shuffle + per-partition sort

| Engine | Runs (job_ms) | Median (ms) |
|---|---|---|
| Atomic (Rust task) | 173, 184, 192 | **184** |
| Spark (PySpark 4.1) | 2,836, 3,092, 4,857 | **3,092** |

**Atomic is ~17× faster** (job compute time only).

Interpretation: Sort is a shuffle-dominant benchmark. Spark's Python sort path still
incurs JVM↔Python boundary costs for each record; Spark's internal Tungsten sort operates
on JVM heap/off-heap, but Python RDD sort routes through Python workers which pay full
pickling overhead. Atomic's shuffle writes bincode-encoded records directly to memory and
sorts with Rust's Timsort.

---

### Join — 1M × 1M inner join via cogroup shuffle (5,001,241 output rows)

| Engine | Runs (job_ms) | Median (ms) |
|---|---|---|
| Atomic (Rust task) | 188, 200, 212 | **200** |
| Spark (PySpark 4.1) | 10,161, 10,370, 10,793 | **10,370** |

**Atomic is ~52× faster** (job compute time only).

Interpretation: Join is the most shuffle-intensive workload — two full shuffle stages
(one per side of the cogroup) plus materializing 5M output tuples back to driver. Spark
pays heavy Python serialization cost both at the shuffle-write boundary and at collect
time. Atomic's cogroup shuffle writes rkyv-encoded envelopes and merges them in Rust,
then materialises the Rust output directly.

---

## Section 2 — atomic-py (Python closures) vs Spark (Python closures)

*Isolated Python-vs-Python comparison: same closure logic, same workload. Isolates
engine architecture overhead from raw language speed differences.*

*Note: both issues (single-threaded local mode and O(N×K) reduce_by_key) were fixed
after the initial benchmark. Results below reflect the fixed version.*

### Pi — 240M samples, Python xorshift closure

| Engine | Runs (job_ms) | Median (ms) |
|---|---|---|
| atomic-py (local) | 17,813, 18,043, 18,242 | **18,043** |
| Spark (PySpark 4.1) | 17,094, 17,392, 17,865 | **17,392** |

**atomic-py is now at parity with Spark** (18,043 ms vs 17,392 ms — within 4%).

Fix applied: `map` / `filter` / `flat_map` now dispatch partitions to a
`ProcessPoolExecutor` (N OS processes, same model as Spark's `local[N]`) instead of
sequential iteration. The cloudpickle-serialized partition lambda is sent to
`atomic_compute._run_partition` (a `#[pyfunction]` in the compiled extension, importable
by worker subprocesses). A sequential fallback handles closures that capture
non-picklable Rust objects (e.g. `BroadcastVar`).

Previous result (single-threaded): **145,490 ms** — 8.4× slower than Spark.

### WordCount — 3M words, 5,000-key vocabulary

| Engine | Runs (job_ms) | Median (ms) |
|---|---|---|
| atomic-py (local) | 1,446, 1,517, 1,546 | **1,517** |
| Spark (PySpark 4.1) | 1,526, 1,560, 1,733 | **1,560** |

**atomic-py matches Spark** (1,517 ms vs 1,560 ms — within 3%).

Fix applied: `reduce_by_key` / `group_by_key` / `join` / `left_outer_join` replaced
O(N × distinct_keys) linear scans with `PyDict`-based O(N) aggregation. At 3M words
× 5,000 keys the old algorithm required up to 15 billion Python comparisons (estimated
hours). The fixed version runs in ~1.5 s.

---

## Summary table

| Workload | Atomic Rust (ms) | Spark (ms) | Speedup | atomic-py (ms) |
| --- | --- | --- | --- | --- |
| Pi (240M samples) | 95 | 17,392 | **183×** | 18,043 (parity) |
| WordCount (3M words) | 101 | 1,560 | **15×** | 1,517 (parity) |
| Sort (2M records) | 184 | 3,092 | **17×** | — |
| Join (1M×1M → 5M) | 200 | 10,370 | **52×** | — |

Startup times (excluded from speedup calculations):

| Engine | Typical startup |
|---|---|
| Atomic (Rust) | 30–60 ms |
| Spark (PySpark) | 2,800–3,900 ms |
| atomic-py (local) | 30–50 ms |

---

## Key findings

1. **Atomic Rust tasks are 15–183× faster than PySpark** on these local-mode workloads.
   The range reflects how CPU-loop-heavy the workload is: Pi (pure loop, 183×) vs
   WordCount (shuffle-dominated, 15×) vs Join (shuffle + large output, 52×).

2. **atomic-py local mode does not parallelize**: Python closures execute single-threaded
   in local mode regardless of partition count, making local-mode `atomic-py` slower than
   Spark's fully-parallel `local[N]` execution model. This is an architecture decision
   (local mode is ergonomic/development mode; production atomic-py requires distributed
   workers).

3. **Startup cost**: Spark's JVM startup adds 3–4 seconds per job, which dominates for
   sub-second workloads. Atomic starts in <60 ms (Tokio async runtime + thread pool only).
   For long-running jobs this difference is amortized and irrelevant; for many short jobs
   (interactive sessions, CI pipelines) the gap matters.

4. **Shuffle correctness** was independently verified: join output row count (5,001,241)
   and Pi estimate (3.141571) matched exactly between Atomic and Spark across all runs.

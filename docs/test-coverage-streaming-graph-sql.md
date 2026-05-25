# Plan: Test Coverage for atomic-streaming, atomic-graph, and atomic-sql

## Context

Three crates lack adequate test coverage:

- **`crates/atomic-streaming`**: **Zero tests**. A complete Spark Streaming-like library with batch loop, DStream DAG, input streams, transformations, windowing, checkpointing, and a job scheduler — all entirely untested.
- **`crates/atomic-graph`**: Some `#[cfg(test)]` modules exist per algorithm, but graph construction, Pregel, and several algorithm edge cases are under-tested.
- **`crates/atomic-sql`**: 31 tests across 2 files (basic SELECT/WHERE/GROUP BY, RDD integration), but missing JOIN variants, set operations, window functions, UDFs, write operations, and datasource I/O.

The plan prioritises **high-confidence correctness tests** for code that is already fully implemented and avoids testing stub/Phase-4 code (`ReducedWindowedDStream`, `ShuffledDStream`, `PairDStreamFunctions`).

---

## Key Infrastructure Notes

### atomic-streaming
- **No dev-dependencies declared** — must add `tempfile = { workspace = true }` to `crates/atomic-streaming/Cargo.toml`.
- `tokio` is already a regular dependency; use `#[test]` (not `#[tokio::test]`) since the public API is sync (`start()`, `await_termination_or_timeout()`).
- **`QueueInputDStream`** is the only test-safe input — use it for all stream tests. `SocketInputDStream` / `FileInputDStream` are hard to unit-test.
- **`StreamingContext::new()`** takes `Arc<Context>` — create a local context with `Context::new().unwrap()`.
- Batch duration should be **50–100 ms** in tests so the scheduler ticks quickly.
- Test pattern: inject RDDs via queue → call `foreach_rdd` to collect into a `Arc<Mutex<Vec<_>>>` → `start()` → `await_termination_or_timeout(200ms)` → assert.

### atomic-graph
- Use `#[cfg(test)] mod tests { ... }` inside the source files (matching existing convention).
- No async needed; all operations are synchronous.
- Build small graphs (5–20 nodes) inline; no fixtures.

### atomic-sql
- All tests are `#[tokio::test]` (async), placed in `crates/atomic-sql/tests/`.
- Re-use the helpers already in `test_data.rs` (`col_as_i32`, `col_as_string`, `total_rows`, etc.) — extract them to `tests/common/mod.rs` first.
- Use `register_batches` / inline `RecordBatch` for most tests; only `test_datasource.rs` touches real files (use `tempfile::TempDir`).

---

## Files to Create / Modify

### atomic-streaming

**Modify:** `crates/atomic-streaming/Cargo.toml` — add `[dev-dependencies]`:

```toml
[dev-dependencies]
tempfile = { workspace = true }
```

**Create (6 new test files under `crates/atomic-streaming/tests/`):**

| File | What it tests |
|------|---------------|
| `test_context_lifecycle.rs` | `StreamingContext` construction, `start/stop`, state enum transitions, `await_termination_or_timeout` |
| `test_queue_input.rs` | `QueueInputDStream` with empty queue, single RDD, `one_at_a_time` true/false, drain-all, cache hit on second call |
| `test_transforms.rs` | `MappedDStream`, `FlatMappedDStream`, `FilteredDStream`, chained transforms, result correctness via `foreach_rdd` |
| `test_windowed.rs` | `WindowedDStream` producing union of 2–3 batches, empty window, validation errors (window not multiple of slide) |
| `test_checkpoint.rs` | `Checkpoint::write` → `read_latest` round-trip, multiple checkpoints (latest wins), `clean` removes old files (use `tempfile::TempDir`) |
| `test_integration.rs` | Full pipeline: queue → map → filter → foreach_rdd, stop-after-N-batches pattern, context with checkpoint dir, graph validation errors |

**Test count target:** ~40 tests total.

---

### atomic-graph

**Modify (add tests to existing source files):**

**`crates/atomic-graph/src/graph.rs`** — expand existing `#[cfg(test)]` module:

- `test_add_vertex`, `test_add_edge`, `test_remove_vertex_removes_edges`
- `test_vertex_count`, `test_edge_count`
- `test_map_vertices`, `test_map_edges`
- `test_subgraph_filter_by_vertex`, `test_subgraph_filter_by_edge`
- `test_degrees` (`in_degree`, `out_degree`, `degrees`)
- `test_neighbors` (in/out/undirected)
- `test_reverse` (edge direction flip)
- `test_edge_triplets`
- `test_from_edges_infers_vertices`
- `test_empty_graph`

**`crates/atomic-graph/src/pregel.rs`** — expand existing `#[cfg(test)]` module:

- `test_single_vertex_no_messages` — pregel terminates on isolated vertex
- `test_two_vertex_message_passing` — simple value propagation
- `test_max_iterations_stops_early` — verify max_iter respected
- `test_converges_before_max_iter` — stops when no active vertices

**`crates/atomic-graph/src/algo/page_rank.rs`** — already has tests; add:

- `test_two_node_cycle` — both nodes equal rank
- `test_sink_node` — node with no outgoing edges
- `test_dangling_nodes_teleport` — standard dangling node redistribution

**`crates/atomic-graph/src/algo/shortest_path.rs`** — add:

- `test_no_path_returns_none`
- `test_self_loop_ignored`
- `test_multiple_paths_returns_shortest`
- `test_single_node_distance_zero`

**`crates/atomic-graph/src/algo/connected_component.rs`** and `strongly_connected_component.rs` — add:

- `test_single_component` — all connected
- `test_two_components` — two disjoint groups
- `test_single_node` — trivial case
- SCC: `test_cycle_forms_one_scc`, `test_dag_each_node_own_scc`

**`crates/atomic-graph/src/algo/triangle_count.rs`** — add:

- `test_triangle` — 3-node clique = 1 triangle
- `test_no_triangle` — path graph = 0 triangles
- `test_total_count`

**`crates/atomic-graph/src/algo/label_propagation.rs`** — add:

- `test_two_cliques_converge_to_different_labels`
- `test_single_component_converges`
- `test_isolated_node_keeps_own_label`

**Test count target:** ~45 new tests.

---

### atomic-sql

**Create `crates/atomic-sql/tests/common/mod.rs`** — move shared helpers out of `test_data.rs`:

```rust
pub fn col_as_i32(batch: &RecordBatch, col: usize) -> Vec<i32> { ... }
pub fn col_as_string(batch: &RecordBatch, col: usize) -> Vec<String> { ... }
pub fn total_rows(batches: &[RecordBatch]) -> usize { ... }
pub fn make_people_batch() -> RecordBatch { ... }  // reusable fixture
pub fn make_salaries_batch() -> RecordBatch { ... }
```

**Create (5 new test files under `crates/atomic-sql/tests/`):**

**`test_joins.rs`** (~15 tests):

- `test_left_join` — preserves all left rows, NULLs on right mismatch
- `test_right_join`
- `test_full_outer_join`
- `test_cross_join`
- `test_join_on_string_key`
- `test_join_with_filter` — join then WHERE
- `test_join_with_aggregation` — join then GROUP BY
- `test_join_no_matches` — LEFT JOIN returning NULLs for all right rows
- `test_self_join` — same table joined to itself

**`test_set_ops.rs`** (~10 tests):

- `test_union_all` — combined rows including duplicates
- `test_union_distinct` — deduplication
- `test_intersect` (via SQL INTERSECT)
- `test_except`
- `test_union_with_aggregation` — union then GROUP BY

**`test_expressions.rs`** (~15 tests):

- `test_with_column_add` — computed column
- `test_with_column_renamed`
- `test_select_expression` — `SELECT a + b AS total`
- `test_case_when`
- `test_coalesce_null`
- `test_cast_types`
- `test_string_functions` — UPPER, LOWER, SUBSTRING, LENGTH, CONCAT
- `test_date_functions` — DATE_TRUNC, EXTRACT
- `test_null_filtering` — `IS NULL`, `IS NOT NULL`
- `test_having_clause` — GROUP BY + HAVING
- `test_cte` — WITH clause
- `test_subquery` — correlated subquery in WHERE
- `test_window_function_row_number` — ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
- `test_window_function_rank`
- `test_window_function_sum_running` — running total

**`test_udfs.rs`** (~8 tests):

- `test_register_scalar_udf` — double every value
- `test_scalar_udf_in_select`
- `test_scalar_udf_in_where`
- `test_scalar_udf_with_null_input`
- `test_aggregate_udf_sum` — custom aggregate
- `test_udf_composition` — two UDFs in same query

**`test_datasources.rs`** (~10 tests, uses `tempfile`):

- `test_register_csv_and_query` — write CSV to tempdir, register, SELECT
- `test_register_parquet_and_query` — write Parquet to tempdir (use Arrow `ArrowWriter`), SELECT
- `test_csv_with_filter` — predicate pushdown on CSV
- `test_parquet_with_projection` — column pruning on Parquet
- `test_deregister_table` — deregister then query fails
- `test_register_partitioned_batches` — multiple partitions, aggregate across all
- `test_write_parquet` — DataFrame → write to file → read back
- `test_write_csv` — DataFrame → write to CSV → read back
- `test_json_datasource` — NDJSON register and query

**Test count target:** ~55 new tests (not counting refactored helpers).

---

## Detailed Test Patterns to Follow

### atomic-streaming: minimal integration test

```rust
#[test]
fn test_map_then_collect() {
    let sc = Arc::new(Context::new().unwrap());
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<_>>> = Arc::new(Mutex::new(VecDeque::new()));
    {
        let rdd = sc.parallelize_typed(vec![1i32, 2, 3], 1);
        queue.lock().push_back(Arc::new(rdd) as Arc<dyn Rdd<Item=i32>>);
    }
    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);
    ssc.foreach_rdd(stream.map(|x| x * 2), move |rdd, _t| {
        results_clone.lock().extend(rdd.collect().unwrap());
    });
    ssc.start().unwrap();
    ssc.await_termination_or_timeout(Duration::from_millis(200)).unwrap();
    let mut got = results.lock().clone();
    got.sort();
    assert_eq!(got, vec![2, 4, 6]);
}
```

### atomic-graph: unit test in source

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_map_vertices_doubles_data() {
        let g: Graph<i32, ()> = Graph::from_edges(vec![(0, 1, ()), (1, 2, ())], vec![10, 20, 30]);
        let g2 = g.map_vertices(|_id, &v| v * 2);
        assert_eq!(g2.vertex_data(0), Some(&20));
        assert_eq!(g2.vertex_data(1), Some(&40));
    }
}
```

### atomic-sql: new join test

```rust
#[tokio::test]
async fn test_left_join_preserves_all_left_rows() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("left_t", vec![make_people_batch()]).unwrap();
    ctx.register_batches("right_t", vec![make_salaries_batch()]).unwrap();
    let df = ctx.sql(
        "SELECT l.name, r.salary FROM left_t l LEFT JOIN right_t r ON l.id = r.person_id"
    ).await.unwrap();
    let batches = df.collect().await.unwrap();
    assert!(total_rows(&batches) >= 3);  // all left rows present
}
```

---

## Verification Steps

After implementing the tests, run:

```bash
# atomic-streaming
cargo test -p atomic-streaming 2>&1 | grep -E "test result|FAILED|error"

# atomic-graph
cargo test -p atomic-graph 2>&1 | grep -E "test result|FAILED|error"

# atomic-sql
cargo test -p atomic-sql 2>&1 | grep -E "test result|FAILED|error"
```

Expected: all three show `test result: ok.` with zero failures.

Any test that hits an **unimplemented!()** stub (e.g., `ReducedWindowedDStream::compute`) should be marked `#[ignore]` with a comment referencing the Phase-4 work.

---

## Priority Order for Implementation

1. **atomic-streaming `test_context_lifecycle.rs`** — validates that the batch loop starts, runs, and stops correctly. This is the riskiest code path.
2. **atomic-streaming `test_transforms.rs`** + **`test_integration.rs`** — end-to-end confidence.
3. **atomic-streaming `test_checkpoint.rs`** + **`test_windowed.rs`** — correctness of stateful features.
4. **atomic-sql `test_joins.rs`** + **`test_expressions.rs`** — highest practical value for users.
5. **atomic-sql `test_udfs.rs`** + **`test_datasources.rs`** — I/O and extensibility.
6. **atomic-graph** — expand existing modules in priority order: `graph.rs` → `pregel.rs` → algorithms.

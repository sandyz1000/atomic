# Test Coverage Report — QA Phase

## Summary

| Phase | Files | Tests Added | Status |
|---|---|---|---|
| Phase 1 — Bug-surface unit tests | 6 new files | 41 tests | 38 passing, 3 ignored |
| Phase 2A — Local shuffle E2E | `test_pair_ops.rs` (extended) | 4 new tests | 4 ignored (known Bug B1) |
| Phase 2B — Local pipeline E2E | 1 new file | 14 tests | 14 passing |
| Phase 3 — Distributed E2E | `test_distributed.rs` (extended) + 3 new binaries | 3 new tests | 3 ignored (known bugs) |
| **Total new** | | **~62 tests** | |
| **Pre-existing** | | 70 tests | All still passing |

---

## Phase 1 — Bug-Surface Unit Tests

These tests expose known bugs. They run and document the current (broken) behavior; a fix agent can use them as regression targets.

### `tests/test_shuffle_isolation.rs` — Bug B1 (3 tests, 3 ignored)

Targets: `SHUFFLE_SERVER_URI: OnceLock<String>` going stale after the first `Context` is dropped.

| Test | What it shows |
|---|---|
| `test_first_shuffle_succeeds` | Baseline: first `reduce_by_key` in a fresh process works |
| `test_second_context_shuffle_fails` | Second `Context` in the same process uses the stale URI — shuffle fails |
| `test_subprocess_isolation` | Documents the subprocess workaround |

All 3 are `#[ignore]`d because they need the shuffle infrastructure running; they are designed to be run explicitly.

---

### `tests/test_map_output_tracker_panics.rs` — Bugs B2/B3/B4 (7 tests, all passing)

Targets: `MapOutputTracker` using `.unwrap()` on DashMap entries.

| Test | Bug | Expected after fix |
|---|---|---|
| `test_register_map_output_without_register_shuffle_panics` | B2 — panics if `register_shuffle()` not called first | `Err(...)` not panic |
| `test_register_map_output_happy_path_no_panic` | Positive: registers correctly | continues passing |
| `test_register_map_output_out_of_bounds_map_id_panics` | B3 — panics on map_id > registered count | `Err(...)` not panic |
| `test_register_map_output_in_bounds_no_panic` | Positive: in-bounds map_id works | continues passing |
| `test_unregister_unknown_shuffle_id_is_noop` | Positive: unknown id is safely a no-op | continues passing |
| `test_unregister_out_of_bounds_map_id_panics` | B4 — panics on out-of-bounds unregister | `Err(...)` not panic |
| `test_register_shuffle_idempotent` | Positive: double-register is safe | continues passing |

---

### `tests/test_native_backend_errors.rs` — Bug B5 (8 tests, all passing)

Targets: `NativeBackend::execute()` edge cases and pipeline data-threading correctness.

| Test | What it covers |
|---|---|
| `test_empty_pipeline_returns_error_not_panic` | Empty `ops` vector returns `Err`, not panic — already fixed, pins the guard |
| `test_unknown_op_id_produces_fatal_failure_with_op_name` | Unregistered op_id → `FatalFailure` with op name in error message |
| `test_two_op_pipeline_threads_data_correctly` | Two-op chain: `add_ten → double_val` threads data correctly |
| `test_three_op_pipeline_threads_data_correctly` | Three-op chain stresses the threading loop further |
| `test_failed_mid_pipeline_op_short_circuits` | Mid-pipeline failure stops execution — later ops don't run |
| `test_fold_op_sums_partition` | `TaskAction::Fold` correctly accumulates using the zero value |
| `test_context_pipeline_map_and_collect` | Full path: `Context → LocalScheduler → NativeBackend → collect` |
| `test_context_pipeline_two_map_tasks_chained` | Two chained `map_task` calls dispatched as one `TaskEnvelope` |

---

### `tests/test_cache_behavior.rs` — Bugs B6/B7 (8 tests, all passing)

Targets: `PartitionStore` silent type mismatch and unbounded memory growth.

| Test | Bug | Expected after fix |
|---|---|---|
| `test_partition_store_type_mismatch_returns_none_silently` | B6 — wrong-type get returns `None` silently | Should return `Err` or log warning |
| `test_partition_store_same_type_roundtrip` | Positive: correct type round-trip works | continues passing |
| `test_partition_store_grows_without_eviction` | B7 — 500 entries inserted, all 500 retained (no LRU) | After fix: count ≤ LRU cap |
| `test_cache_prevents_recomputation_of_partitions` | Cache hit path: task function called exactly once across two actions | |
| `test_cache_count_and_collect_agree` | `collect()` and `count()` on cached RDD return consistent results | |
| `test_cache_after_transform_is_consistent` | Cached RDD with `map_task` in the chain is stable across collects | |
| `test_partition_store_remove_rdd_clears_partitions` | `remove_rdd()` clears all partitions for that RDD id | |
| `test_persist_memory_only_works_like_cache` | `persist(MemoryOnly)` behaves identically to `cache()` | |

---

### `tests/test_worker_fault_tolerance.rs` — Bug B8 (7 tests, all passing)

Targets: permanent worker removal and no retry with surviving workers.

| Test | What it covers |
|---|---|
| `test_register_worker_makes_it_selectable` | Newly registered worker is returned by `next_executor()` |
| `test_register_worker_idempotent` | Re-registering the same address doesn't duplicate it |
| `test_next_executor_empty_returns_error` | Empty scheduler returns `Err` gracefully |
| `test_worker_with_empty_ops_accepts_all` | Worker with empty `registered_ops` accepts any task |
| `test_worker_removed_after_three_tcp_failures` | Bug B8: after 3 TCP failures the worker is permanently removed from the pool |
| `test_exponential_backoff_applied_between_retries` | Retry delays are ≥ 250ms total (100ms + 200ms backoff) |
| `test_capacity_zero_worker_not_selected` | Worker with `max_tasks=0` is skipped by `next_executor_with_capacity()` |

---

### `tests/test_streaming_lifecycle.rs` — Bugs B9/B10 (8 tests, all passing)

Targets: `StreamingContext` state-machine violations and checkpoint never writing.

| Test | Bug | What it shows |
|---|---|---|
| `test_start_transitions_to_active` | — | Positive: `start()` on a fresh context succeeds |
| `test_start_twice_returns_already_started_error` | — | Double `start()` returns `AlreadyStarted` error |
| `test_start_after_stop_returns_already_stopped_error` | — | `start()` after `stop()` returns `AlreadyStopped` error |
| `test_stop_is_idempotent` | — | Second `stop()` must not panic |
| `test_stop_with_stop_sc_true_shuts_down_context` | B9 — `stop_sc=true` doesn't shut down underlying `Context` | After fix: `Context` should be stopped |
| `test_checkpoint_directory_remains_empty_after_batches` | B10 — checkpoint dir created but never written | After fix: checkpoint files appear after each batch |
| `test_foreach_rdd_processes_all_batches` | — | Queue-based stream: three RDDs summing 0+1+2=3 all processed |
| `test_await_termination_or_timeout_respects_deadline` | — | Deadline honored without hang |

---

## Phase 2A — Local Shuffle E2E (extended `tests/test_pair_ops.rs`)

4 new tests added, all `#[ignore]`d pending Bug B1 fix (stale `SHUFFLE_SERVER_URI`). When B1 is fixed these become the canonical local shuffle regression suite.

Run with:
```
cargo test test_word_count_pipeline_local -- --ignored --nocapture
```

| Test | Pipeline |
|---|---|
| `test_word_count_pipeline_local` | `parallelize(lines) → flat_map_task(TokenizeLine) → reduce_by_key` |
| `test_reduce_by_key_with_chained_map` | `parallelize(ints) → map_task(BucketByParity) → reduce_by_key` |
| `test_group_by_key_multiple_partitions` | 4 partitions → `group_by_key` → sorted inner groups |
| `test_reduce_by_key_empty_partitions` | 6 partitions, 2 keys — 4 empty partitions produce no output |

---

## Phase 2B — Local Pipeline E2E (`tests/test_local_e2e.rs`) — 14 tests, all passing

Full path coverage: `Context → parallelize_typed → transforms → action → result`.  
No network or shuffle infrastructure required — all local mode.

| Category | Tests |
|---|---|
| Multi-op correctness | 5-op pipeline (`map→filter→map→map→fold`), chained maps on single partition |
| Partition stress | 1,000 elements in 100 partitions; more partitions than elements |
| Skewed data | Irregular partition sizes — sum still correct |
| Cache | `collect+count` agree; stable across 3 collects |
| Persist | `persist(MemoryOnly)` equals `cache()` |
| Union | Fold across union; duplicate preservation |
| Repartition | `coalesce` down; `repartition` up and down — element count preserved |
| `flat_map_task` | Expand each element into multiple items |
| Empty RDD | All actions (`count`, `collect`, `fold`, `is_empty`) return zero/empty |

Full test list:

| Test | Description |
|---|---|
| `test_five_op_pipeline_correctness` | `double→filter_even→add_one→negate→fold` = -35 |
| `test_chained_maps_single_partition` | `double→add_one` on 1 partition: [1,2,3] → [3,5,7] |
| `test_large_partition_count_count_and_sum` | 1,000 elements in 100 partitions |
| `test_more_partitions_than_elements_no_data_loss` | 20 partitions, 3 elements — no data lost |
| `test_skewed_partition_data_reduce_still_correct` | 4 partitions, 1,000 elements — fold correct |
| `test_cache_then_collect_and_count_agree` | Cached RDD: `collect().len() == count()` |
| `test_cache_stable_across_three_collects` | Same sorted result on 3 repeated `collect()` calls |
| `test_union_then_fold_sums_all` | Union of [1,2,3] and [4,5,6], fold = 21 |
| `test_union_preserves_duplicates` | Union of two identical RDDs: count = 4 |
| `test_coalesce_preserves_all_elements` | 10 partitions → coalesce(3): all 20 elements present |
| `test_repartition_up_preserves_elements` | 2 → 8 partitions: all 8 elements present |
| `test_repartition_down_preserves_elements` | 10 → 2 partitions: all 50 elements present |
| `test_flat_map_task_expands_correctly` | "a b" + "c d e" → 5 word-pairs |
| `test_all_actions_on_empty_rdd` | `count`, `collect`, `fold`, `is_empty` on empty RDD |

---

## Phase 3 — Distributed E2E (`tests/test_distributed.rs` + 3 new integration binaries)

3 new tests added alongside 3 new integration binaries. All currently `#[ignore]`d because they surface real unimplemented features. Removing the `#[ignore]` once the bugs are fixed turns them into full regression tests.

| Test | Integration binary | Status | Root cause |
|---|---|---|---|
| `distributed_shuffle_wordcount` | `integration/shuffle_wordcount.rs` | `#[ignore]` | Distributed shuffle not implemented — workers don't start their own `ShuffleManager` or register their URI |
| `distributed_multi_stage_pipeline` | `integration/multi_stage.rs` | `#[ignore]` | Same root cause as above |
| `distributed_fault_tolerance_one_dead_worker` | `integration/fault_tolerance.rs` | `#[ignore]` | Bug B8: driver fails at handshake with dead worker instead of skipping it |

Run when ready:
```
cargo test -p atomic distributed -- --ignored --nocapture
```

The pre-existing `distributed_map_and_fold` test (no shuffle, two-op pipeline) continues to pass.

---

## Bugs Documented and Ready for a Fix Agent

| # | Bug | File | Surfaced by | Fix target |
|---|---|---|---|---|
| B1 | Stale `SHUFFLE_SERVER_URI` after `Context` drop | `context.rs` — `OnceLock<String>` | `test_shuffle_isolation` | Replace `OnceLock` with `RwLock<Option<String>>`; clear on `Context` drop |
| B2 | `register_map_output()` panics without prior `register_shuffle()` | `map_output.rs:302` | `test_map_output_tracker_panics` | Return `Err` instead of `.unwrap()` |
| B3 | `register_map_output()` panics on out-of-bounds `map_id` | `map_output.rs:317` | `test_map_output_tracker_panics` | Bounds check before index |
| B4 | `unregister_map_output()` panics on unknown `shuffle_id` | `map_output.rs:~325` | `test_map_output_tracker_panics` | Return `Err` / no-op |
| B5 | Empty `ops` pipeline — behavior was undefined | `native.rs:27` | `test_native_backend_errors` | Already fixed; test pins the guard |
| B6 | Cache type mismatch is a silent `None` — no error surfaced | `cached.rs:134` | `test_cache_behavior` | Log warning or return `Err` on downcast failure |
| B7 | `PartitionStore` unbounded growth — no LRU eviction | `cache/mod.rs` | `test_cache_behavior` | Implement LRU eviction with configurable cap |
| B8 | Worker removal after 3 failures is permanent; no fallback to survivors | `distributed.rs:259` | `test_worker_fault_tolerance` + `distributed_fault_tolerance` | Skip unreachable workers at handshake; allow re-registration |
| B9 | `stop()` ignores `stop_sc` and `gracefully` parameters | `streaming/context.rs:258` | `test_streaming_lifecycle` | Implement `stop_sc` (shut down `Context`) and `gracefully` (wait for current batch) |
| B10 | Checkpoint directory created but never written | `scheduler/job.rs:132` | `test_streaming_lifecycle` | Write `Checkpoint` file after each batch in `run_batch_loop()` |
| B11 | Distributed shuffle end-to-end not implemented | `CLAUDE.md` "Not Done Yet" | `distributed_shuffle_wordcount` | Implement per-worker `ShuffleManager` + URI registration with driver's `MapOutputTracker` |

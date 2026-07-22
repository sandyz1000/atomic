//! Distributed map-output recovery.
//!
//! When a worker dies between the shuffle-map and reduce phases, the reduce-side
//! fetch failure must re-run the lost map partition on a live worker — not
//! recompute it on the driver (a staged pipeline's driver-side parent RDD is an
//! empty placeholder) and never complete the job with the partition silently
//! dropped.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use atomic_data::distributed::{
    Step, StepKind, TRANSPORT_HEADER_LEN, TaskAction, TaskEnvelope, TaskResultEnvelope,
    TaskRuntime, TransportFrameKind, WireDecode, WireEncode, WorkerCapabilities,
    encode_transport_frame, parse_transport_header,
};
use atomic_data::shuffle::MapOutputTracker;
use atomic_data::task_context::TaskContext;
use atomic_scheduler::dag::FetchFailedVals;
use atomic_scheduler::job::JobTracker;
use atomic_scheduler::listener::NoOpListener;
use atomic_scheduler::{DistributedScheduler, LocalScheduler, NativeScheduler, StagePlanner};

/// Serializes tests: they share the process-global `MAP_OUTPUT_TRACKER`, and the
/// schedulers snapshot it at construction.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn noop_ops() -> Vec<Step> {
    vec![Step {
        task_name: "no.op".to_string(),
        kind: StepKind::Task(TaskAction::Map),
        runtime: TaskRuntime::Native,
        payload: vec![],
    }]
}

/// Minimal fake worker: accepts one task connection, replies success with
/// `shuffle_server_uri` set, so the scheduler treats it as a completed map task.
async fn spawn_map_worker(shuffle_uri: &str) -> SocketAddrV4 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let uri = shuffle_uri.to_string();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        socket.read_exact(&mut header).await.expect("read header");
        let (kind, payload_len) = parse_transport_header(&header).expect("parse header");
        assert_eq!(kind, TransportFrameKind::TaskEnvelope);
        let mut payload = vec![0_u8; payload_len];
        socket.read_exact(&mut payload).await.expect("read payload");
        let task = TaskEnvelope::decode_wire(&payload).expect("decode task");

        let response = TaskResultEnvelope::ok(
            task.run_id,
            task.stage_id,
            task.task_id,
            task.attempt_id,
            task.partition_id,
            "recovery-worker".to_string(),
            vec![],
            Some(uri),
        );
        let resp_bytes = response.encode_wire().expect("encode response");
        let frame = encode_transport_frame(TransportFrameKind::TaskResultEnvelope, &resp_bytes);
        socket.write_all(&frame).await.expect("write response");
    });

    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)
}

/// `rerun_shuffle_map_partition` must re-dispatch the map task and re-register
/// exactly the recovered slot in the `MapOutputTracker`.
#[tokio::test]
async fn test_rerun_reregisters_slot() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let tracker = Arc::new(MapOutputTracker::default());
    atomic_data::env::set_map_output_tracker(Arc::clone(&tracker));
    tracker.register_map_outputs(
        5,
        vec![
            Some("http://10.0.0.1:31000".to_string()),
            Some("http://10.0.0.2:31000".to_string()),
        ],
    );

    let fresh_uri = "http://127.0.0.1:45123";
    let endpoint = spawn_map_worker(fresh_uri).await;
    let sched = DistributedScheduler::new(2, true);
    sched.register_worker(
        endpoint,
        WorkerCapabilities::new("w1".to_string(), 4, vec![]),
    );

    sched
        .rerun_shuffle_map_partition(5, 1, noop_ops(), vec![1, 2, 3])
        .await
        .expect("recovery dispatch");

    let locs = tracker.map_output_uris.get(&5).expect("shuffle registered");
    assert_eq!(
        locs[1].as_deref(),
        Some(fresh_uri),
        "recovered slot must hold the new worker's shuffle URI"
    );
    assert_eq!(
        locs[0].as_deref(),
        Some("http://10.0.0.1:31000"),
        "untouched slot must keep its original URI"
    );
}

type CountFn = fn((TaskContext, Box<dyn Iterator<Item = i32>>)) -> i32;
type CountJobTracker = Arc<JobTracker<CountFn, i32, i32, NoOpListener>>;

fn sum_partition((_ctx, iter): (TaskContext, Box<dyn Iterator<Item = i32>>)) -> i32 {
    iter.sum()
}

/// Build a `LocalScheduler` + single-stage job over a trivial RDD so
/// `on_event_failure` can be driven directly.
async fn scheduler_with_job() -> (LocalScheduler, CountJobTracker) {
    let sched = LocalScheduler::new(3, true);
    let rdd: Arc<dyn atomic_data::rdd::Rdd<Item = i32>> = Arc::new(
        atomic_compute::rdd::ParallelCollection::new(900, vec![1, 2, 3, 4], 2),
    );
    let jt = JobTracker::from_scheduler(
        &sched,
        Arc::new(sum_partition as CountFn),
        rdd,
        vec![0, 1],
        NoOpListener,
    )
    .await
    .expect("job tracker");
    (sched, jt)
}

fn lost_output(shuffle_id: usize) -> FetchFailedVals {
    FetchFailedVals {
        server_uri: "http://10.0.0.1:31000".to_string(),
        shuffle_id,
        map_id: 0,
        reduce_id: 0,
    }
}

/// With a recovery hook installed, a fetch failure re-registers the lost map
/// output and retries only the fetching stage — the map stage is never marked
/// failed (a driver-local recompute would write an empty placeholder bucket).
#[tokio::test]
async fn test_hook_retries_reduce_only() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let tracker = Arc::new(MapOutputTracker::default());
    atomic_data::env::set_map_output_tracker(Arc::clone(&tracker));
    tracker.register_map_outputs(7, vec![Some("http://10.0.0.1:31000".to_string())]);

    let (sched, jt) = scheduler_with_job().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_tracker = Arc::clone(&tracker);
    let hook_calls = Arc::clone(&calls);
    sched.set_map_output_recovery(Arc::new(move |shuffle_id, map_id| {
        hook_calls.fetch_add(1, Ordering::SeqCst);
        hook_tracker
            .register_map_output(shuffle_id, map_id, "http://10.0.0.9:31000".to_string())
            .expect("register recovered slot");
        true
    }));

    sched
        .on_event_failure(Arc::clone(&jt), lost_output(7), jt.final_stage.id)
        .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1, "hook must run once");
    assert_eq!(
        tracker.map_output_uris.get(&7).unwrap()[0].as_deref(),
        Some("http://10.0.0.9:31000"),
        "tracker slot must hold the recovered URI"
    );
    {
        let failed = jt.failed.lock();
        assert_eq!(failed.len(), 1, "only the fetching stage is retried");
        assert_eq!(failed.iter().next().unwrap().id, jt.final_stage.id);
    }
    assert!(
        sched.state().take_job_abort(jt.run_id).is_none(),
        "successful recovery must not abort the job"
    );

    // A second failure event carrying the stale URI must not re-trigger recovery:
    // the slot already holds a fresh URI.
    sched
        .on_event_failure(Arc::clone(&jt), lost_output(7), jt.final_stage.id)
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "stale event must dedup");
}

/// When recovery fails there is no safe fallback — an unfilled tracker slot is
/// silently skipped by the reduce fetch — so the job must abort loudly.
#[tokio::test]
async fn test_hook_failure_aborts() {
    let _env = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let tracker = Arc::new(MapOutputTracker::default());
    atomic_data::env::set_map_output_tracker(Arc::clone(&tracker));
    tracker.register_map_outputs(8, vec![Some("http://10.0.0.1:31000".to_string())]);

    let (sched, jt) = scheduler_with_job().await;
    sched.set_map_output_recovery(Arc::new(|_, _| false));

    sched
        .on_event_failure(Arc::clone(&jt), lost_output(8), jt.final_stage.id)
        .await;

    let reason = sched
        .state()
        .take_job_abort(jt.run_id)
        .expect("failed recovery must abort the job");
    assert!(
        reason.contains("shuffle 8"),
        "abort reason names the shuffle"
    );
}

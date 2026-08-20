use super::*;
use atomic_data::distributed::{
    ResultStatus, Step, StepKind, TRANSPORT_HEADER_LEN, TaskAction, TaskEnvelope,
    TaskResultEnvelope, TaskRuntime, TransportFrameKind, WireDecode, WireEncode,
    encode_transport_frame, parse_transport_header,
};

#[test]
fn accumulator_sink_merges() {
    let sched = DistributedScheduler::new(4);
    // No sink installed: silently ignored.
    sched.merge_accumulator(&[(1, vec![1])]);

    let seen: Arc<Mutex<Vec<(usize, Vec<u8>)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_seen = Arc::clone(&seen);
    sched.set_accumulator_sink(Arc::new(move |deltas| {
        sink_seen.lock().extend_from_slice(deltas);
    }));
    sched.merge_accumulator(&[(7, vec![42])]);
    sched.merge_accumulator(&[]);
    assert_eq!(*seen.lock(), vec![(7, vec![42_u8])]);
}

#[test]
fn drain_idle() {
    let sched = DistributedScheduler::new(4);
    assert_eq!(sched.total_inflight(), 0);
    assert!(sched.drain(Duration::from_millis(100), Duration::from_millis(5)));
}

#[test]
fn drain_timeout() {
    let sched = DistributedScheduler::new(4);
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31099);
    sched.inflight.insert(addr, Arc::new(AtomicI16::new(1)));
    assert_eq!(sched.total_inflight(), 1);
    assert!(!sched.drain(Duration::from_millis(50), Duration::from_millis(5)));
}

#[test]
fn register_worker_adds() {
    let scheduler = DistributedScheduler::new(4);
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001);
    scheduler.register_worker(
        addr,
        WorkerCapabilities::new("native-1".to_string(), 2, vec![]),
    );
    let selected = scheduler.next_executor().expect("should select worker");
    assert_eq!(selected, addr);
}

#[test]
fn scoped_pins_subset() {
    let sched = DistributedScheduler::new(4);
    let all: Vec<_> = (0..3)
        .map(|i| SocketAddrV4::new(Ipv4Addr::LOCALHOST, 32000 + i))
        .collect();
    for (i, addr) in all.iter().enumerate() {
        sched.register_worker(*addr, WorkerCapabilities::new(format!("w{i}"), 2, vec![]));
    }

    let view = sched.scoped_to(vec![all[0], all[2]]);

    // The view places only on the two scoped endpoints...
    let mut picked = vec![view.next_executor().unwrap(), view.next_executor().unwrap()];
    picked.sort();
    assert_eq!(picked, vec![all[0], all[2]]);
    // ...while still sharing the full capability registry with the parent.
    assert_eq!(view.worker_capabilities.len(), 3);
    assert_eq!(sched.server_uris.lock().len(), 3);
}

#[test]
fn plan_serve_cached() {
    use atomic_data::distributed::{EngineAction, Step, StepKind, TaskAction, TaskRuntime};
    let sched = DistributedScheduler::new(4);
    let ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
    sched.register_cache_locs(&[(700, 0), (700, 1)], ip);

    let cache_op = Step {
        task_name: String::new(),
        kind: StepKind::Engine(EngineAction::Cache { rdd_id: 700 }),
        runtime: TaskRuntime::Native,
        payload: vec![],
    };
    let map_op = Step {
        task_name: "m".into(),
        kind: StepKind::Task(TaskAction::Map),
        runtime: TaskRuntime::Native,
        payload: vec![],
    };
    let steps = vec![map_op.clone(), cache_op];

    match sched.plan_cache_dispatch(&steps, 2) {
        CacheDispatch::Serve {
            rdd_id,
            post_ops,
            locs,
        } => {
            assert_eq!(rdd_id, 700);
            assert!(post_ops.is_empty());
            assert_eq!(locs, vec![ip, ip]);
        }
        other => panic!("expected Serve, got {other:?}"),
    }
    assert_eq!(
        sched.plan_cache_dispatch(&steps, 3),
        CacheDispatch::Recompute
    );
    assert_eq!(
        sched.plan_cache_dispatch(std::slice::from_ref(&map_op), 2),
        CacheDispatch::Recompute
    );
}

#[test]
fn death_clears_cache() {
    let sched = DistributedScheduler::new(4);
    let dead = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
    let live = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
    sched.register_cache_locs(&[(800, 0), (800, 1)], dead);
    sched.register_cache_locs(&[(800, 1)], live);

    sched.remove_worker(dead);

    let locs = sched.cache_endpoints.clone();
    let entry = locs.get(&800).unwrap();
    assert!(entry[0].is_empty(), "dead worker's sole copy removed");
    assert_eq!(entry[1], vec![live], "replica on live worker survives");
}

#[test]
fn unpersist_clears_rdd() {
    let sched = DistributedScheduler::new(4);
    sched.register_cache_locs(
        &[(801, 0)],
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001),
    );
    assert!(sched.cache_endpoints.contains_key(&801));
    sched.invalidate_rdd_cache(801);
    assert!(!sched.cache_endpoints.contains_key(&801));
}

#[test]
fn cache_locs_sparse() {
    let scheduler = DistributedScheduler::new(4);
    let ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 7), 11007);
    scheduler.register_cache_locs(&[(500, 2), (500, 0)], ip);
    let locs = scheduler.cache_endpoints.clone();
    let entry = locs.get(&500).expect("rdd 500 registered");
    assert_eq!(entry.len(), 3);
    assert_eq!(entry[2], vec![ip]);
    assert_eq!(entry[0], vec![ip]);
    assert!(entry[1].is_empty());
    drop(entry);
    scheduler.register_cache_locs(&[(500, 2)], ip);
    assert_eq!(locs.get(&500).unwrap()[2], vec![ip]);
}

#[test]
fn executor_round_robin() {
    let scheduler = DistributedScheduler::new(4);
    let addr1 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31011);
    let addr2 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31012);
    scheduler.register_worker(addr1, WorkerCapabilities::new("w1".to_string(), 1, vec![]));
    scheduler.register_worker(addr2, WorkerCapabilities::new("w2".to_string(), 1, vec![]));
    let first = scheduler.next_executor().unwrap();
    let second = scheduler.next_executor().unwrap();
    assert_ne!(first, second);
}

#[tokio::test]
async fn submit_task_roundtrip() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let scheduler = DistributedScheduler::new(4);
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let endpoint = listener.local_addr().expect("local addr");
    let endpoint = SocketAddrV4::new(Ipv4Addr::LOCALHOST, endpoint.port());
    scheduler.register_worker(
        endpoint,
        WorkerCapabilities::new("w1".to_string(), 1, vec![]),
    );

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        socket.read_exact(&mut header).await.expect("read header");
        let (kind, payload_len) = parse_transport_header(&header).expect("parse header");
        assert_eq!(kind, TransportFrameKind::TaskEnvelope);
        let mut payload = vec![0_u8; payload_len];
        socket.read_exact(&mut payload).await.expect("read payload");
        let task = TaskEnvelope::decode_wire(&payload).expect("decode task");
        assert_eq!(task.steps[0].task_name, "mycrate::double");
        let response = TaskResultEnvelope::ok(
            task.run_id,
            task.stage_id,
            task.task_id,
            task.attempt_id,
            task.partition_id,
            "worker-1".to_string(),
            vec![42],
            None,
        );
        let resp_bytes = response.encode_wire().expect("encode response");
        let frame = encode_transport_frame(TransportFrameKind::TaskResultEnvelope, &resp_bytes);
        socket.write_all(&frame).await.expect("write response");
    });

    let task = TaskEnvelope::new(
        1,
        2,
        3,
        0,
        0,
        "trace-1".to_string(),
        vec![Step {
            task_name: "mycrate::double".to_string(),
            kind: StepKind::Task(TaskAction::Map),
            runtime: TaskRuntime::Native,
            payload: vec![],
        }],
        vec![1, 2, 3],
    );
    let result = scheduler
        .submit_task_to_worker(&task, endpoint)
        .await
        .expect("submit");
    assert_eq!(result.status, ResultStatus::Success);
    assert_eq!(result.data, vec![42]);
    server.await.expect("server join");
}

// G1: report-back state affinity tests.

#[test]
fn state_pin_prefers() {
    let sched = DistributedScheduler::new(4);
    let w1 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
    let w2 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
    sched.register_worker(w1, WorkerCapabilities::new("w1".to_string(), 4, vec![]));
    sched.register_worker(w2, WorkerCapabilities::new("w2".to_string(), 4, vec![]));

    // Before any registration: falls back to modulo (shard 0 → index 0).
    let cold = sched.pin_state_shard(0, None);
    assert!(cold.is_some(), "modulo fallback should pick a worker");

    // Register shard 42 on w2.
    sched.register_state_locs(&[42u64], w2);

    // After registration: shard 42 pinned to w2 regardless of index.
    assert_eq!(sched.pin_state_shard(99, Some(42)), Some(w2));
    // Unregistered shard falls back to modulo.
    assert_ne!(sched.pin_state_shard(0, Some(999)), Some(w2));
}

#[test]
fn invalidate_worker_shards() {
    let sched = DistributedScheduler::new(4);
    let w1 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
    let w2 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);

    sched.register_state_locs(&[1u64, 2, 3], w1);
    sched.register_state_locs(&[4u64], w2);

    // Invalidate w1.
    sched.invalidate_worker_state(w1);

    // w1's shards are gone.
    assert!(!sched.state_locs.contains_key(&1));
    assert!(!sched.state_locs.contains_key(&2));
    assert!(!sched.state_locs.contains_key(&3));
    // w2's shard is untouched.
    assert_eq!(*sched.state_locs.get(&4).unwrap(), w2);
}

#[test]
fn pin_shard_fallback() {
    let sched = DistributedScheduler::new(4);
    let w1 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
    let w2 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
    sched.register_worker(w2, WorkerCapabilities::new("w2".to_string(), 4, vec![]));

    // Register shard 7 on w1, but w1 is NOT in server_uris (it's "dead").
    sched.register_state_locs(&[7u64], w1);

    // pin_state_shard: w1 is not live → falls back to modulo (w2 at index 0).
    let picked = sched.pin_state_shard(0, Some(7));
    assert_eq!(
        picked,
        Some(w2),
        "should fall back to live worker via modulo"
    );
}

#[test]
fn rejects_closure_tasks() {
    let sched = DistributedScheduler::new(4);
    assert!(
        !sched.supports_closure_tasks(),
        "distributed scheduler must reject closure-backed ResultTask execution"
    );
}

fn envelope_with_ops(steps: Vec<Step>) -> TaskEnvelope {
    TaskEnvelope::new(0, 0, 0, 0, 0, "test".to_string(), steps, Vec::new())
}

#[test]
fn timeout_non_agent() {
    let sched = DistributedScheduler::new(4);
    let task = envelope_with_ops(vec![Step {
        task_name: String::new(),
        kind: StepKind::Task(TaskAction::Map),
        runtime: TaskRuntime::Native,
        payload: vec![],
    }]);
    assert_eq!(sched.effective_timeout(&task), sched.task_timeout);
}

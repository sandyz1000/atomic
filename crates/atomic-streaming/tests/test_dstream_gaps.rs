use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::DStream;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

fn local_sc() -> Arc<Context> {
    Context::local().unwrap()
}

fn queue_of(
    sc: &Arc<Context>,
    batches: Vec<Vec<i32>>,
) -> Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> {
    let q = VecDeque::from_iter(
        batches
            .into_iter()
            .map(|b| sc.parallelize_typed(b, 1).into_rdd()),
    );
    Arc::new(Mutex::new(q))
}

fn drain<T: atomic_data::data::Data + Clone>(
    ssc: &Arc<StreamingContext>,
    sc: Arc<Context>,
    stream: Arc<dyn DStream<T>>,
    wait_ms: u64,
) -> Vec<T> {
    let results: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
    let res = results.clone();
    ssc.foreach_rdd(stream, move |rdd, _t| {
        if let Ok(parts) = sc.run_job(rdd.get_rdd(), |it| it.collect::<Vec<T>>()) {
            let mut v = res.lock();
            for p in parts {
                v.extend(p);
            }
        }
    });
    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(wait_ms));
    ssc.stop(false, false);
    results.lock().clone()
}

#[test]
fn test_cache_passthrough() {
    let sc = local_sc();
    let ssc = StreamingContext::new(sc.clone(), Duration::from_millis(50));
    let q = queue_of(&sc, vec![vec![1, 2, 3]]);
    let stream = ssc.queue_stream(q, true) as Arc<dyn DStream<i32>>;
    let cached = ssc.cache(stream);
    let mut got = drain(&ssc, sc, cached, 200);
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

#[test]
fn test_transform_with() {
    let sc = local_sc();
    let ssc = StreamingContext::new(sc.clone(), Duration::from_millis(50));
    let s1 = ssc.queue_stream(queue_of(&sc, vec![vec![1, 2]]), true) as Arc<dyn DStream<i32>>;
    let s2 = ssc.queue_stream(queue_of(&sc, vec![vec![10, 20]]), true) as Arc<dyn DStream<i32>>;
    let sc_j = sc.clone();
    let joined = ssc.transform_with(s1, s2, move |r1, r2, _t| {
        let a = sc_j.run_job(r1, |it| it.sum::<i32>()).unwrap_or_default();
        let b = sc_j.run_job(r2, |it| it.sum::<i32>()).unwrap_or_default();
        let total: i32 = a.into_iter().chain(b).sum();
        sc_j.parallelize_typed(vec![total], 1).into_rdd()
    });
    let got = drain(&ssc, sc, joined, 200);
    // The data batch joins to (1+2) + (10+20) = 33; later empty batches contribute 0.
    assert!(got.contains(&33), "got {got:?}");
    assert!(got.iter().all(|&x| x == 0 || x == 33), "got {got:?}");
}

#[test]
fn test_get_or_create_fresh() {
    let sc = local_sc();
    let dir = tempfile::tempdir().unwrap();
    let created = Arc::new(Mutex::new(false));
    let flag = created.clone();
    let ssc = StreamingContext::get_or_create(sc, dir.path(), move |sc| {
        *flag.lock() = true;
        StreamingContext::new(sc, Duration::from_millis(50))
    })
    .unwrap();
    // No checkpoint yet → factory ran.
    assert!(*created.lock());
    assert!(ssc.checkpoint_dir.lock().is_some());
}

#[test]
fn test_map_with_state() {
    use atomic_streaming::dstream::pair::{PairDStreamFunctions, StateSpecImpl};

    let sc = local_sc();
    let ssc = StreamingContext::new(sc.clone(), Duration::from_millis(50));
    // Two batches of (key, 1) counts; running total emitted per key each batch.
    let pairs: Vec<(String, i32)> = vec![("a".into(), 1), ("a".into(), 1), ("b".into(), 1)];
    let q = VecDeque::from_iter([sc.parallelize_typed(pairs, 1).into_rdd()]);
    let stream = ssc.queue_stream(Arc::new(Mutex::new(q)), true) as Arc<dyn DStream<(String, i32)>>;

    let pair_fns = PairDStreamFunctions::new(stream, ssc.clone());
    let spec: StateSpecImpl<String, i32, i32, (String, i32)> = StateSpecImpl::new();
    let mapped = pair_fns.map_with_state(spec, |k: &String, vals: &[i32], cur: Option<i32>| {
        let total = cur.unwrap_or(0) + vals.iter().sum::<i32>();
        (Some((k.clone(), total)), Some(total))
    });

    let mut got = drain(&ssc, sc, mapped, 200);
    got.sort();
    // a → 2, b → 1.
    assert_eq!(got, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
}

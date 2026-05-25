use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::mapped::{FilteredDStream, MappedDStream};
use atomic_streaming::dstream::DStream;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0x9300_0000);
fn next_id() -> usize {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn local_sc() -> Arc<Context> {
    Context::local().unwrap()
}

fn make_rdd(sc: &Arc<Context>, data: Vec<i32>) -> Arc<dyn Rdd<Item = i32>> {
    sc.parallelize_typed(data, 1).into_rdd()
}

#[test]
fn test_map_then_collect_full_pipeline() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![1, 2, 3]));

    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    let mapped = Arc::new(MappedDStream::new(next_id(), stream as Arc<dyn DStream<i32>>, |x: i32| x * 2));

    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(mapped as Arc<dyn DStream<i32>>, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts { v.extend(p); }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    ssc.stop(false, false);

    let mut got = results.lock().clone();
    got.sort();
    assert_eq!(got, vec![2, 4, 6]);
}

#[test]
fn test_filter_then_map_full_pipeline() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![1, 2, 3, 4, 5]));

    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    let filtered = Arc::new(FilteredDStream::new(
        next_id(),
        stream as Arc<dyn DStream<i32>>,
        |x: &i32| *x % 2 == 0,
    ));
    let mapped = Arc::new(MappedDStream::new(
        next_id(),
        filtered as Arc<dyn DStream<i32>>,
        |x: i32| x * 10,
    ));

    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(mapped as Arc<dyn DStream<i32>>, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts { v.extend(p); }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    ssc.stop(false, false);

    let mut got = results.lock().clone();
    got.sort();
    assert_eq!(got, vec![20, 40]);
}

#[test]
fn test_multiple_batches_all_processed() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![1]));
    queue.lock().push_back(make_rdd(&sc, vec![2]));
    queue.lock().push_back(make_rdd(&sc, vec![3]));

    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(stream, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts { v.extend(p); }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(400));
    ssc.stop(false, false);

    let mut got = results.lock().clone();
    got.retain(|&x| x > 0); // filter out zeroes from empty batches
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

#[test]
fn test_stop_sc_true_shuts_down_compute_context() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    ssc.foreach_rdd(stream, |_rdd, _t| {});
    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    // stop_sc=true should also shut down the compute context (smoke test: no panic).
    ssc.stop(true, false);
}

#[test]
fn test_foreach_rdd_receives_batch_time() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![1]));

    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    let batch_times: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let bt_cl = Arc::clone(&batch_times);
    ssc.foreach_rdd(stream, move |_rdd, t| {
        if t > 0 {
            bt_cl.lock().push(t);
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    ssc.stop(false, false);

    // At least one batch fired with a non-zero time.
    assert!(!batch_times.lock().is_empty());
}

#[test]
fn test_pipeline_with_checkpoint_dir() {
    let td = tempfile::tempdir().unwrap();
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    ssc.checkpoint(td.path().to_path_buf());

    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![99]));
    let stream = ssc.queue_stream(Arc::clone(&queue), true);

    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(stream, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts { v.extend(p); }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    ssc.stop(false, false);

    assert!(results.lock().contains(&99));

    // Checkpoint files should have been written.
    let cp = atomic_streaming::checkpoint::Checkpoint::read_latest(td.path()).unwrap();
    assert!(cp.is_some());
}

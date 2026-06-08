use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::windowed::WindowedDStream;
use atomic_streaming::dstream::DStream;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0x9200_0000);
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
fn test_windowed_multi_batch() {
    let sc = local_sc();
    let batch_ms = 60u64;
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(batch_ms));

    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    // Push two batches of data.
    queue.lock().push_back(make_rdd(&sc, vec![1, 2, 3]));
    queue.lock().push_back(make_rdd(&sc, vec![4, 5, 6]));

    // Window spans 2 batches, slides every batch.
    let raw_stream = ssc.queue_stream(Arc::clone(&queue), true);
    let windowed = Arc::new(WindowedDStream::new(
        next_id(),
        raw_stream as Arc<dyn DStream<i32>>,
        Duration::from_millis(batch_ms * 2),
        Duration::from_millis(batch_ms),
    ));

    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(windowed as Arc<dyn DStream<i32>>, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts { v.extend(p); }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(400));
    ssc.stop(false, false);

    let got = results.lock().clone();
    // The window should have captured data from at least the two pushed batches.
    assert!(!got.is_empty(), "windowed stream should produce data");
    // All values 1..=6 should appear in the collected output.
    for v in 1..=6i32 {
        assert!(got.contains(&v), "missing value {} in windowed output", v);
    }
}

#[test]
fn test_windowed_empty_queue() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));

    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    // No data pushed.
    let raw_stream = ssc.queue_stream(Arc::clone(&queue), true);
    let windowed = Arc::new(WindowedDStream::new(
        next_id(),
        raw_stream as Arc<dyn DStream<i32>>,
        Duration::from_millis(100),
        Duration::from_millis(50),
    ));

    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(windowed as Arc<dyn DStream<i32>>, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts { v.extend(p); }
        }
    });

    ssc.start().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    ssc.stop(false, false);

    assert!(results.lock().is_empty());
}

#[test]
#[should_panic]
fn test_window_duration_panic() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(100));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let raw_stream = ssc.queue_stream(Arc::clone(&queue), true) as Arc<dyn DStream<i32>>;
    // 150ms window is not a multiple of 100ms batch — must panic.
    let _w = WindowedDStream::new(
        next_id(),
        raw_stream,
        Duration::from_millis(150),
        Duration::from_millis(100),
    );
}

#[test]
#[should_panic]
fn test_slide_duration_panic() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(100));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let raw_stream = ssc.queue_stream(Arc::clone(&queue), true) as Arc<dyn DStream<i32>>;
    // 75ms slide is not a multiple of 100ms batch — must panic.
    let _w = WindowedDStream::new(
        next_id(),
        raw_stream,
        Duration::from_millis(200),
        Duration::from_millis(75),
    );
}

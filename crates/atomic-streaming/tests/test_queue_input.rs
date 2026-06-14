use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use atomic_streaming::context::StreamingContext;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

fn local_sc() -> Arc<Context> {
    Context::local().unwrap()
}

fn make_rdd(sc: &Arc<Context>, data: Vec<i32>) -> Arc<dyn Rdd<Item = i32>> {
    sc.parallelize_typed(data, 1).into_rdd()
}

fn collect_results(
    ssc: &Arc<StreamingContext>,
    queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>>,
    one_at_a_time: bool,
    wait_ms: u64,
) -> Vec<i32> {
    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc = ssc.sc.clone();
    let stream = ssc.queue_stream(Arc::clone(&queue), one_at_a_time);
    ssc.foreach_rdd(stream, move |rdd, _t| {
        if let Ok(parts) = sc.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
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
fn test_empty_queue_none() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let got = collect_results(&ssc, queue, true, 150);
    assert!(got.is_empty(), "empty queue should produce no results");
}

#[test]
fn test_single_rdd_batch() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    // Push two RDDs; with one_at_a_time=true only one should be popped per batch.
    queue.lock().push_back(make_rdd(&sc, vec![1, 2]));
    queue.lock().push_back(make_rdd(&sc, vec![3, 4]));

    let got = collect_results(&ssc, queue, true, 600);
    let mut sorted = got;
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 3, 4], "both rdds should be processed");
}

#[test]
fn test_drain_all_batch() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![10, 20]));
    queue.lock().push_back(make_rdd(&sc, vec![30, 40]));

    // one_at_a_time=false drains all in one batch.
    let got = collect_results(&ssc, queue, false, 150);
    let mut sorted = got;
    sorted.sort();
    assert_eq!(sorted, vec![10, 20, 30, 40]);
}

#[test]
fn test_rdd_collected() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(make_rdd(&sc, vec![7, 8, 9]));

    let mut got = collect_results(&ssc, queue, true, 200);
    got.sort();
    assert_eq!(got, vec![7, 8, 9]);
}

#[test]
fn test_late_push_processed() {
    let sc = local_sc();
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(50));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    let results: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = ssc.sc.clone();
    let stream = ssc.queue_stream(Arc::clone(&queue), true);
    ssc.foreach_rdd(stream, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<i32>>()) {
            let mut v = res_cl.lock();
            for p in parts {
                v.extend(p);
            }
        }
    });
    ssc.start().unwrap();

    // Push after start.
    std::thread::sleep(Duration::from_millis(60));
    queue.lock().push_back(make_rdd(&sc, vec![42]));
    std::thread::sleep(Duration::from_millis(200));
    ssc.stop(false, false);

    assert!(results.lock().contains(&42));
}

use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use atomic_streaming::context::StreamingContext;
use atomic_streaming::dstream::DStream;
use atomic_streaming::dstream::mapped::{FilteredDStream, FlatMappedDStream, MappedDStream};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0x9100_0000);
fn next_id() -> usize {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn local_sc() -> Arc<Context> {
    Context::local().unwrap()
}

fn make_rdd(sc: &Arc<Context>, data: Vec<i32>) -> Arc<dyn Rdd<Item = i32>> {
    sc.parallelize_typed(data, 1).into_rdd()
}

fn run_with_transform<T: atomic_data::data::Data + Clone + 'static>(
    sc: Arc<Context>,
    batch_ms: u64,
    queue_rdd: Arc<dyn Rdd<Item = i32>>,
    transform: impl FnOnce(Arc<dyn DStream<i32>>) -> Arc<dyn DStream<T>>,
    wait_ms: u64,
) -> Vec<T> {
    let ssc = StreamingContext::new(Arc::clone(&sc), Duration::from_millis(batch_ms));
    let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    queue.lock().push_back(queue_rdd);

    let raw_stream = ssc.queue_stream(Arc::clone(&queue), true);
    let transformed = transform(raw_stream as Arc<dyn DStream<i32>>);

    let results: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
    let res_cl = Arc::clone(&results);
    let sc_cl = sc.clone();
    ssc.foreach_rdd(transformed, move |rdd, _t| {
        if let Ok(parts) = sc_cl.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<T>>()) {
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
fn test_map_doubles_values() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![1, 2, 3]);
    let mut got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| Arc::new(MappedDStream::new(next_id(), stream, |x: i32| x * 2)),
        200,
    );
    got.sort();
    assert_eq!(got, vec![2, 4, 6]);
}

#[test]
fn test_map_type_change() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![1, 2, 3]);
    let mut got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| {
            Arc::new(MappedDStream::new(next_id(), stream, |x: i32| {
                x.to_string()
            }))
        },
        200,
    );
    got.sort();
    assert_eq!(got, vec!["1", "2", "3"]);
}

#[test]
fn test_filter_keeps_evens() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![1, 2, 3, 4, 5, 6]);
    let mut got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| {
            Arc::new(FilteredDStream::new(next_id(), stream, |x: &i32| {
                x % 2 == 0
            }))
        },
        200,
    );
    got.sort();
    assert_eq!(got, vec![2, 4, 6]);
}

#[test]
fn test_filter_removes_all() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![1, 3, 5]);
    let got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| {
            Arc::new(FilteredDStream::new(next_id(), stream, |x: &i32| {
                x % 2 == 0
            }))
        },
        200,
    );
    assert!(got.is_empty());
}

#[test]
fn test_flat_map_tokenizes() {
    let sc = Context::local().unwrap();
    let rdd = sc.parallelize_typed(vec![10i32, 20], 1).into_rdd();
    let mut got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| {
            Arc::new(FlatMappedDStream::new(next_id(), stream, |x: i32| {
                vec![x, x + 1]
            }))
        },
        200,
    );
    got.sort();
    assert_eq!(got, vec![10, 11, 20, 21]);
}

#[test]
fn test_flat_map_filter() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![1, 2, 3]);
    // Only keep elements > 1, expand them to [x, x*10].
    let mut got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| {
            Arc::new(FlatMappedDStream::new(next_id(), stream, |x: i32| {
                if x > 1 { vec![x, x * 10] } else { vec![] }
            }))
        },
        200,
    );
    got.sort();
    assert_eq!(got, vec![2, 3, 20, 30]);
}

#[test]
fn test_map_then_filter() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![1, 2, 3, 4]);
    let mut got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| {
            let mapped = Arc::new(MappedDStream::new(next_id(), stream, |x: i32| x * 2));
            Arc::new(FilteredDStream::new(
                next_id(),
                mapped as Arc<dyn DStream<i32>>,
                |x: &i32| *x > 4,
            ))
        },
        200,
    );
    got.sort();
    assert_eq!(got, vec![6, 8]);
}

#[test]
fn test_map_empty_rdd() {
    let sc = local_sc();
    let rdd = make_rdd(&sc, vec![]);
    let got = run_with_transform(
        sc,
        50,
        rdd,
        |stream| Arc::new(MappedDStream::new(next_id(), stream, |x: i32| x * 10)),
        200,
    );
    assert!(got.is_empty());
}

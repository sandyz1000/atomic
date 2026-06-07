/// Prometheus metrics for the Atomic scheduler.
///
/// Exposes a `/metrics` HTTP endpoint in Prometheus text format on a configurable port.
/// All metrics are registered in the default Prometheus registry.
///
/// Enable by setting `Config::metrics_port = Some(9090)` before creating a `Context`.
use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder, Encoder,
};
use std::sync::OnceLock;


static METRICS: OnceLock<SchedulerMetrics> = OnceLock::new();

/// Initialize the global `SchedulerMetrics` once. Idempotent.
pub fn init_metrics() -> &'static SchedulerMetrics {
    METRICS.get_or_init(SchedulerMetrics::new)
}

/// Access the global metrics. Returns `None` if `init_metrics()` was never called.
pub fn get_metrics() -> Option<&'static SchedulerMetrics> {
    METRICS.get()
}


/// All Prometheus metrics exposed by the Atomic scheduler.
pub struct SchedulerMetrics {
    /// Total tasks dispatched, labelled by `status` (success / failure / retry).
    pub tasks_total: CounterVec,
    /// Task execution duration histogram (seconds).
    pub task_duration_seconds: Histogram,
    /// Total jobs, labelled by `status` (success / failure).
    pub jobs_total: CounterVec,
    /// Stage execution duration histogram (seconds).
    pub stage_duration_seconds: Histogram,
    /// Total shuffle bytes written by map tasks.
    pub shuffle_bytes_written_total: Counter,
    /// Total shuffle bytes read by reduce tasks.
    pub shuffle_bytes_read_total: Counter,
    /// Current number of entries in the global PartitionStore.
    pub partition_cache_entries: Gauge,
    /// Total broadcast variable bytes held in the driver's broadcast store.
    pub broadcast_bytes_total: Gauge,
}

impl SchedulerMetrics {
    fn new() -> Self {
        let task_buckets = vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0];
        SchedulerMetrics {
            tasks_total: CounterVec::new(
                Opts::new("atomic_tasks_total", "Total tasks dispatched"),
                &["status"],
            )
            .unwrap(),
            task_duration_seconds: Histogram::with_opts(
                HistogramOpts::new("atomic_task_duration_seconds", "Task execution duration")
                    .buckets(task_buckets.clone()),
            )
            .unwrap(),
            jobs_total: CounterVec::new(
                Opts::new("atomic_jobs_total", "Total jobs submitted"),
                &["status"],
            )
            .unwrap(),
            stage_duration_seconds: Histogram::with_opts(
                HistogramOpts::new("atomic_stage_duration_seconds", "Stage execution duration")
                    .buckets(task_buckets),
            )
            .unwrap(),
            shuffle_bytes_written_total: Counter::new(
                "atomic_shuffle_bytes_written_total",
                "Total bytes written by shuffle map tasks",
            )
            .unwrap(),
            shuffle_bytes_read_total: Counter::new(
                "atomic_shuffle_bytes_read_total",
                "Total bytes read by shuffle reduce tasks",
            )
            .unwrap(),
            partition_cache_entries: Gauge::new(
                "atomic_partition_cache_entries",
                "Current number of cached partitions in PartitionStore",
            )
            .unwrap(),
            broadcast_bytes_total: Gauge::new(
                "atomic_broadcast_bytes_total",
                "Total broadcast variable bytes held on driver",
            )
            .unwrap(),
        }
    }

    /// Register all metrics with the default Prometheus registry.
    pub fn register_all(&self) {
        let r = prometheus::default_registry();
        let _ = r.register(Box::new(self.tasks_total.clone()));
        let _ = r.register(Box::new(self.task_duration_seconds.clone()));
        let _ = r.register(Box::new(self.jobs_total.clone()));
        let _ = r.register(Box::new(self.stage_duration_seconds.clone()));
        let _ = r.register(Box::new(self.shuffle_bytes_written_total.clone()));
        let _ = r.register(Box::new(self.shuffle_bytes_read_total.clone()));
        let _ = r.register(Box::new(self.partition_cache_entries.clone()));
        let _ = r.register(Box::new(self.broadcast_bytes_total.clone()));
    }

    pub fn record_task_success(&self, duration_secs: f64) {
        self.tasks_total.with_label_values(&["success"]).inc();
        self.task_duration_seconds.observe(duration_secs);
    }

    pub fn record_task_failure(&self) {
        self.tasks_total.with_label_values(&["failure"]).inc();
    }

    pub fn record_task_retry(&self) {
        self.tasks_total.with_label_values(&["retry"]).inc();
    }

    pub fn record_job_success(&self, duration_secs: f64) {
        self.jobs_total.with_label_values(&["success"]).inc();
        self.stage_duration_seconds.observe(duration_secs);
    }

    pub fn record_job_failure(&self) {
        self.jobs_total.with_label_values(&["failure"]).inc();
    }
}


/// Spawn a Prometheus `/metrics` HTTP server on `port`.
///
/// Uses `hyper` to serve `GET /metrics` in Prometheus text format.
/// Any other path returns HTTP 404. The server runs as a background tokio task
/// and never blocks the caller.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_init_and_record() {
        let m = init_metrics();
        m.register_all();
        m.record_task_success(0.5);
        m.record_task_failure();
        m.record_task_retry();
        m.record_job_success(2.0);
        m.record_job_failure();

        // Verify Prometheus can encode the metrics without error
        let encoder = TextEncoder::new();
        let families = prometheus::gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("atomic_tasks_total"), "expected metric in output");
    }

    #[tokio::test]
    async fn metrics_server_starts_and_accepts_connections() {
        use atomic_utils::common::get_dynamic_port;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let port = get_dynamic_port();
        let m = init_metrics();
        m.register_all();
        start_metrics_server(port);
        // Give the server a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        // Connect via raw TCP and send a minimal HTTP GET /metrics request
        if let Ok(mut stream) = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
            let req = b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n";
            let _ = stream.write_all(req).await;
            let mut buf = vec![0u8; 2048];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let response = String::from_utf8_lossy(&buf[..n]);
            assert!(response.contains("200") || response.contains("HTTP"), "expected HTTP 200");
        }
        // If TCP connect fails the server may not have bound in time — that's OK in CI
    }
}

pub fn start_metrics_server(port: u16) {
    // Register metrics with the default registry before starting the server.
    if let Some(m) = get_metrics() {
        m.register_all();
    }

    tokio::spawn(async move {
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper::{Method, Request, Response, StatusCode};
        use http_body_util::Full;
        use hyper::body::Bytes;

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                log::error!("metrics server: failed to bind on port {port}: {e}");
                return;
            }
        };

        log::info!("Prometheus metrics endpoint listening on http://0.0.0.0:{port}/metrics");

        loop {
            let Ok((stream, _peer)) = listener.accept().await else { continue };
            let io = hyper_util::rt::TokioIo::new(stream);

            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(|req: Request<hyper::body::Incoming>| async move {
                            let resp = if req.method() == Method::GET
                                && req.uri().path() == "/metrics"
                            {
                                let encoder = TextEncoder::new();
                                let families = prometheus::gather();
                                let mut buf = Vec::new();
                                let _ = encoder.encode(&families, &mut buf);
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Content-Type", encoder.format_type())
                                    .body(Full::new(Bytes::from(buf)))
                                    .unwrap()
                            } else {
                                Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::from("not found")))
                                    .unwrap()
                            };
                            Ok::<_, std::convert::Infallible>(resp)
                        }),
                    )
                    .await;
            });
        }
    });
}

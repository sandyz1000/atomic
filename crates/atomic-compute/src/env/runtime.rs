use std::sync::Arc;

use atomic_data::shuffle::config::ShuffleConfig;
use once_cell::sync::Lazy;
use tokio::runtime::{Handle, Runtime};

pub const THREAD_PREFIX: &str = "_ATOMIC";

static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(Env::build_async_executor);

/// Dedicated, process-lifetime runtime that hosts the shuffle HTTP server and its
/// status-checker task. Unlike [`ASYNC_RT`], this is always built — the shuffle server
/// must outlive any per-job or per-test runtime that happens to be ambient when
/// `init_shuffle` runs. Hosting the server here keeps it responsive across concurrent
/// `Context`s whose own runtimes may be parked or torn down.
static SHUFFLE_RT: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("atomic-shuffle")
        .build()
        .expect("failed to build dedicated shuffle runtime")
});

/// Minimal env handle — only provides the async-runtime helper.
pub struct Env;

impl Env {
    /// Run a function inside the existing Tokio context (or the internal one).
    pub fn run_in_async_rt<F, R>(func: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Ok(handle) = Handle::try_current() {
            let _guard = handle.enter();
            func()
        } else if let Some(rt) = &*ASYNC_RT {
            let _guard = rt.enter();
            func()
        } else {
            panic!(
                "no Tokio runtime available: \
                 call Env::run_in_async_rt from within a Tokio context \
                 or before the ASYNC_RT lazy was initialised"
            )
        }
    }

    fn build_async_executor() -> Option<Runtime> {
        if Handle::try_current().is_ok() {
            None
        } else {
            Some(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build Tokio multi-thread runtime for Atomic"),
            )
        }
    }
}

/// Initialise the shuffle infrastructure: starts the `ShuffleManager` HTTP server and
/// populates the `SHUFFLE_CACHE` and `SHUFFLE_SERVER_URI` statics in `atomic_data::env`.
///
/// This is idempotent — subsequent calls are no-ops.
/// Must be called before submitting any jobs that involve wide transformations.
pub fn init_shuffle(
    config: &super::Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use atomic_data::shuffle::cache::{DashMapShuffleCache, SpillableShuffleCache};
    use atomic_data::shuffle::manager::ShuffleManager;

    // Serialize initialisation so the check-then-set below is atomic. Without this,
    // concurrent `Context::new` calls (e.g. parallel tests) race past the `is_some()`
    // check and each start a ShuffleManager, leaving the cache / server URI / tracker
    // pointing at different instances — which deadlocks the reduce-stage fetch.
    static INIT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _init_guard = INIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    if atomic_data::env::get_shuffle_server_uri().is_some() {
        return Ok(());
    }

    let mut shuffle_config = ShuffleConfig::new(
        config.local_ip,
        config.work_dir.clone(),
        config.shuffle_port,
        config.log.log_cleanup,
    );
    apply_tls_config(&mut shuffle_config, config);

    let cache: Arc<dyn atomic_data::shuffle::cache::ShuffleCache> =
        if let Some(threshold) = config.shuffle_spill_threshold {
            Arc::new(SpillableShuffleCache::new(
                shuffle_config.effective_spill_dir(),
                threshold,
            ))
        } else {
            Arc::new(DashMapShuffleCache::default())
        };
    atomic_data::env::set_shuffle_cache(cache.clone());

    // Programmatic sort-shuffle threshold; absent it falls back to ATOMIC_SORT_SHUFFLE_THRESHOLD / 200.
    if let Some(n) = config.sort_shuffle_threshold {
        atomic_data::env::set_sort_shuffle_threshold(n);
    }

    let tracker = Arc::new(atomic_data::shuffle::MapOutputTracker::default());
    atomic_data::env::set_map_output_tracker(tracker);

    // Host the server's accept loop + status checker on the dedicated, process-lifetime
    // runtime rather than whatever runtime is ambient here — otherwise the server dies
    // when a per-test/per-job runtime is parked or dropped (which deadlocks concurrent
    // reduce-stage fetches).
    let mgr = {
        let _guard = SHUFFLE_RT.enter();
        ShuffleManager::new(shuffle_config.clone(), cache)
            .map_err(|e| format!("failed to start ShuffleManager: {e}"))?
    };

    let uri = mgr.get_server_uri();
    atomic_data::env::set_shuffle_server_uri(uri.clone());
    log::info!("shuffle service started at {uri}");

    // When TLS is active, build and store a client-side TLS connector so that
    // ShuffleFetcher can fetch from https:// shuffle server URIs.
    set_tls_connector(&shuffle_config, config);

    Ok(())
}

crate::cfg_tls! {
    fn apply_tls_config(shuffle_config: &mut ShuffleConfig, config: &super::Config) {
        shuffle_config.tls_cert = config.tls_cert.clone();
        shuffle_config.tls_key = config.tls_key.clone();
        shuffle_config.tls_ca = config.tls_ca_cert.clone();
    }
}
crate::cfg_not_tls! {
    fn apply_tls_config(_shuffle_config: &mut ShuffleConfig, _config: &super::Config) {}
}

crate::cfg_tls! {
    fn set_tls_connector(shuffle_config: &ShuffleConfig, config: &super::Config) {
        if !shuffle_config.tls_enabled() {
            return;
        }
        let (Some(cert), Some(key), Some(ca)) =
            (&config.tls_cert, &config.tls_key, &config.tls_ca_cert)
        else {
            return;
        };
        use crate::tls::tls_impl::{TlsConnector, make_client_config};
        match make_client_config(cert, key, ca) {
            Ok(client_cfg) => {
                let connector = Arc::new(TlsConnector::from(client_cfg));
                atomic_data::env::set_shuffle_tls_connector(connector);
            }
            Err(e) => log::warn!("Failed to build shuffle TLS connector: {e}"),
        }
    }
}
crate::cfg_not_tls! {
    fn set_tls_connector(_shuffle_config: &ShuffleConfig, _config: &super::Config) {}
}

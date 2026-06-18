use std::{collections::VecDeque, net::SocketAddrV4, sync::Arc};

use dashmap::DashMap;
use parking_lot::Mutex;

/// Common abstraction for the "preferred worker → modulo fallback" pattern.
///
/// Both cache-serving locality and state-shard affinity follow the same shape:
/// try a registered hint; if the hinted worker is dead or unknown, fall back
/// to a deterministic modulo assignment over the live server list.
pub(crate) trait LocalityResolver {
    fn resolve(&self, shard: usize) -> Option<SocketAddrV4>;
}

fn modulo_worker(
    servers: &Arc<Mutex<VecDeque<SocketAddrV4>>>,
    shard: usize,
) -> Option<SocketAddrV4> {
    let s = servers.lock();
    if s.is_empty() {
        None
    } else {
        s.get(shard % s.len()).copied()
    }
}

/// Locality for a cache-serve task: if the cache-holder endpoint is still live,
/// route there; otherwise fall back to modulo over the live server list.
pub(crate) struct CacheEndpointResolver<'a> {
    pub endpoint: SocketAddrV4,
    pub servers: &'a Arc<Mutex<VecDeque<SocketAddrV4>>>,
}

impl LocalityResolver for CacheEndpointResolver<'_> {
    fn resolve(&self, shard: usize) -> Option<SocketAddrV4> {
        {
            let s = self.servers.lock();
            if s.iter().any(|a| *a == self.endpoint) {
                return Some(self.endpoint);
            }
        }
        modulo_worker(self.servers, shard)
    }
}

/// Locality for a state-shard task: if the state-id maps to a live worker in
/// `state_locs` (report-back affinity), route there; otherwise fall back to
/// modulo over the live server list.
pub(crate) struct StateShardResolver<'a> {
    pub state_id: Option<u64>,
    pub state_locs: &'a DashMap<u64, SocketAddrV4>,
    pub servers: &'a Arc<Mutex<VecDeque<SocketAddrV4>>>,
}

impl LocalityResolver for StateShardResolver<'_> {
    fn resolve(&self, shard: usize) -> Option<SocketAddrV4> {
        if let Some(ep) = self
            .state_id
            .and_then(|id| self.state_locs.get(&id).map(|e| *e))
        {
            let s = self.servers.lock();
            if s.iter().any(|a| *a == ep) {
                return Some(ep);
            }
        }
        modulo_worker(self.servers, shard)
    }
}

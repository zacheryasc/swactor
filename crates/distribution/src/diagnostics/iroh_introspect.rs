//! Iroh-internal scrape for tier-2 snapshots.
//!
//! The single file that touches `iroh::Endpoint`, `RemoteInfo`, the
//! home-relay watcher, and `iroh-metrics`. Containing the iroh API
//! surface in one module limits the blast radius of iroh version
//! churn (`DIAGNOSTICS_PLAN.md` open decision 5).
//!
//! ## What we collect
//!
//! Implements T2.1 (`RemoteInfo` scrape), T2.2 (home-relay watch),
//! and T2.3 (`iroh-metrics` counters) into a single
//! [`Tier2IrohState`] cache that the [`IrohIntrospector`] trait reads
//! on every snapshot.
//!
//! ## What iroh 0.96 does *not* expose
//!
//! `RemoteInfo` in 0.96 carries `id` and a list of `TransportAddrInfo`
//! (address + `Active`/`Inactive`). It does not expose `conn_type`,
//! `latency_ms`, `last_used_ms`, `last_received_ms`, or per-address
//! provenance. Those become explicit `None`s in the snapshot, and a
//! one-time `Custom { kind: "iroh_api_missing", ... }` event lists the
//! gaps so the post-processor can render them rather than treat
//! missing data as zero.
//!
//! `conn_type` is *derived* from the address-usage view (Direct if any
//! active IP addr exists, Relay if any active relay addr exists, Mixed
//! if both, None otherwise). Heuristic — the post-processor reading
//! the bundle should compare against actual message flow before
//! concluding anything.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::{Endpoint, PublicKey, Watcher};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::diagnostics::event::{ConnType, Event};
use crate::diagnostics::sink::DynEmitter;
use crate::diagnostics::snapshot::{
    IrohIntrospector, MetricSample, MetricValueWire, Tier2ConnectionCache, Tier2IrohState,
    Tier2Peer, TransportAddrWire,
};
use crate::diagnostics::wall_ms_now;
use crate::types::NodeId;

/// Default cadence for the background introspection task. Cheap to
/// dial up or down — the scrape itself just reads in-memory iroh
/// state plus issues short async `remote_info` lookups.
pub const DEFAULT_SCRAPE_INTERVAL: Duration = Duration::from_secs(1);

/// Configuration for [`IrohIntrospect::start`]. Defaults match the
/// constants documented elsewhere in `DIAGNOSTICS_PLAN.md`.
#[derive(Debug, Clone)]
pub struct IntrospectConfig {
    pub scrape_interval: Duration,
}

impl Default for IntrospectConfig {
    fn default() -> Self {
        Self {
            scrape_interval: DEFAULT_SCRAPE_INTERVAL,
        }
    }
}

/// Cached tier-2 state plus the set of peers we know to look up.
///
/// Shared between the polling task and any number of
/// [`IrohIntrospector::capture`] callers. The polling task is the
/// only writer to `state`; readers clone the inner value.
#[derive(Debug)]
struct Shared {
    state: Mutex<Tier2IrohState>,
    peers: Mutex<HashSet<NodeId>>,
    last_conn_types: Mutex<HashMap<NodeId, Option<ConnType>>>,
    last_home_relay: Mutex<Option<String>>,
    cache_tracker: Arc<ConnectionCacheTracker>,
}

/// Per-peer connection-cache lifecycle accounting (`DIAGNOSTICS_PLAN.md`
/// T2.4). The iroh driver records every cache touch through one of
/// the `note_*` methods; the introspector reads
/// [`Self::snapshot`] at scrape time and includes it under
/// [`Tier2IrohState::connection_cache`].
///
/// Cheap: a single `Mutex<HashMap>` keyed by peer. Methods are
/// idempotent and order-independent except for `note_dial_success`,
/// which is the only path that bumps `generation`.
#[derive(Debug, Default)]
pub struct ConnectionCacheTracker {
    entries: Mutex<HashMap<NodeId, CacheEntry>>,
}

#[derive(Debug, Default, Clone)]
struct CacheEntry {
    generation: u64,
    created_at_ms: Option<u64>,
    last_successful_send_at_ms: Option<u64>,
    last_failure_at_ms: Option<u64>,
    last_failure_reason: Option<String>,
}

impl ConnectionCacheTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current generation for a peer. Returns 0 if this is the first
    /// time we have heard of the peer. Used by the iroh driver to
    /// label `ConnectionCacheHit/Miss/Invalidated` events with the
    /// same generation the snapshot reports.
    pub fn generation_for(&self, peer: NodeId) -> u64 {
        self.entries
            .lock()
            .expect("connection cache tracker poisoned")
            .get(&peer)
            .map(|e| e.generation)
            .unwrap_or(0)
    }

    /// Record that a fresh `iroh::Connection` was just inserted into
    /// the driver's cache for this peer. Bumps `generation` and stamps
    /// `created_at_ms`. Returns the new generation.
    pub fn note_dial_success(&self, peer: NodeId, at_ms: u64) -> u64 {
        let mut map = self
            .entries
            .lock()
            .expect("connection cache tracker poisoned");
        let entry = map.entry(peer).or_default();
        entry.generation += 1;
        entry.created_at_ms = Some(at_ms);
        // Clear stale failure context — a fresh connection is its
        // own starting point.
        entry.last_failure_at_ms = None;
        entry.last_failure_reason = None;
        entry.generation
    }

    /// Record a successful send via the cached connection.
    pub fn note_send_success(&self, peer: NodeId, at_ms: u64) {
        let mut map = self
            .entries
            .lock()
            .expect("connection cache tracker poisoned");
        let entry = map.entry(peer).or_default();
        entry.last_successful_send_at_ms = Some(at_ms);
    }

    /// Record a send failure or cache invalidation reason.
    pub fn note_failure(&self, peer: NodeId, at_ms: u64, reason: &str) {
        let mut map = self
            .entries
            .lock()
            .expect("connection cache tracker poisoned");
        let entry = map.entry(peer).or_default();
        entry.last_failure_at_ms = Some(at_ms);
        entry.last_failure_reason = Some(reason.to_string());
    }

    /// Snapshot the per-peer aggregate without any conn_type
    /// enrichment — the introspector fills that in from its tier-2
    /// peer scrape.
    pub fn snapshot(&self) -> Vec<Tier2ConnectionCache> {
        let map = self
            .entries
            .lock()
            .expect("connection cache tracker poisoned");
        let mut out: Vec<Tier2ConnectionCache> = map
            .iter()
            .map(|(peer, entry)| Tier2ConnectionCache {
                peer_node_id_hex: node_id_hex_lower(peer),
                generation: entry.generation,
                created_at_ms: entry.created_at_ms,
                last_successful_send_at_ms: entry.last_successful_send_at_ms,
                last_failure_at_ms: entry.last_failure_at_ms,
                last_failure_reason: entry.last_failure_reason.clone(),
                observed_conn_type_at_last_use: None,
            })
            .collect();
        out.sort_by(|a, b| a.peer_node_id_hex.cmp(&b.peer_node_id_hex));
        out
    }
}

/// The iroh introspector. Owns the polling task and the home-relay
/// watcher task; both abort on drop via the held [`JoinHandle`]s.
pub struct IrohIntrospect {
    shared: Arc<Shared>,
    _scrape_task: AbortOnDrop,
    _relay_task: AbortOnDrop,
}

/// Tiny RAII guard so introspector drop also aborts the spawned
/// background tasks. Without this a long-lived runtime would keep
/// dead introspectors alive forever.
struct AbortOnDrop(Option<JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

impl IrohIntrospect {
    /// Build an introspector and spin up its polling + home-relay
    /// tasks. Both run on `runtime` so the caller doesn't need a
    /// blocking runtime to read snapshots later.
    ///
    /// `emitter` receives the one-time `iroh_api_missing` Custom
    /// event plus every `RelayChanged` and `IrohConnTypeChanged`
    /// transition.
    pub fn start(
        endpoint: Endpoint,
        runtime: Handle,
        emitter: DynEmitter,
        config: IntrospectConfig,
        cache_tracker: Arc<ConnectionCacheTracker>,
    ) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(Tier2IrohState {
                api_gaps: api_gaps(),
                ..Tier2IrohState::default()
            }),
            peers: Mutex::new(HashSet::new()),
            last_conn_types: Mutex::new(HashMap::new()),
            last_home_relay: Mutex::new(None),
            cache_tracker,
        });

        emitter.emit_event(Event::Custom {
            kind: "iroh_api_missing".into(),
            fields: serde_json::json!({
                "iroh_version": "0.96",
                "fields": api_gaps(),
                "note": "iroh 0.96 RemoteInfo exposes id + addrs only; \
                         conn_type derived heuristically from address usage",
            }),
        });

        let scrape_task = spawn_scrape_task(
            endpoint.clone(),
            runtime.clone(),
            Arc::clone(&shared),
            emitter.clone(),
            config.scrape_interval,
        );
        let relay_task = spawn_relay_watcher(
            endpoint,
            runtime.clone(),
            Arc::clone(&shared),
            emitter,
        );

        Self {
            shared,
            _scrape_task: AbortOnDrop(Some(scrape_task)),
            _relay_task: AbortOnDrop(Some(relay_task)),
        }
    }

    /// Tell the introspector about a peer we want covered in the
    /// tier-2 snapshot. Idempotent.
    pub fn register_peer(&self, node_id: NodeId) {
        self.shared
            .peers
            .lock()
            .expect("iroh introspect peers poisoned")
            .insert(node_id);
    }

    /// For tests: force a synchronous scrape right now rather than
    /// waiting for the polling tick. Useful so a test asserting on
    /// snapshot contents doesn't have to sleep for `scrape_interval`.
    /// Not part of normal operation.
    pub fn force_refresh_blocking(&self, endpoint: &Endpoint, runtime: &Handle) {
        let peers = self
            .shared
            .peers
            .lock()
            .expect("iroh introspect peers poisoned")
            .clone();
        let metrics = scrape_metrics(endpoint);
        let home = home_relay_str(endpoint);
        let peers_state = runtime.block_on(collect_peer_states(endpoint, peers.iter().copied()));
        let connection_cache = build_cache_snapshot(&self.shared.cache_tracker, &peers_state);
        let mut state = self.shared.state.lock().expect("iroh introspect state poisoned");
        state.home_relay_url = home;
        state.peers = peers_state;
        state.metrics = metrics;
        state.connection_cache = connection_cache;
        state.scraped_at_ms = wall_ms_now();
    }
}

impl IrohIntrospector for IrohIntrospect {
    fn capture(&self) -> Tier2IrohState {
        self.shared
            .state
            .lock()
            .expect("iroh introspect state poisoned")
            .clone()
    }
}

fn spawn_scrape_task(
    endpoint: Endpoint,
    runtime: Handle,
    shared: Arc<Shared>,
    emitter: DynEmitter,
    interval: Duration,
) -> JoinHandle<()> {
    runtime.spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let peers: Vec<NodeId> = {
                let guard = shared.peers.lock().expect("iroh introspect peers poisoned");
                guard.iter().copied().collect()
            };
            let peers_state = collect_peer_states(&endpoint, peers.iter().copied()).await;
            let metrics = scrape_metrics(&endpoint);
            let home = home_relay_str(&endpoint);

            // Diff conn types and emit IrohConnTypeChanged.
            {
                let mut last = shared
                    .last_conn_types
                    .lock()
                    .expect("iroh introspect last_conn_types poisoned");
                for p in &peers_state {
                    let node_id = match parse_node_id_hex(&p.peer_node_id_hex) {
                        Some(n) => n,
                        None => continue,
                    };
                    let new = p.conn_type;
                    let prev_known = last.contains_key(&node_id);
                    let prev = last.insert(node_id, new).flatten();
                    let new_val = new;
                    // Emit only on transitions and only after we've
                    // seen at least one prior scrape — the very first
                    // observation is not a transition.
                    if prev_known && prev != new_val {
                        emitter.emit_event(Event::IrohConnTypeChanged {
                            peer: node_id,
                            old: prev.unwrap_or(ConnType::None),
                            new: new_val.unwrap_or(ConnType::None),
                        });
                    }
                }
            }

            let connection_cache =
                build_cache_snapshot(&shared.cache_tracker, &peers_state);
            let mut state = shared
                .state
                .lock()
                .expect("iroh introspect state poisoned");
            state.home_relay_url = home;
            state.peers = peers_state;
            state.metrics = metrics;
            state.connection_cache = connection_cache;
            state.scraped_at_ms = wall_ms_now();
        }
    })
}

/// Merge the cache tracker's bookkeeping with the latest tier-2 peer
/// scrape so each cache entry carries `observed_conn_type_at_last_use`
/// from iroh's current view. Iroh-side conn_type is missing for peers
/// iroh has not heard of, in which case the field stays `None`.
fn build_cache_snapshot(
    tracker: &ConnectionCacheTracker,
    peers_state: &[Tier2Peer],
) -> Vec<Tier2ConnectionCache> {
    let mut conn_types: HashMap<&str, Option<ConnType>> = HashMap::new();
    for p in peers_state {
        conn_types.insert(p.peer_node_id_hex.as_str(), p.conn_type);
    }
    let mut snap = tracker.snapshot();
    for entry in &mut snap {
        if let Some(ct) = conn_types.get(entry.peer_node_id_hex.as_str()) {
            entry.observed_conn_type_at_last_use = *ct;
        }
    }
    snap
}

fn spawn_relay_watcher(
    endpoint: Endpoint,
    runtime: Handle,
    shared: Arc<Shared>,
    emitter: DynEmitter,
) -> JoinHandle<()> {
    runtime.spawn(async move {
        let mut watcher = endpoint.watch_addr();
        loop {
            let addr = watcher.get();
            let new_url = addr.relay_urls().next().map(|u| u.to_string());
            let old = {
                let mut slot = shared
                    .last_home_relay
                    .lock()
                    .expect("iroh introspect last_home_relay poisoned");
                let prev = slot.clone();
                if prev != new_url {
                    *slot = new_url.clone();
                    Some(prev)
                } else {
                    None
                }
            };
            if let Some(prev) = old {
                // Suppress the very first "no relay yet → no relay
                // yet" transition; only emit when something actually
                // changed.
                emitter.emit_event(Event::RelayChanged {
                    old_url: prev,
                    new_url: new_url.clone(),
                });
            }
            if watcher.updated().await.is_err() {
                break;
            }
        }
    })
}

async fn collect_peer_states(
    endpoint: &Endpoint,
    peers: impl IntoIterator<Item = NodeId>,
) -> Vec<Tier2Peer> {
    let mut out = Vec::new();
    for peer in peers {
        let hex = node_id_hex_lower(&peer);
        let public_key = match PublicKey::from_bytes(&peer.0) {
            Ok(k) => k,
            Err(_) => {
                out.push(empty_peer(hex));
                continue;
            }
        };
        match endpoint.remote_info(public_key).await {
            Some(info) => out.push(remote_info_to_wire(hex, info)),
            None => out.push(empty_peer(hex)),
        }
    }
    out.sort_by(|a, b| a.peer_node_id_hex.cmp(&b.peer_node_id_hex));
    out
}

fn remote_info_to_wire(hex: String, info: iroh::endpoint::RemoteInfo) -> Tier2Peer {
    let mut direct = Vec::new();
    let mut relays = Vec::new();
    let mut active_direct = false;
    let mut active_relay = false;
    for addr_info in info.addrs() {
        let usage = format!("{:?}", addr_info.usage()).to_lowercase();
        let is_active = usage == "active";
        match addr_info.addr() {
            iroh::TransportAddr::Ip(sa) => {
                if is_active {
                    active_direct = true;
                }
                direct.push(TransportAddrWire {
                    addr: sa.to_string(),
                    usage: usage.clone(),
                });
            }
            iroh::TransportAddr::Relay(url) => {
                if is_active {
                    active_relay = true;
                }
                relays.push(TransportAddrWire {
                    addr: url.to_string(),
                    usage: usage.clone(),
                });
            }
            _ => {}
        }
    }
    let conn_type = match (active_direct, active_relay) {
        (true, true) => Some(ConnType::Mixed),
        (true, false) => Some(ConnType::Direct),
        (false, true) => Some(ConnType::Relay),
        (false, false) => {
            // We've heard of the peer but no addr is in active use.
            Some(ConnType::None)
        }
    };
    Tier2Peer {
        peer_node_id_hex: hex,
        conn_type,
        latency_ms: None,
        last_used_ms: None,
        last_received_ms: None,
        direct_addresses: direct,
        relay_urls: relays,
        addr_sources: None,
    }
}

fn empty_peer(hex: String) -> Tier2Peer {
    Tier2Peer {
        peer_node_id_hex: hex,
        conn_type: None,
        latency_ms: None,
        last_used_ms: None,
        last_received_ms: None,
        direct_addresses: Vec::new(),
        relay_urls: Vec::new(),
        addr_sources: None,
    }
}

fn scrape_metrics(endpoint: &Endpoint) -> Vec<MetricSample> {
    use iroh_metrics::{MetricValue, MetricsGroupSet};
    #[allow(unused_imports)]
    use iroh_metrics::MetricsGroup as _;
    let mut out = Vec::new();
    for (group, item) in endpoint.metrics().iter() {
        let value = match item.value() {
            MetricValue::Counter(v) => MetricValueWire::Counter(v),
            MetricValue::Gauge(v) => MetricValueWire::Gauge(v),
            MetricValue::Histogram { count, sum, .. } => {
                MetricValueWire::Histogram { count, sum }
            }
            _ => continue,
        };
        out.push(MetricSample {
            group: group.to_string(),
            name: item.name().to_string(),
            value,
        });
    }
    out
}

fn home_relay_str(endpoint: &Endpoint) -> Option<String> {
    endpoint.addr().relay_urls().next().map(|u| u.to_string())
}

fn api_gaps() -> Vec<String> {
    vec![
        "RemoteInfo.conn_type".into(),
        "RemoteInfo.latency_ms".into(),
        "RemoteInfo.last_used_ms".into(),
        "RemoteInfo.last_received_ms".into(),
        "TransportAddrInfo.source".into(),
    ]
}

fn node_id_hex_lower(id: &NodeId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in id.0 {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

fn parse_node_id_hex(hex: &str) -> Option<NodeId> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = hex_nibble(pair[0])?;
        let lo = hex_nibble(pair[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(NodeId(out))
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

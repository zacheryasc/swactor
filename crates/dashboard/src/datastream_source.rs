//! Datastream → dashboard adapter.
//!
//! Binds the UDP sink the demo cluster ships to (the same wire the dumb
//! `swactor-datastream-collector` reads), **demultiplexes** the per-node frames,
//! and drives the dashboard's *existing* views from them — no bespoke UI:
//!
//!   * the single-node **Overview / Actors** page (`/`) via a synthesized
//!     [`RuntimeStats`] (each datastream channel becomes one synthetic actor row);
//!   * the canonical **Distribution** connection-graph page
//!     (`/plugin/distribution`, [`crate::DISTRIBUTION_PAGE_HTML`]) via a
//!     [`DistributionNodeSnapshot`] rebuilt from the selected node's membership —
//!     so it renders the exact SWIM graph a live node shows;
//!   * a cross-node **Fleet** table (`/plugin/vastai`) served in the same
//!     dashboard chrome (nav bar + palette), not a separate app.
//!
//! Process output and membership transitions are emitted as `tracing` events so
//! they flow through the dashboard's activity-log path.
//!
//! A node is only shown while it is *live* (has shipped a frame within
//! [`NODE_TTL`]); a node that stops streaming drops out of every view, so a
//! restarted/departed node leaves no ghost in the graph or the fleet table.

use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distribution::datastream::catalog::{
    self, IdentityRecord, LifecycleCost, MembershipTransition, Record, ResourceSample,
    RuntimeStats as DsRuntimeStats, TransportInternals,
};
use distribution::datastream::wire::decode_delivery;
use distribution::snapshot::{DistributionNodeSnapshot, MemberInfo};

use swactor::actor::ActorAddress;
use swactor::stats::{ActorInfo, RuntimeStats, TickTiming, WorkerInfo};

use crate::plugin::{DashboardPlugin, PluginResponse};
use crate::{DashboardHandle, DISTRIBUTION_PAGE_HTML};

/// A node counts as live — shown in the graph and fleet table — if it has
/// streamed a frame within this window. Nodes ship resource/runtime/transport
/// samples every ~1s, so a node silent past this has left the cluster; its
/// `models` entry lingers but is filtered out of every view (no ghost nodes).
const NODE_TTL: Duration = Duration::from_secs(8);

/// Per-node accumulator: the latest value seen on each typed channel plus
/// running membership/process state. One of these drives the display.
#[derive(Default)]
struct DatastreamModel {
    identity: Option<IdentityRecord>,
    resource: Option<ResourceSample>,
    runtime: Option<DsRuntimeStats>,
    transport: Option<TransportInternals>,
    lifecycle: Option<LifecycleCost>,
    /// peer node-id → latest liveness state.
    membership: HashMap<String, String>,
    /// last membership transition, formatted for display.
    last_transition: Option<String>,
    /// proc label → (line count, last line).
    procs: HashMap<String, (u64, String)>,
    first_seen: Option<Instant>,
    last_seen: Option<Instant>,
}

/// A log line to surface through the dashboard's activity path.
enum LogEvent {
    Info(String),
    Warn(String),
}

impl DatastreamModel {
    /// Fold one frame's channel/payload into the model, returning any activity
    /// log events it produced (process output, membership transitions).
    fn update(&mut self, channel: &str, payload: &[u8]) -> Vec<LogEvent> {
        let now = Instant::now();
        self.first_seen.get_or_insert(now);
        self.last_seen = Some(now);
        let mut events = Vec::new();

        match channel {
            catalog::IDENTITY => {
                if let Ok(r) = IdentityRecord::decode(payload) {
                    self.identity = Some(r);
                }
            }
            catalog::HOST_RESOURCE => {
                if let Ok(r) = ResourceSample::decode(payload) {
                    self.resource = Some(r);
                }
            }
            catalog::RUNTIME_STATS => {
                if let Ok(r) = DsRuntimeStats::decode(payload) {
                    self.runtime = Some(r);
                }
            }
            catalog::TRANSPORT_INTERNALS => {
                if let Ok(r) = TransportInternals::decode(payload) {
                    self.transport = Some(r);
                }
            }
            catalog::LIFECYCLE_COST => {
                if let Ok(r) = LifecycleCost::decode(payload) {
                    self.lifecycle = Some(r);
                }
            }
            catalog::MEMBERSHIP => {
                if let Ok(t) = MembershipTransition::decode(payload) {
                    // Display the short id; key the membership map by the full
                    // id so the views can resolve it to a friendly label.
                    let line = format!("{}: {} → {}", short_id(&t.peer), t.from, t.to);
                    self.membership.insert(t.peer.clone(), t.to.clone());
                    self.last_transition = Some(line.clone());
                    events.push(LogEvent::Info(format!("membership {line}")));
                }
            }
            // Raw-text process output: `proc.<label>.{stdout,stderr}`.
            _ if channel.starts_with("proc.") => {
                let line = String::from_utf8_lossy(payload).into_owned();
                if let Some((label, is_err)) = parse_proc_channel(channel) {
                    let entry = self.procs.entry(label.to_string()).or_default();
                    entry.0 += 1;
                    entry.1 = line.clone();
                    let tagged = format!("[{label}] {line}");
                    events.push(if is_err {
                        LogEvent::Warn(tagged)
                    } else {
                        LogEvent::Info(tagged)
                    });
                }
            }
            // Unknown channel — ignored for display (still demuxed cleanly).
            _ => {}
        }
        events
    }

    /// Has this node streamed a frame within [`NODE_TTL`]?
    fn is_live(&self, now: Instant) -> bool {
        self.last_seen
            .map(|t| now.duration_since(t) < NODE_TTL)
            .unwrap_or(false)
    }

    /// A friendly label for this node: `region · short-id` once its identity is
    /// known, else just the short id.
    fn label(&self, node_id: &str) -> String {
        match &self.identity {
            Some(idr) if !idr.region.is_empty() => {
                format!("{} · {}", idr.region, short_id(node_id))
            }
            _ => short_id(node_id),
        }
    }

    /// Total process-output lines seen across all labels.
    fn total_proc_lines(&self) -> u64 {
        self.procs.values().map(|(n, _)| *n).sum()
    }

    /// The peers this node currently sees that are themselves still live, as
    /// `(peer_id, state)`. Filtering by liveness drops stale incarnations a
    /// lost SWIM `dead` transition would otherwise leave stuck at `alive`.
    fn live_peers<'a>(&'a self, live: &HashSet<String>) -> Vec<(&'a String, &'a String)> {
        self.membership
            .iter()
            .filter(|(peer, _)| live.contains(peer.as_str()))
            .collect()
    }

    /// Synthesize the dashboard's native stats from the accumulated datastream.
    fn to_runtime_stats(&self) -> RuntimeStats {
        let ds_rt = self.runtime.clone().unwrap_or(DsRuntimeStats {
            actors_live: 0,
            mailbox_depth: 0,
            scheduled_tasks: 0,
        });

        // One synthetic worker = this node.
        let worker = WorkerInfo {
            id: 0,
            num_actors: ds_rt.actors_live as usize,
            mailbox_depth: ds_rt.mailbox_depth as usize,
            messages_processed: self.total_proc_lines(),
            local_sends: 0,
            cross_sends: 0,
            inbox_sends: 0,
            type_mismatches: 0,
            panics: 0,
            messages_dropped: 0,
            restarts: 0,
            stops: 0,
        };

        // Each datastream channel becomes one synthetic actor row, with columns
        // and the per-actor breakdown chart repurposed to show its values.
        let mut actor_details: Vec<ActorInfo> = Vec::new();

        if let Some(r) = &self.resource {
            actor_details.push(synth_actor(
                "host.resource",
                Some(format!(
                    "cpu {:.0}% mem {}/{}MB",
                    r.cpu_pct, r.mem_used_mb, r.mem_total_mb
                )),
                self.total_proc_lines(),
                vec![
                    ("cpu_pct".to_string(), r.cpu_pct.round() as u64),
                    ("mem_used_mb".to_string(), r.mem_used_mb as u64),
                    ("mem_total_mb".to_string(), r.mem_total_mb as u64),
                    ("gpu_pct".to_string(), r.gpu_pct.round() as u64),
                    ("disk_used_gb".to_string(), r.disk_used_gb as u64),
                    ("net_rx_kbps".to_string(), r.net_rx_kbps as u64),
                    ("net_tx_kbps".to_string(), r.net_tx_kbps as u64),
                ],
            ));
        }

        if let Some(t) = &self.transport {
            actor_details.push(synth_actor(
                "transport.internals",
                Some(format!(
                    "relay {} · {} direct · {} relayed",
                    if t.relay_connected { "up" } else { "down" },
                    t.direct_peers,
                    t.relay_peers
                )),
                0,
                vec![
                    ("relay_connected".to_string(), t.relay_connected as u64),
                    ("direct_peers".to_string(), t.direct_peers as u64),
                    ("relay_peers".to_string(), t.relay_peers as u64),
                    ("rtt_ms_p50".to_string(), t.rtt_ms_p50 as u64),
                ],
            ));
        }

        if !self.membership.is_empty() || self.last_transition.is_some() {
            let mut counts: HashMap<&str, u64> = HashMap::new();
            for state in self.membership.values() {
                *counts.entry(state.as_str()).or_default() += 1;
            }
            let breakdown = counts
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            actor_details.push(synth_actor(
                "membership",
                self.last_transition.clone(),
                self.membership.len() as u64,
                breakdown,
            ));
        }

        // One row per process label, newest line as "last message".
        let mut procs: Vec<(&String, &(u64, String))> = self.procs.iter().collect();
        procs.sort_by(|a, b| a.0.cmp(b.0));
        for (label, (count, last)) in procs {
            actor_details.push(synth_actor(
                &format!("proc.{label}"),
                Some(last.clone()),
                *count,
                vec![("lines".to_string(), *count)],
            ));
        }

        let actors = actor_details
            .iter()
            .map(|a| (a.address, a.worker_id))
            .collect();

        let uptime_ms = self
            .lifecycle
            .as_ref()
            .map(|l| l.uptime_s * 1000)
            .unwrap_or_else(|| {
                self.first_seen
                    .map(|t| t.elapsed().as_millis() as u64)
                    .unwrap_or(0)
            });

        RuntimeStats {
            num_workers: 1,
            uptime_ms,
            actors,
            workers: vec![worker],
            actor_details,
            // No per-tick phase timing on the wire; one empty worker entry keeps
            // the phase bars degrading to a single fill rather than panicking.
            tick_timings: vec![Vec::<TickTiming>::new()],
        }
    }

    /// Does this node match the optional selection filter? Matches against the
    /// node id and (once known) the identity region/role.
    fn matches(&self, node_id: &str, filter: &str) -> bool {
        if node_id.contains(filter) {
            return true;
        }
        match &self.identity {
            Some(id) => {
                id.region == filter || format!("{:?}", id.role).eq_ignore_ascii_case(filter)
            }
            None => false,
        }
    }

    /// Rebuild a [`DistributionNodeSnapshot`] for this node from the demuxed
    /// stream, so the canonical Distribution page renders its SWIM connection
    /// graph exactly as it would for a live node. Only live peers are included
    /// (`peer_labels`/`live` are the fleet-wide label map and live set).
    ///
    /// Fields the datastream does not carry (routing table, cache, directory,
    /// registry) are honest zeros — the graph and membership panel, which is all
    /// this view exists to show, are driven entirely by `members`.
    fn dist_snapshot(
        &self,
        node_id: &str,
        peer_labels: &HashMap<String, String>,
        live: &HashSet<String>,
    ) -> DistributionNodeSnapshot {
        let mut members: Vec<MemberInfo> = self
            .live_peers(live)
            .into_iter()
            .map(|(peer, state)| MemberInfo {
                node_id: peer.clone(),
                addr: None,
                state: state.clone(),
                incarnation: 0,
                is_authorized: None,
                label: None,
                relay_url: None,
                node_name: Some(peer_labels.get(peer).cloned().unwrap_or_else(|| short_id(peer))),
            })
            .collect();
        members.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        let count = |want: &str| members.iter().filter(|m| m.state == want).count();
        let (alive_count, suspect_count, dead_count) =
            (count("alive"), count("suspect"), count("dead"));

        DistributionNodeSnapshot {
            node_id: node_id.to_string(),
            listen_addr: None,
            members,
            alive_count,
            suspect_count,
            dead_count,
            routing_table_size: 0,
            routing_buckets: Vec::new(),
            routing_neighbors: Vec::new(),
            cache_size: 0,
            cache_entries: Vec::new(),
            directory_entry_count: 0,
            repair_queue_size: 0,
            registry_size: 0,
            registry_tombstones: 0,
            registry_entries: Vec::new(),
            recent_probe_targets: Vec::new(),
            peer_auth_mode: "open".into(),
            authorized_peer_count: None,
            node_name: Some(peer_labels.get(node_id).cloned().unwrap_or_else(|| short_id(node_id))),
            invite_code: None,
            // `relay_url` means "this node runs an embedded relay server" (it
            // draws a teal ring in the graph) — not "is connected to a relay".
            // The datastream doesn't carry that, so leave it unset.
            relay_url: None,
            version: None,
            join_statuses: Vec::new(),
        }
    }

    /// A compact per-node summary row for the fleet table.
    fn node_summary(
        &self,
        node_id: &str,
        expected_peers: usize,
        selected: bool,
        live: &HashSet<String>,
    ) -> serde_json::Value {
        let peers = self.live_peers(live);
        let count = |want: &str| peers.iter().filter(|(_, s)| s.as_str() == want).count() as u32;
        let (alive, suspect, dead) = (count("alive"), count("suspect"), count("dead"));
        let seen = peers.len();
        // "Converged" from this node's vantage: it sees every other live node,
        // all alive (no suspect/dead).
        let converged = suspect == 0 && dead == 0 && seen >= expected_peers && expected_peers > 0;

        let r = self.resource.as_ref();
        let t = self.transport.as_ref();
        let rt = self.runtime.as_ref();
        let id = self.identity.as_ref();
        let last_proc = self
            .procs
            .values()
            .map(|(_, line)| line.clone())
            .last()
            .unwrap_or_default();

        serde_json::json!({
            "id": node_id,
            "short": short_id(node_id),
            "region": id.map(|i| i.region.clone()).unwrap_or_default(),
            "role": id.map(|i| format!("{:?}", i.role).to_lowercase()).unwrap_or_default(),
            "selected": selected,
            "cpu_pct": r.map(|r| r.cpu_pct.round() as u32).unwrap_or(0),
            "mem_used_mb": r.map(|r| r.mem_used_mb).unwrap_or(0),
            "mem_total_mb": r.map(|r| r.mem_total_mb).unwrap_or(0),
            "gpu_pct": r.map(|r| r.gpu_pct.round() as u32).unwrap_or(0),
            "actors_live": rt.map(|r| r.actors_live).unwrap_or(0),
            "mailbox_depth": rt.map(|r| r.mailbox_depth).unwrap_or(0),
            "relay_connected": t.map(|t| t.relay_connected).unwrap_or(false),
            "direct_peers": t.map(|t| t.direct_peers).unwrap_or(0),
            "relay_peers": t.map(|t| t.relay_peers).unwrap_or(0),
            "rtt_ms_p50": t.map(|t| t.rtt_ms_p50).unwrap_or(0),
            "alive": alive,
            "suspect": suspect,
            "dead": dead,
            "converged": converged,
            "proc_lines": self.total_proc_lines(),
            "last_proc": last_proc,
        })
    }
}

/// `node-id → "region · short-id"` for every demuxed node.
fn build_labels(models: &HashMap<String, DatastreamModel>) -> HashMap<String, String> {
    models.iter().map(|(id, m)| (id.clone(), m.label(id))).collect()
}

/// The set of node ids currently live (streamed within [`NODE_TTL`]).
fn live_set(models: &HashMap<String, DatastreamModel>, now: Instant) -> HashSet<String> {
    models
        .iter()
        .filter(|(_, m)| m.is_live(now))
        .map(|(id, _)| id.clone())
        .collect()
}

/// Build the Fleet-table JSON model from every *live* demuxed node.
fn fleet_json(
    models: &HashMap<String, DatastreamModel>,
    selected: Option<&str>,
    live: &HashSet<String>,
) -> String {
    let node_count = live.len();
    let expected_peers = node_count.saturating_sub(1);
    let mut nodes: Vec<serde_json::Value> = models
        .iter()
        .filter(|(id, _)| live.contains(id.as_str()))
        .map(|(id, m)| m.node_summary(id, expected_peers, selected == Some(id.as_str()), live))
        .collect();
    // Stable order: region then short id, so rows don't jump around.
    nodes.sort_by(|a, b| {
        (a["region"].as_str(), a["short"].as_str())
            .cmp(&(b["region"].as_str(), b["short"].as_str()))
    });
    // Whole fleet converged once every node has converged and there's >1 node.
    let converged =
        node_count > 1 && nodes.iter().all(|n| n["converged"].as_bool() == Some(true));

    serde_json::json!({
        "node_count": node_count,
        "converged": converged,
        "nodes": nodes,
    })
    .to_string()
}

/// Plugin backed by a shared cache string: serves a fixed HTML page, emits its
/// cache on the SSE stream under `name`, and answers `GET /api/plugin/{name}`.
/// Used for both the Fleet table (`vastai`) and the Distribution graph
/// (`distribution`); each is fed by [`run_datastream_ingest`].
struct CachePlugin {
    name: &'static str,
    page: &'static str,
    cache: Arc<Mutex<Option<String>>>,
}

impl CachePlugin {
    fn new(name: &'static str, page: &'static str, cache: Arc<Mutex<Option<String>>>) -> Self {
        Self { name, page, cache }
    }
}

impl DashboardPlugin for CachePlugin {
    fn name(&self) -> &str {
        self.name
    }

    fn snapshot_json(&self) -> Option<String> {
        self.cache.lock().unwrap().clone()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "" | "model" | "snapshot") => {
                PluginResponse::json(self.cache.lock().unwrap().clone().unwrap_or_else(|| "{}".into()))
            }
            // The Distribution page has Re-peer / dismiss buttons; in this
            // read-only datastream view they are inert (acknowledged, no-op).
            ("POST", "rejoin" | "clear_status") => PluginResponse::json(r#"{"ok":true}"#.into()),
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(self.page)
    }
}

/// Minimal `peers` plugin so the Distribution page's `fetch('/api/plugin/peers')`
/// resolves cleanly (open auth, no managed peer list) instead of 404ing. Silent
/// on the SSE stream.
struct PeersStub;

impl DashboardPlugin for PeersStub {
    fn name(&self) -> &str {
        "peers"
    }
    fn snapshot_json(&self) -> Option<String> {
        None
    }
    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "" | "list") => PluginResponse::json(r#"{"mode":"open","peers":[]}"#.into()),
            _ => PluginResponse::not_found(),
        }
    }
}

/// Build one synthetic actor row with a deterministic address from its name.
///
/// `mailbox_depth` is always 0: a datastream channel is not a real mailbox, and
/// the dashboard's WarningDetector flags any row with `depth > 0` whose
/// `messages_processed` is flat as a "stalled actor". The channel's real values
/// live in `last_msg_type` and the `message_type_counts` breakdown instead.
fn synth_actor(
    name: &str,
    last_msg_type: Option<String>,
    messages_processed: u64,
    message_type_counts: Vec<(String, u64)>,
) -> ActorInfo {
    ActorInfo {
        address: addr_from(name),
        worker_id: 0,
        mailbox_depth: 0,
        last_msg_type,
        messages_processed,
        poisoned: false,
        name: Some(name.to_string()),
        message_type_counts,
    }
}

/// Deterministic 32-byte address from a channel name (FNV-1a in the first 8
/// bytes — which is what the address Hash/Display use — plus the name splatted
/// after for readability). Stable across frames so sparklines accumulate.
fn addr_from(name: &str) -> ActorAddress {
    let mut bytes = [0u8; 32];
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    bytes[..8].copy_from_slice(&hash.to_le_bytes());
    for (i, b) in name.bytes().take(24).enumerate() {
        bytes[8 + i] = b;
    }
    ActorAddress(bytes)
}

/// Split `proc.<label>.stdout` / `proc.<label>.stderr` into `(label, is_stderr)`.
fn parse_proc_channel(channel: &str) -> Option<(&str, bool)> {
    let rest = channel.strip_prefix("proc.")?;
    if let Some(label) = rest.strip_suffix(".stderr") {
        Some((label, true))
    } else {
        rest.strip_suffix(".stdout").map(|label| (label, false))
    }
}

/// First 8 chars of an id — the readable short form used throughout the views.
fn short_id(id: &str) -> String {
    id[..id.len().min(8)].to_string()
}

/// Bind the UDP datastream sink, demux frames, and drive the dashboard's views.
/// Blocks forever, like the dumb collector's `main`.
///
/// Registers three plugins fed off the demuxed stream:
///   * `distribution` — the canonical SWIM connection-graph page, snapshotting
///     the selected node;
///   * `vastai` (the nav's "Fleet") — the cross-node telemetry table;
///   * `peers` — an open-auth stub so the graph page's peer fetch resolves.
///
/// The single-node Overview/Actors page is driven via `handle.set_stats`.
pub fn run_datastream_ingest(
    bind: &str,
    node_filter: Option<&str>,
    handle: &DashboardHandle,
) -> std::io::Result<()> {
    let sock = UdpSocket::bind(bind)?;
    eprintln!("datastream dashboard: listening on {bind} (one frame per datagram)");

    // Distribution graph (selected node) and Fleet table (all live nodes), each
    // a shared cache the ingest loop refreshes and the SSE loop fans out.
    let dist_cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let fleet_cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    handle.register_plugin(Arc::new(CachePlugin::new(
        "distribution",
        DISTRIBUTION_PAGE_HTML,
        Arc::clone(&dist_cache),
    )) as Arc<dyn DashboardPlugin>);
    handle.register_plugin(Arc::new(CachePlugin::new(
        "vastai",
        FLEET_HTML,
        Arc::clone(&fleet_cache),
    )) as Arc<dyn DashboardPlugin>);
    // The base dashboard nav has a Datastore link; the datastream carries no
    // datastore, so serve an honest "not available" page in-chrome rather than
    // 404ing. Empty cache → silent on the SSE stream.
    handle.register_plugin(Arc::new(CachePlugin::new(
        "datastore",
        DATASTORE_HTML,
        Arc::new(Mutex::new(None)),
    )) as Arc<dyn DashboardPlugin>);
    handle.register_plugin(Arc::new(PeersStub) as Arc<dyn DashboardPlugin>);

    let mut models: HashMap<String, DatastreamModel> = HashMap::new();
    let mut selected: Option<String> = None;
    // 64 KiB comfortably exceeds a UDP datagram; a frame never spans datagrams.
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let (n, _src) = match sock.recv_from(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("datastream dashboard: recv error: {e}");
                continue;
            }
        };
        let (stream, frame) = match decode_delivery(&buf[..n]) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("datastream dashboard: dropped malformed datagram ({n} B): {e:?}");
                continue;
            }
        };

        let node = stream.node.as_str().to_string();
        let model = models.entry(node.clone()).or_default();
        let events = model.update(frame.channel.as_str(), &frame.payload);

        // Pick the display node: first matching the filter, else first seen.
        if selected.is_none() {
            let qualifies = match node_filter {
                Some(f) => model.matches(&node, f),
                None => true,
            };
            if qualifies {
                eprintln!("datastream dashboard: displaying node {}", short_id(&node));
                selected = Some(node.clone());
            }
        }

        if selected.as_deref() == Some(node.as_str()) {
            // Surface process output / membership through the activity log.
            for ev in events {
                match ev {
                    LogEvent::Info(m) => tracing::info!(target: "datastream", "{m}"),
                    LogEvent::Warn(m) => tracing::warn!(target: "datastream", "{m}"),
                }
            }
            handle.set_stats(model.to_runtime_stats());
        }

        // Refresh both views from the current live nodes on each frame.
        let now = Instant::now();
        let live = live_set(&models, now);
        let labels = build_labels(&models);
        *fleet_cache.lock().unwrap() = Some(fleet_json(&models, selected.as_deref(), &live));
        if let Some(sel) = selected.as_deref() {
            if let Some(m) = models.get(sel) {
                let snap = m.dist_snapshot(sel, &labels, &live);
                if let Ok(json) = serde_json::to_string(&snap) {
                    *dist_cache.lock().unwrap() = Some(json);
                }
            }
        }
    }
}

/// Cross-node **Fleet** table, served in the dashboard's own chrome (the same
/// header / nav bar / palette as the Overview and Distribution pages, so it is a
/// section of the one app — not a separate UI). Renders the `vastai` SSE event
/// built by [`fleet_json`]: one row per live node with its resource / runtime /
/// transport telemetry and a fleet-wide SWIM-convergence pill.
const FLEET_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime – Fleet</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }
  .header { display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e; }
  .header-left { display: flex; align-items: center; }
  .header h1 { font-size: 16px; font-weight: 600; color: #fff; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; background: #4caf50;
    display: inline-block; margin-left: 8px; vertical-align: middle; }
  .status-dot.disconnected { background: #f44336; }
  .status-dot.done { background: #ff9800; }
  .nav-links { display: flex; gap: 4px; margin-left: 20px; }
  .nav-link { color: #888; text-decoration: none; font-size: 12px;
    padding: 4px 10px; border-radius: 3px; transition: color 0.2s; }
  .nav-link:hover { color: #e0e0e0; }
  .nav-link.active { color: #fff; background: #2a2d3e; }
  .header-right { display: flex; align-items: center; gap: 12px; }
  .pill { padding: 3px 12px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .pill.ok { background: #14361f; color: #4ade80; }
  .pill.warn { background: #3a2d12; color: #fbbf24; }
  .content { padding: 16px 20px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 7px 12px; border-bottom: 1px solid #1f2230; white-space: nowrap; }
  th { color: #888; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: .5px; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  tr.sel td { background: #161e2e; }
  .tag { padding: 1px 8px; border-radius: 4px; background: #1c1f2e; color: #888; font-size: 11px; }
  .tag.coordinator { background: #1c2d4a; color: #79c0ff; }
  .tag.worker { background: #1a2e2e; color: #56d4dd; }
  .bar { display: inline-block; width: 64px; height: 7px; background: #1c1f2e; border-radius: 4px;
    overflow: hidden; vertical-align: middle; margin-right: 6px; }
  .bar > i { display: block; height: 100%; background: #4ade80; }
  .bar > i.hi { background: #fbbf24; } .bar > i.crit { background: #f87171; }
  .st { padding: 1px 6px; border-radius: 4px; font-weight: 600; font-size: 11px; }
  .st.suspect { background: #3a2d12; color: #fbbf24; } .st.dead { background: #3a1518; color: #f87171; }
  .muted { color: #666; }
  .last { max-width: 380px; overflow: hidden; text-overflow: ellipsis; color: #999; }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <h1>Swactor Runtime Dashboard <span id="statusDot" class="status-dot disconnected"></span></h1>
    <nav class="nav-links">
      <a href="/" class="nav-link">Overview</a>
      <a href="/actors" class="nav-link">Actors</a>
      <a href="/plugin/distribution" class="nav-link">Distribution</a>
      <a href="/plugin/datastore" class="nav-link">Datastore</a>
      <a href="/plugin/vastai" class="nav-link active">Fleet</a>
    </nav>
  </div>
  <div class="header-right">
    <span id="conv" class="pill warn">SWIM: …</span>
  </div>
</div>
<div class="content">
  <table>
    <thead><tr>
      <th>node</th><th>region</th><th>role</th><th>CPU</th><th>mem</th>
      <th class="num">actors</th><th class="num">mbox</th><th>transport</th>
      <th class="num">peers</th><th class="num">proc</th><th>last line</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <p id="empty" class="muted" style="margin-top:12px;">waiting for telemetry…</p>
</div>
<script>
(function () {
  var state = { node_count: 0, converged: false, nodes: [] };
  function esc(s){ return String(s==null?"":s).replace(/[&<>]/g, function(c){
    return {"&":"&amp;","<":"&lt;",">":"&gt;"}[c]; }); }
  function bar(pct){ var c = pct>=90?"crit":pct>=70?"hi":"";
    return '<span class="bar"><i class="'+c+'" style="width:'+Math.min(100,pct)+'%"></i></span>'+pct+'%'; }

  function render(){
    var conv = document.getElementById("conv");
    if (state.node_count > 1 && state.converged){ conv.className="pill ok"; conv.textContent="SWIM: converged ("+state.node_count+" nodes)"; }
    else { conv.className="pill warn"; conv.textContent="SWIM: converging ("+state.node_count+" nodes)"; }

    var rows = state.nodes.map(function(n){
      var peers = n.alive + (n.suspect?(' <span class="st suspect">'+n.suspect+'</span>'):'')
                + (n.dead?(' <span class="st dead">'+n.dead+'</span>'):'');
      return '<tr class="'+(n.selected?"sel":"")+'">'
        + '<td>'+esc(n.short)+(n.selected?' <span class="muted">(shown)</span>':'')+'</td>'
        + '<td>'+esc(n.region)+'</td>'
        + '<td><span class="tag '+esc(n.role)+'">'+esc(n.role||"?")+'</span></td>'
        + '<td>'+bar(n.cpu_pct)+'</td>'
        + '<td>'+n.mem_used_mb+'/'+n.mem_total_mb+'MB</td>'
        + '<td class="num">'+n.actors_live+'</td>'
        + '<td class="num">'+n.mailbox_depth+'</td>'
        + '<td>'+(n.relay_connected?'relay ':'')+n.direct_peers+'d/'+n.relay_peers+'r</td>'
        + '<td class="num">'+peers+'</td>'
        + '<td class="num">'+n.proc_lines+'</td>'
        + '<td class="last">'+esc(n.last_proc)+'</td>'
        + '</tr>';
    }).join("");
    document.getElementById("rows").innerHTML = rows;
    document.getElementById("empty").style.display = state.nodes.length ? "none" : "block";
  }

  function onModel(m){ if(!m||!m.nodes) return; state = m; render(); }

  fetch("/api/plugin/vastai/model").then(function(r){return r.json();}).then(onModel).catch(function(){});
  var dot = document.getElementById("statusDot");
  var es = new EventSource("/events");
  es.onopen = function(){ dot.className = "status-dot"; };
  es.onerror = function(){ dot.className = "status-dot disconnected"; };
  es.addEventListener("vastai", function(e){ try { onModel(JSON.parse(e.data)); } catch(err){} });
  es.addEventListener("done", function(){ dot.className = "status-dot done"; es.close(); });
  window.addEventListener("beforeunload", function(){ es.close(); });
})();
</script>
</body>
</html>"#;

/// Honest in-chrome placeholder for the Datastore nav link: the datastream demo
/// ships no datastore telemetry, so rather than 404 the link, explain that.
const DATASTORE_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Swactor Runtime – Datastore</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Menlo', 'Consolas', 'Monaco', monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }
  .header { display: flex; align-items: center; padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e; }
  .header h1 { font-size: 16px; font-weight: 600; color: #fff; }
  .nav-links { display: flex; gap: 4px; margin-left: 20px; }
  .nav-link { color: #888; text-decoration: none; font-size: 12px; padding: 4px 10px; border-radius: 3px; }
  .nav-link:hover { color: #e0e0e0; }
  .nav-link.active { color: #fff; background: #2a2d3e; }
  .note { margin: 80px auto; max-width: 520px; text-align: center; color: #888; line-height: 1.6; }
  .note b { color: #cbd5e1; }
</style>
</head>
<body>
<div class="header">
  <h1>Swactor Runtime Dashboard</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link">Overview</a>
    <a href="/actors" class="nav-link">Actors</a>
    <a href="/plugin/distribution" class="nav-link">Distribution</a>
    <a href="/plugin/datastore" class="nav-link active">Datastore</a>
    <a href="/plugin/vastai" class="nav-link">Fleet</a>
  </nav>
</div>
<div class="note">
  <p><b>No datastore in this view.</b></p>
  <p>This dashboard is fed by the per-node telemetry <b>datastream</b>, which does not
     carry datastore contents. See <a href="/plugin/distribution" class="nav-link">Distribution</a>
     for the cluster connection graph or <a href="/plugin/vastai" class="nav-link">Fleet</a> for per-node telemetry.</p>
</div>
</body>
</html>"#;

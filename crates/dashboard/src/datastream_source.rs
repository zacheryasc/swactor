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
//!     distribution-page JSON rebuilt from the selected node's membership and
//!     `dist.state` — so it renders the exact SWIM graph a live node shows;
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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datastream::Record;
use datastream::frame::{Frame, StreamId};
use datastream::health::{DATASTREAM_HEALTH, DatastreamHealth};
use distribution::telemetry::{
    DIST_STATE, DistributionState, MEMBERSHIP, MembershipTransition, TRANSPORT_INTERNALS,
    TransportInternals,
};

use crate::telemetry::{
    ActorRec, ActorRuntimeDetail, HOST_RESOURCE, IDENTITY, IdentityRecord, RUNTIME_ACTORS,
    RUNTIME_STATS, RUNTIME_WORKERS, ResourceSample, RuntimeStats as DsRuntimeStats, WorkerCounters,
};

use swactor::actor::ActorAddress;
use swactor::stats::{ActorInfo, RuntimeStats, TickTiming, WorkerInfo};

use crate::plugin::{DashboardPlugin, PluginResponse};

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
    /// Consolidated distribution-subsystem state (cache/registry/directory/...).
    dist_state: Option<DistributionState>,
    /// Per-actor runtime detail (the real actor table).
    actor_detail: Option<ActorRuntimeDetail>,
    /// Aggregated worker-runtime counters (routing/error tallies + tick timing).
    worker_counters: Option<WorkerCounters>,
    /// Datastream self-health (mux assigned/dropped + loss rate).
    datastream_health: Option<DatastreamHealth>,
    /// peer node-id → latest liveness state.
    membership: HashMap<String, String>,
    /// peer node-id → cause of its most recent liveness transition (the SWIM
    /// observer's reason string; the value-add of W3/W4 over the state-diff).
    membership_reason: HashMap<String, String>,
    /// last membership transition, formatted for display (with its cause).
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
            IDENTITY => {
                if let Ok(r) = IdentityRecord::decode(payload) {
                    self.identity = Some(r);
                }
            }
            HOST_RESOURCE => {
                if let Ok(r) = ResourceSample::decode(payload) {
                    self.resource = Some(r);
                }
            }
            RUNTIME_STATS => {
                if let Ok(r) = DsRuntimeStats::decode(payload) {
                    self.runtime = Some(r);
                }
            }
            TRANSPORT_INTERNALS => {
                if let Ok(r) = TransportInternals::decode(payload) {
                    self.transport = Some(r);
                }
            }
            DIST_STATE => {
                if let Ok(r) = DistributionState::decode(payload) {
                    self.dist_state = Some(r);
                }
            }
            RUNTIME_ACTORS => {
                if let Ok(r) = ActorRuntimeDetail::decode(payload) {
                    self.actor_detail = Some(r);
                }
            }
            RUNTIME_WORKERS => {
                if let Ok(r) = WorkerCounters::decode(payload) {
                    self.worker_counters = Some(r);
                }
            }
            DATASTREAM_HEALTH => {
                if let Ok(r) = DatastreamHealth::decode(payload) {
                    self.datastream_health = Some(r);
                }
            }
            MEMBERSHIP => {
                if let Ok(t) = MembershipTransition::decode(payload) {
                    // Display the short id; key the membership map by the full
                    // id so the views can resolve it to a friendly label. Carry
                    // the observer's cause string through to the display — it is
                    // the whole value-add of the production observer over the old
                    // state-diff (which only ever knew *that* a peer changed).
                    let line = if t.reason.is_empty() {
                        format!("{}: {} → {}", short_id(&t.peer), t.from, t.to)
                    } else {
                        format!(
                            "{}: {} → {} ({})",
                            short_id(&t.peer),
                            t.from,
                            t.to,
                            t.reason
                        )
                    };
                    self.membership.insert(t.peer.clone(), t.to.clone());
                    if !t.reason.is_empty() {
                        self.membership_reason
                            .insert(t.peer.clone(), t.reason.clone());
                    }
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

    /// A friendly label for this node. The stream carries only the node id
    /// (a node is generic; its job is resolved orchestrator-side), so the label
    /// is its short id.
    fn label(&self, node_id: &str) -> String {
        short_id(node_id)
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

    /// The per-actor rows for the Actors/Overview table. Prefers the real actor
    /// detail the node ships on `runtime.actors`; falls back to one synthetic row
    /// per datastream channel when only the aggregate runtime stats are present
    /// (an older producer), so the page degrades rather than going blank.
    fn actor_rows(&self) -> Vec<ActorInfo> {
        if let Some(detail) = &self.actor_detail {
            if !detail.actors.is_empty() {
                return detail.actors.iter().map(real_actor_row).collect();
            }
        }

        let mut rows: Vec<ActorInfo> = Vec::new();
        if let Some(r) = &self.resource {
            rows.push(synth_actor(
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
            rows.push(synth_actor(
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
            rows.push(synth_actor(
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
            rows.push(synth_actor(
                &format!("proc.{label}"),
                Some(last.clone()),
                *count,
                vec![("lines".to_string(), *count)],
            ));
        }
        rows
    }

    /// Synthesize the dashboard's native stats from the accumulated datastream.
    fn to_runtime_stats(&self) -> RuntimeStats {
        let ds_rt = self.runtime.clone().unwrap_or(DsRuntimeStats {
            actors_live: 0,
            mailbox_depth: 0,
            scheduled_tasks: 0,
        });

        // One synthetic worker = this node. Routing/error counters come from the
        // `runtime.workers` channel (the deep slice behind the thin runtime.stats);
        // they stay 0 only until that channel has been folded.
        let wc = self.worker_counters.clone().unwrap_or_default();
        let worker = WorkerInfo {
            id: 0,
            num_actors: ds_rt.actors_live as usize,
            mailbox_depth: ds_rt.mailbox_depth as usize,
            messages_processed: if wc.messages_processed > 0 {
                wc.messages_processed
            } else {
                self.total_proc_lines()
            },
            local_sends: wc.local_sends,
            cross_sends: wc.cross_sends,
            inbox_sends: wc.inbox_sends,
            type_mismatches: wc.type_mismatches,
            panics: wc.panics,
            messages_dropped: wc.messages_dropped,
            restarts: wc.restarts,
            stops: wc.stops,
        };

        let actor_details = self.actor_rows();

        let actors = actor_details
            .iter()
            .map(|a| (a.address, a.worker_id))
            .collect();

        // Uptime since this node's first frame was folded (the legacy
        // provider.lifecycle uptime had no producer and was removed).
        let uptime_ms = self
            .first_seen
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);

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

    /// Does this node match the optional selection filter? The stream carries
    /// only the node id, so the filter matches against that.
    fn matches(&self, node_id: &str, filter: &str) -> bool {
        node_id.contains(filter)
    }

    /// Rebuild the distribution-page JSON for this node from the demuxed stream,
    /// so the canonical Distribution page renders its SWIM graph and panels
    /// exactly as it would for a live node. Members (and their liveness) come
    /// from the `membership` channel; cache / registry / directory / probe /
    /// peer-auth from the `dist.state` record; name / listen addr / relay /
    /// version from `identity`. Only live peers are included.
    ///
    /// `invite_code` and `join_statuses` are node-interactive state the
    /// datastream does not carry; a node overlays its own live values, and the
    /// fleet view leaves them null/empty.
    fn dist_snapshot(
        &self,
        node_id: &str,
        peer_labels: &HashMap<String, String>,
        live: &HashSet<String>,
    ) -> serde_json::Value {
        let mut members: Vec<(String, String)> = self
            .live_peers(live)
            .into_iter()
            .map(|(peer, state)| (peer.clone(), state.clone()))
            .collect();
        members.sort_by(|a, b| a.0.cmp(&b.0));

        let count = |want: &str| members.iter().filter(|(_, s)| s == want).count();
        let (alive_count, suspect_count, dead_count) =
            (count("alive"), count("suspect"), count("dead"));
        let members_json: Vec<serde_json::Value> = members
            .iter()
            .map(|(peer, state)| {
                serde_json::json!({
                    "node_id": peer,
                    "addr": serde_json::Value::Null,
                    "state": state,
                    "incarnation": 0,
                    "node_name": peer_labels.get(peer).cloned().unwrap_or_else(|| short_id(peer)),
                    // Cause of this peer's last transition (observer reason),
                    // null until one has been seen.
                    "reason": self.membership_reason.get(peer).cloned(),
                })
            })
            .collect();

        let ds = self.dist_state.as_ref();
        let id = self.identity.as_ref();
        let nonempty = |s: &String| !s.is_empty();

        let cache_entries: Vec<serde_json::Value> = ds
            .map(|d| {
                d.cache_entries
                    .iter()
                    .map(
                        |e| serde_json::json!({ "actor_addr": e.actor_addr, "node_id": e.node_id }),
                    )
                    .collect()
            })
            .unwrap_or_default();
        let registry_entries: Vec<serde_json::Value> = ds
            .map(|d| {
                d.registry_entries
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "name": e.name,
                            "actor_addr": e.actor_addr,
                            "node_id": e.node_id,
                            "tombstone": e.tombstone,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let peer_auth_mode = ds
            .map(|d| d.peer_auth_mode.clone())
            .filter(nonempty)
            .unwrap_or_else(|| "open".into());
        // The old snapshot reported `None` in open mode; preserve that.
        let authorized_peer_count = match ds {
            Some(d) if peer_auth_mode != "open" => serde_json::json!(d.authorized_peer_count),
            _ => serde_json::Value::Null,
        };

        let node_name = id
            .map(|i| i.node_name.clone())
            .filter(nonempty)
            .or_else(|| peer_labels.get(node_id).cloned())
            .unwrap_or_else(|| short_id(node_id));

        serde_json::json!({
            "node_id": node_id,
            "listen_addr": id.map(|i| i.listen_addr.clone()).filter(nonempty),
            "members": members_json,
            "alive_count": alive_count,
            "suspect_count": suspect_count,
            "dead_count": dead_count,
            "cache_size": ds.map(|d| d.cache_size).unwrap_or(0),
            "cache_entries": cache_entries,
            "directory_route_count": ds.map(|d| d.directory_route_count).unwrap_or(0),
            "registry_size": ds.map(|d| d.registry_size).unwrap_or(0),
            "registry_tombstones": ds.map(|d| d.registry_tombstones).unwrap_or(0),
            "registry_entries": registry_entries,
            "recent_probe_targets": ds.map(|d| d.recent_probe_targets.clone()).unwrap_or_default(),
            "peer_auth_mode": peer_auth_mode,
            "authorized_peer_count": authorized_peer_count,
            "node_name": node_name,
            "invite_code": serde_json::Value::Null,
            // "this node runs an embedded relay" — carried on identity now.
            "relay_url": id.map(|i| i.relay_url.clone()).filter(nonempty),
            "version": id.map(|i| i.version.clone()).filter(nonempty),
            "join_statuses": Vec::<serde_json::Value>::new(),
        })
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
        let wc = self.worker_counters.as_ref();
        let dh = self.datastream_health.as_ref();
        let last_proc = self
            .procs
            .values()
            .map(|(_, line)| line.clone())
            .last()
            .unwrap_or_default();

        serde_json::json!({
            "id": node_id,
            "short": short_id(node_id),
            // A node is generic on the stream; the orchestrator attaches
            // role/region as lease metadata (Phase 2). Blank until then.
            "region": "",
            "role": "",
            "selected": selected,
            "cpu_pct": r.map(|r| r.cpu_pct.round() as u32).unwrap_or(0),
            "mem_used_mb": r.map(|r| r.mem_used_mb).unwrap_or(0),
            "mem_total_mb": r.map(|r| r.mem_total_mb).unwrap_or(0),
            "gpu_pct": r.map(|r| r.gpu_pct.round() as u32).unwrap_or(0),
            "disk_used_gb": r.map(|r| r.disk_used_gb).unwrap_or(0),
            "net_rx_kbps": r.map(|r| r.net_rx_kbps).unwrap_or(0),
            "net_tx_kbps": r.map(|r| r.net_tx_kbps).unwrap_or(0),
            "actors_live": rt.map(|r| r.actors_live).unwrap_or(0),
            "mailbox_depth": rt.map(|r| r.mailbox_depth).unwrap_or(0),
            // Real polled value (was a hardcoded 0 in the consumer).
            "scheduled_tasks": rt.map(|r| r.scheduled_tasks).unwrap_or(0),
            // Worker-runtime counters (runtime.workers channel): routing/error
            // tallies + tick timing, the deep slice behind runtime.stats.
            "worker_counters": {
                "num_workers": wc.map(|w| w.num_workers).unwrap_or(0),
                "local_sends": wc.map(|w| w.local_sends).unwrap_or(0),
                "cross_sends": wc.map(|w| w.cross_sends).unwrap_or(0),
                "inbox_sends": wc.map(|w| w.inbox_sends).unwrap_or(0),
                "type_mismatches": wc.map(|w| w.type_mismatches).unwrap_or(0),
                "panics": wc.map(|w| w.panics).unwrap_or(0),
                "messages_dropped": wc.map(|w| w.messages_dropped).unwrap_or(0),
                "restarts": wc.map(|w| w.restarts).unwrap_or(0),
                "stops": wc.map(|w| w.stops).unwrap_or(0),
                "messages_processed": wc.map(|w| w.messages_processed).unwrap_or(0),
                "tick_p50_us": wc.map(|w| w.tick_p50_us).unwrap_or(0),
            },
            "relay_connected": t.map(|t| t.relay_connected).unwrap_or(false),
            "direct_peers": t.map(|t| t.direct_peers).unwrap_or(0),
            "relay_peers": t.map(|t| t.relay_peers).unwrap_or(0),
            "rtt_ms_p50": t.map(|t| t.rtt_ms_p50).unwrap_or(0),
            "alive": alive,
            "suspect": suspect,
            "dead": dead,
            "converged": converged,
            // Most recent SWIM liveness transition this node observed, carrying
            // the observer's cause string (the `reason` was always "" before the
            // migration installed the production observer). `null` until a
            // transition has been folded.
            "last_transition": self.last_transition.clone(),
            // Datastream self-health (datastream.health channel): the pipe
            // reporting its own integrity. loss_rate_ppm = dropped/assigned × 1e6.
            "datastream": {
                "assigned": dh.map(|d| d.assigned).unwrap_or(0),
                "dropped": dh.map(|d| d.dropped).unwrap_or(0),
                "loss_rate_ppm": dh.map(|d| d.loss_rate_ppm).unwrap_or(0),
            },
            "proc_lines": self.total_proc_lines(),
            "last_proc": last_proc,
        })
    }
}

/// `node-id → "region · short-id"` for every demuxed node.
fn build_labels(models: &HashMap<String, DatastreamModel>) -> HashMap<String, String> {
    models
        .iter()
        .map(|(id, m)| (id.clone(), m.label(id)))
        .collect()
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
    let converged = node_count > 1 && nodes.iter().all(|n| n["converged"].as_bool() == Some(true));

    serde_json::json!({
        "node_count": node_count,
        "converged": converged,
        "nodes": nodes,
    })
    .to_string()
}

/// What one folded frame produced: the refreshed Fleet table JSON, the selected
/// node's Distribution snapshot, its synthesized `RuntimeStats` (only when the
/// frame was for the selected node), and any activity-log lines.
pub struct FleetUpdate {
    /// Fleet-table JSON for every live node — write into the `vastai` cache.
    pub fleet_json: String,
    /// The selected node's Distribution snapshot JSON, if a node is selected.
    pub dist_json: Option<String>,
    /// Synthesized single-node stats, present only when this frame was for the
    /// selected node. A UDP demo pushes it via `set_stats`; an orchestrator with
    /// its own live runtime ignores it.
    pub stats: Option<RuntimeStats>,
    /// `(is_warn, message)` activity-log lines for the selected node.
    pub logs: Vec<(bool, String)>,
}

/// The fleet aggregate: per-node demuxed [`DatastreamModel`]s, the selected
/// node, and an optional selection filter. One [`ingest`](Self::ingest) call
/// folds a delivered frame and yields a [`FleetUpdate`]. Transport-agnostic —
/// fed from UDP (the raw demo) or the cluster `datastream-sink` (the
/// orchestrator) alike.
pub struct FleetView {
    models: HashMap<String, DatastreamModel>,
    selected: Option<String>,
    node_filter: Option<String>,
}

impl FleetView {
    /// A fresh fleet view. `node_filter` selects which node drives the
    /// single-node Overview/Distribution panes (the first node whose id contains
    /// the filter); `None` selects the first node seen.
    pub fn new(node_filter: Option<String>) -> Self {
        Self {
            models: HashMap::new(),
            selected: None,
            node_filter,
        }
    }

    /// The live datastream telemetry for `node_id` as a JSON object (the same
    /// per-node fields the Fleet table shows), or `None` if no frame has been
    /// folded for it yet. The orchestrator's `FleetLifecycle` calls this to
    /// merge a launched node's live telemetry onto its launch entry once the
    /// node has joined and started streaming.
    pub fn node_metrics(&self, node_id: &str) -> Option<serde_json::Value> {
        let now = Instant::now();
        let live = live_set(&self.models, now);
        let expected_peers = live.len().saturating_sub(1);
        self.models
            .get(node_id)
            .map(|m| m.node_summary(node_id, expected_peers, false, &live))
    }

    /// Has `node_id` streamed a frame within the liveness window?
    pub fn is_live(&self, node_id: &str) -> bool {
        self.models
            .get(node_id)
            .map(|m| m.is_live(Instant::now()))
            .unwrap_or(false)
    }

    /// Fold one delivered `(stream, frame)` into the fleet and recompute the
    /// views. See [`FleetUpdate`] for what is returned.
    pub fn ingest(&mut self, stream: &StreamId, frame: &Frame) -> FleetUpdate {
        let node = stream.node.as_str().to_string();
        let model = self.models.entry(node.clone()).or_default();
        let events = model.update(frame.channel.as_str(), &frame.payload);

        // Pick the display node: first matching the filter, else first seen.
        if self.selected.is_none() {
            let qualifies = match self.node_filter.as_deref() {
                Some(f) => model.matches(&node, f),
                None => true,
            };
            if qualifies {
                self.selected = Some(node.clone());
            }
        }
        let is_selected = self.selected.as_deref() == Some(node.as_str());

        // Activity log + single-node stats only for the selected node.
        let logs = if is_selected {
            events
                .into_iter()
                .map(|e| match e {
                    LogEvent::Info(m) => (false, m),
                    LogEvent::Warn(m) => (true, m),
                })
                .collect()
        } else {
            Vec::new()
        };

        let now = Instant::now();
        let live = live_set(&self.models, now);
        let labels = build_labels(&self.models);
        let fleet_json = fleet_json(&self.models, self.selected.as_deref(), &live);
        let stats = if is_selected {
            self.models.get(&node).map(|m| m.to_runtime_stats())
        } else {
            None
        };
        let dist_json = self.selected.as_deref().and_then(|sel| {
            self.models
                .get(sel)
                .and_then(|m| serde_json::to_string(&m.dist_snapshot(sel, &labels, &live)).ok())
        });

        FleetUpdate {
            fleet_json,
            dist_json,
            stats,
            logs,
        }
    }
}

/// A ready-to-register Fleet (`vastai`) plugin backed by `cache`: serves the
/// Fleet page and the `vastai` SSE/JSON model. The orchestrator registers this
/// and feeds `cache` from its `datastream-sink`.
pub fn fleet_cache_plugin(cache: Arc<Mutex<Option<String>>>) -> Arc<dyn DashboardPlugin> {
    Arc::new(CachePlugin::new("vastai", FLEET_HTML, cache))
}

/// Plugin backed by a shared cache string: serves a fixed HTML page, emits its
/// cache on the SSE stream under `name`, and answers `GET /api/plugin/{name}`.
/// Used for both the Fleet table (`vastai`) and the Distribution graph
/// (`distribution`); the orchestrator's `datastream-sink` actor folds frames
/// into a `FleetView` and writes the JSON the Fleet plugin serves.
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
            ("GET", "" | "model" | "snapshot") => PluginResponse::json(
                self.cache
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "{}".into()),
            ),
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

/// Map a wire [`ActorRec`] into a dashboard [`ActorInfo`] row.
fn real_actor_row(a: &ActorRec) -> ActorInfo {
    ActorInfo {
        address: parse_addr_hex(&a.address).unwrap_or_else(|| addr_from(&a.name)),
        worker_id: 0,
        mailbox_depth: a.mailbox_depth as usize,
        last_msg_type: (!a.last_msg_type.is_empty()).then(|| a.last_msg_type.clone()),
        messages_processed: a.messages_processed,
        poisoned: a.poisoned,
        name: (!a.name.is_empty()).then(|| a.name.clone()),
        message_type_counts: a.message_type_counts.clone(),
    }
}

/// Parse a 64-char hex actor address back into an [`ActorAddress`]; `None` if it
/// is not exactly 32 hex-encoded bytes.
fn parse_addr_hex(hex: &str) -> Option<ActorAddress> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(ActorAddress(bytes))
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
  .state { padding: 1px 8px; border-radius: 4px; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: .3px; }
  .state.bootstrapping { background: #2a2433; color: #c4b5fd; }
  .state.live { background: #14361f; color: #4ade80; }
  .state.failed { background: #3a1518; color: #f87171; }
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
      <th>node</th><th>state</th><th>region</th><th>role</th><th>CPU</th><th>mem</th>
      <th class="num">disk</th><th class="num">net ↓/↑</th>
      <th class="num">actors</th><th class="num">mbox</th><th>transport</th>
      <th class="num">peers</th><th class="num">proc</th><th class="num">pipe</th><th>last line</th>
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
    var live = (state.live_count==null) ? state.node_count : state.live_count;
    if (state.node_count > 1 && state.converged){ conv.className="pill ok"; conv.textContent="SWIM: converged ("+state.node_count+" nodes)"; }
    else { conv.className="pill warn"; conv.textContent="SWIM: converging ("+live+"/"+state.node_count+" live)"; }

    var rows = state.nodes.map(function(n){
      var peers = n.alive + (n.suspect?(' <span class="st suspect">'+n.suspect+'</span>'):'')
                + (n.dead?(' <span class="st dead">'+n.dead+'</span>'):'');
      var st = n.state || "live";
      return '<tr class="'+(n.selected?"sel":"")+'">'
        + '<td>'+esc(n.short)+(n.selected?' <span class="muted">(shown)</span>':'')+'</td>'
        + '<td><span class="state '+esc(st)+'">'+esc(st)+'</span></td>'
        + '<td>'+esc(n.region)+'</td>'
        + '<td><span class="tag '+esc(n.role)+'">'+esc(n.role||"?")+'</span></td>'
        + '<td>'+bar(n.cpu_pct)+'</td>'
        + '<td>'+n.mem_used_mb+'/'+n.mem_total_mb+'MB</td>'
        + '<td class="num">'+(n.disk_used_gb||0)+'G</td>'
        + '<td class="num">'+(n.net_rx_kbps||0)+'/'+(n.net_tx_kbps||0)+'</td>'
        + '<td class="num">'+n.actors_live+'</td>'
        + '<td class="num">'+n.mailbox_depth+'</td>'
        + '<td>'+(n.relay_connected?'relay ':'')+n.direct_peers+'d/'+n.relay_peers+'r</td>'
        + '<td class="num">'+peers+'</td>'
        + '<td class="num">'+n.proc_lines+'</td>'
        + '<td class="num" title="frames assigned / dropped">'
          +((n.datastream&&n.datastream.assigned)||0)
          +((n.datastream&&n.datastream.dropped)?(' <span class="st dead">-'+n.datastream.dropped+'</span>'):'')
          +'</td>'
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

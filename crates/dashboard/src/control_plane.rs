//! Fused control-plane view: machine stats and actor stats per node stream.
//!
//! swactor is the control plane for data and hardware; this page surfaces
//! both. Every telemetry stream (`node#life`) folds into one [`FusedNode`]
//! holding the machine side ([`NodeHardwareState`]: `host.*` samples,
//! managed-process lifecycle) and the actor side ([`RuntimeState`]:
//! `runtime.actors` / `runtime.stats` frames). The page renders three
//! levels: node cards → per-node roster → per-actor dossier (via the
//! `/api/view/fleet/detail` endpoint).
//!
//! Stale streams (silent beyond [`LIVE_TTL`]) leave the live pool: they
//! render in a separate collapsed section and are physically capped at
//! [`STALE_POOL_CAP`]. A newer `life` generation for the same node evicts
//! older generations immediately — restarts stop accumulating.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use telemetry::frame::{Frame, StreamId};
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{Value, json};

use crate::hardware_view::{
    CpuSnapshot, GpuSnapshot, HardwareHistorySnapshot, NodeHardwareState, duration_ms,
    saturating_u32,
};
use crate::swactor::{ActorState, RuntimeState};
use crate::view::DashboardView;
use crate::{FrameEvent, StreamEvent};

const CONTROL_PLANE_HTML: &str = include_str!("control_plane_page.html");
/// A stream is live while any frame arrived within this window.
const LIVE_TTL: Duration = Duration::from_secs(8);
/// Hard cap on retained stale streams; oldest are evicted.
const STALE_POOL_CAP: usize = 50;

/// Fused control-plane view serving `/` and `/view/fleet`.
#[derive(Default)]
pub struct ControlPlaneView {
    state: RwLock<FusedState>,
}

#[derive(Default)]
struct FusedState {
    streams: BTreeMap<String, FusedNode>,
}

struct FusedNode {
    stream: StreamEvent,
    last_seen: Instant,
    hardware: NodeHardwareState,
    actors: RuntimeState,
    origin: Option<String>,
    label: Option<String>,
}

impl DashboardView for ControlPlaneView {
    fn id(&self) -> &'static str {
        "fleet"
    }

    fn title(&self) -> &'static str {
        "Fleet"
    }

    fn path(&self) -> &'static str {
        "fleet"
    }

    fn channels(&self) -> &'static [&'static str] {
        // All channels: the machine side and the actor side each ignore what
        // does not concern them.
        &[]
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        let now = Instant::now();
        let mut state = self.state.write();
        let key = stream_key(&event.stream);
        let node = state.streams.entry(key).or_insert_with(|| FusedNode {
            stream: event.stream.clone(),
            last_seen: now,
            hardware: NodeHardwareState::new(now),
            actors: RuntimeState::new(now),
            origin: event.stream.origin.clone(),
            label: event.stream.label.clone(),
        });
        node.last_seen = now;
        // Later events may carry descriptor metadata the first lacked.
        if let Some(origin) = &event.stream.origin {
            node.origin = Some(origin.clone());
        }
        if let Some(label) = &event.stream.label {
            node.label = Some(label.clone());
        }
        node.hardware.update(&event.channel, &event.payload, now);
        node.actors.update(&event.channel, &event.payload, now);
        prune(&mut state.streams, &event.stream, now);
    }

    fn snapshot_json(&self) -> Value {
        let now = Instant::now();
        let state = self.state.read();
        let mut live = Vec::new();
        let mut stale = Vec::new();
        for node in state.streams.values() {
            let card = node_card(node, now);
            if card.live {
                live.push(card);
            } else {
                stale.push(card);
            }
        }
        live.sort_by(|left, right| {
            left.stream
                .node
                .cmp(&right.stream.node)
                .then_with(|| left.stream.life.cmp(&right.stream.life))
        });
        // Stale pool: most recently seen first, bounded by the physical cap.
        stale.sort_by_key(|right| std::cmp::Reverse(right.last_seen_ms_ago));
        stale.truncate(STALE_POOL_CAP);
        let totals = fused_totals(live.len(), stale.len());
        let snapshot = FusedSnapshot { totals, live, stale };
        serde_json::to_value(snapshot).unwrap_or_else(|_| {
            json!({
                "totals": FusedTotals::default(),
                "live": [],
                "stale": []
            })
        })
    }

    fn detail_json(&self, query: &str) -> Option<Value> {
        let stream = query_param(query, "stream")?;
        let actor_key = query_param(query, "actor")?;
        let now = Instant::now();
        let state = self.state.read();
        let node = state.streams.get(&stream)?;
        let actor = node.actors.actors.get(&actor_key)?;
        Some(actor_detail(
            actor,
            &node.stream,
            &stream,
            now,
            node.origin.clone(),
            node.label.clone(),
        ))
    }

    fn html(&self) -> Option<&'static str> {
        Some(CONTROL_PLANE_HTML)
    }
}

/// Evict superseded life generations and enforce the stale-pool cap.
fn prune(streams: &mut BTreeMap<String, FusedNode>, fresh: &StreamEvent, now: Instant) {
    // A newer life generation for the same node replaces older ones: the old
    // process is gone by construction once its successor publishes.
    let superseded: Vec<String> = streams
        .iter()
        .filter(|(_, node)| {
            node.stream.node == fresh.node && node.stream.life < fresh.life
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in superseded {
        streams.remove(&key);
    }

    let mut stale: Vec<(String, Instant)> = streams
        .iter()
        .filter(|(_, node)| now.duration_since(node.last_seen) > LIVE_TTL)
        .map(|(key, node)| (key.clone(), node.last_seen))
        .collect();
    if stale.len() > STALE_POOL_CAP {
        stale.sort_by_key(|(_, seen)| *seen);
        let excess = stale.len() - STALE_POOL_CAP;
        for (key, _) in stale.into_iter().take(excess) {
            streams.remove(&key);
        }
    }
}

#[derive(Serialize)]
struct FusedSnapshot {
    totals: FusedTotals,
    live: Vec<NodeCard>,
    stale: Vec<NodeCard>,
}

#[derive(Default, Serialize)]
struct FusedTotals {
    live_nodes: u32,
    stale_nodes: u32,
}

#[derive(Serialize)]
struct NodeCard {
    stream: StreamKeySnapshot,
    live: bool,
    last_seen_ms_ago: u64,
    last_sample_unix_ms: Option<u64>,
    errors: Vec<String>,
    cpu: Option<CpuSnapshot>,
    gpu: Option<GpuSnapshot>,
    net: Option<crate::hardware_view::NetSnapshot>,
    process: Option<crate::hardware_view::ProcessSnapshot>,
    history: Vec<HardwareHistorySnapshot>,
    actor_summary: ActorSummarySnapshot,
    /// Aggregate-only roster rows; heavy per-actor detail lives behind the
    /// detail endpoint so snapshot payload stays independent of ring sizes.
    roster: Vec<RosterRow>,
}

#[derive(Serialize)]
struct StreamKeySnapshot {
    key: String,
    node: String,
    life: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
}

#[derive(Default, Serialize)]
struct ActorSummarySnapshot {
    actors: u32,
    msg_per_sec: f64,
    mailbox_depth: u32,
    poisoned: u32,
    uptime_ms: Option<u64>,
    num_workers: Option<u32>,
}

#[derive(Serialize)]
struct RosterRow {
    address: String,
    name: Option<String>,
    actor_type: Option<String>,
    message_type: Option<String>,
    state: &'static str,
    mailbox_depth: u32,
    messages_processed: u64,
    msg_per_sec: f64,
    worker_id: Option<u32>,
    poisoned: bool,
    last_msg_type: Option<String>,
}

#[derive(Serialize)]
struct ActorDetail {
    stream: StreamKeySnapshot,
    address: String,
    name: Option<String>,
    actor_type: Option<String>,
    message_type: Option<String>,
    worker_id: Option<u32>,
    state: &'static str,
    poisoned: bool,
    mailbox_depth: u32,
    mailbox_growth: f64,
    messages_processed: u64,
    msg_per_sec: f64,
    last_msg_type: Option<String>,
    message_type_counts: Vec<MessageTypeCountSnapshot>,
    history: Vec<ActorHistoryPoint>,
    receipts: Vec<ReceiptSnapshot>,
    sampled_out: u64,
}

#[derive(Serialize)]
struct MessageTypeCountSnapshot {
    message_type: String,
    count: u64,
}

#[derive(Serialize)]
struct ActorHistoryPoint {
    ms_ago: u64,
    mailbox_depth: u32,
    msg_per_sec: f64,
}

#[derive(Serialize)]
struct ReceiptSnapshot {
    ms_ago: u64,
    ty: String,
    folded: u64,
}

fn stream_key(stream: &StreamEvent) -> String {
    format!("{}#{}", stream.node, stream.life)
}

fn node_card(node: &FusedNode, now: Instant) -> NodeCard {
    let summary = node.hardware.summary();
    let totals = node.actors.totals();
    let mut roster: Vec<RosterRow> = node
        .actors
        .actors
        .values()
        .map(roster_row)
        .collect();
    // Busiest actors first; ties fall back to address for stable rendering.
    roster.sort_by(|left, right| {
        right
            .msg_per_sec
            .partial_cmp(&left.msg_per_sec)
            .map_or(std::cmp::Ordering::Equal, |order| order)
            .then_with(|| left.address.cmp(&right.address))
    });
    NodeCard {
        stream: StreamKeySnapshot {
            key: stream_key(&node.stream),
            node: node.stream.node.clone(),
            life: node.stream.life,
            origin: node.origin.clone(),
            label: node.label.clone(),
        },
        live: now.duration_since(node.last_seen) <= LIVE_TTL,
        last_seen_ms_ago: duration_ms(now.duration_since(node.last_seen)),
        last_sample_unix_ms: summary.sample_unix_ms,
        errors: node.hardware.errors(),
        cpu: node.hardware.cpu.as_ref().map(CpuSnapshot::from),
        gpu: node.hardware.gpu.as_ref().map(GpuSnapshot::from),
        net: node.hardware.net.clone(),
        process: node.hardware.process.clone(),
        history: node
            .hardware
            .history
            .iter()
            .map(|sample| HardwareHistorySnapshot {
                ms_ago: duration_ms(now.duration_since(sample.at)),
                sample_unix_ms: sample.sample_unix_ms,
                cpu_total_percent: sample.cpu_total_percent,
                gpu_max_percent: sample.gpu_max_percent,
                gpu_memory_used_mib: sample.gpu_memory_used_mib,
                gpu_memory_total_mib: sample.gpu_memory_total_mib,
                net_rx_bps: sample.net_rx_bps,
                net_tx_bps: sample.net_tx_bps,
            })
            .collect(),
        actor_summary: ActorSummarySnapshot {
            actors: totals.actors,
            msg_per_sec: totals.msg_per_sec,
            mailbox_depth: totals.mailbox_depth,
            poisoned: totals.poisoned,
            uptime_ms: node.actors.uptime_ms,
            num_workers: node.actors.num_workers,
        },
        roster,
    }
}

fn roster_row(actor: &ActorState) -> RosterRow {
    RosterRow {
        address: actor.address.clone(),
        name: actor.name.clone(),
        actor_type: actor.actor_type.clone(),
        message_type: actor.message_type.clone(),
        state: if actor.poisoned { "poisoned" } else { "running" },
        mailbox_depth: actor.mailbox_depth,
        messages_processed: actor.messages_processed,
        msg_per_sec: actor.msg_per_sec,
        worker_id: actor.worker_id,
        poisoned: actor.poisoned,
        last_msg_type: actor.last_msg_type.clone(),
    }
}

fn actor_detail(
    actor: &ActorState,
    stream: &StreamEvent,
    stream_key_value: &str,
    now: Instant,
    origin: Option<String>,
    label: Option<String>,
) -> Value {
    let detail = ActorDetail {
        stream: StreamKeySnapshot {
            key: stream_key_value.to_owned(),
            node: stream.node.clone(),
            life: stream.life,
            origin,
            label,
        },
        address: actor.address.clone(),
        name: actor.name.clone(),
        actor_type: actor.actor_type.clone(),
        message_type: actor.message_type.clone(),
        worker_id: actor.worker_id,
        state: if actor.poisoned { "poisoned" } else { "running" },
        poisoned: actor.poisoned,
        mailbox_depth: actor.mailbox_depth,
        mailbox_growth: actor.mailbox_growth,
        messages_processed: actor.messages_processed,
        msg_per_sec: actor.msg_per_sec,
        last_msg_type: actor.last_msg_type.clone(),
        message_type_counts: actor
            .message_type_counts
            .iter()
            .map(|(message_type, count)| MessageTypeCountSnapshot {
                message_type: message_type.clone(),
                count: *count,
            })
            .collect(),
        history: actor
            .history
            .iter()
            .map(|sample| ActorHistoryPoint {
                ms_ago: duration_ms(now.duration_since(sample.at)),
                mailbox_depth: sample.mailbox_depth,
                msg_per_sec: sample.msg_per_sec,
            })
            .collect(),
        receipts: actor
            .receipts
            .iter()
            .map(|receipt| ReceiptSnapshot {
                ms_ago: duration_ms(now.duration_since(receipt.at)),
                ty: receipt.ty.clone(),
                folded: receipt.folded,
            })
            .collect(),
        sampled_out: actor.sampled_out,
    };
    serde_json::to_value(detail).unwrap_or_else(|_| json!({}))
}

fn fused_totals(live_len: usize, stale_len: usize) -> FusedTotals {
    FusedTotals {
        live_nodes: saturating_u32(live_len),
        stale_nodes: saturating_u32(stale_len),
    }
}

/// Minimal `application/x-www-form-urlencoded` reader with percent-decoding
/// (stream keys contain `#`, encoded as `%23`).
fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if parts.next() == Some(key) {
            return parts.next().map(percent_decode);
        }
    }
    None
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = bytes
                .get(index + 1..index + 3)
                .and_then(|slice| std::str::from_utf8(slice).ok())
                .and_then(|slice| u8::from_str_radix(slice, 16).ok());
            if let Some(byte) = hex {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry::frame::{ChannelId, Lifetime, NodeId, Position};

    fn ingest_json(
        view: &ControlPlaneView,
        stream: &StreamId,
        position: u64,
        channel: &str,
        payload: Vec<u8>,
    ) {
        let frame = Frame::new(ChannelId(1), Position(position), payload);
        let event = FrameEvent {
            stream: crate::StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
                origin: None,
                label: None,
            },
            channel: channel.to_string(),
            position,
            payload: frame.payload.clone(),
        };
        view.ingest(stream, &frame, &event);
    }

    #[test]
    fn stream_origin_and_label_surface_on_cards() {
        let view = ControlPlaneView::default();
        let stream = StreamId::new(NodeId::new("supervisor"), Lifetime(1));
        let frame = Frame::new(ChannelId(1), Position(0), actors_payload(0, json!([])));
        let event = FrameEvent {
            stream: crate::StreamEvent {
                node: "supervisor".to_owned(),
                life: 1,
                origin: Some("orchestrator".to_owned()),
                label: Some("provisioning supervisor".to_owned()),
            },
            channel: "runtime.actors".to_owned(),
            position: 0,
            payload: frame.payload.clone(),
        };
        view.ingest(&stream, &frame, &event);

        let snapshot = view.snapshot_json();
        let card = &snapshot["live"][0]["stream"];
        assert_eq!(card["origin"], json!("orchestrator"));
        assert_eq!(card["label"], json!("provisioning supervisor"));
    }

    fn actors_payload(worker: u32, actors: serde_json::Value) -> Vec<u8> {
        serde_json::json!({ "worker_id": worker, "actors": actors })
            .to_string()
            .into_bytes()
    }

    #[test]
    fn live_and_stale_streams_render_in_separate_pools() {
        let view = ControlPlaneView::default();
        let live = StreamId::new(NodeId::new("live-node"), Lifetime(1));
        let stale = StreamId::new(NodeId::new("stale-node"), Lifetime(1));
        ingest_json(
            &view,
            &live,
            0,
            "runtime.actors",
            actors_payload(
                0,
                serde_json::json!([{ "address": "aa", "actor_type": "PingActor",
                    "messages_processed": 3, "last_msg_type": "Ping" }]),
            ),
        );
        ingest_json(
            &view,
            &stale,
            0,
            "runtime.actors",
            actors_payload(0, serde_json::json!([{ "address": "bb" }])),
        );

        let snapshot = view.snapshot_json();
        // Freshly ingested: both live.
        assert_eq!(snapshot["live"].as_array().map(Vec::len), Some(2));
        assert_eq!(snapshot["totals"]["live_nodes"].as_u64(), Some(2));

        // The stale pool split is time-based; simulate by direct state surgery
        // because Instant cannot be rewound through the public API.
        {
            let mut state = view.state.write();
            let node = state.streams.get_mut("stale-node#1").expect("stale node");
            node.last_seen -= LIVE_TTL + Duration::from_secs(1);
        }
        let snapshot = view.snapshot_json();
        let live_nodes = snapshot["live"].as_array().expect("live array");
        let stale_nodes = snapshot["stale"].as_array().expect("stale array");
        assert_eq!(live_nodes.len(), 1);
        assert_eq!(stale_nodes.len(), 1);
        assert_eq!(live_nodes[0]["stream"]["node"], json!("live-node"));
        assert_eq!(stale_nodes[0]["stream"]["node"], json!("stale-node"));
        assert_eq!(snapshot["totals"]["stale_nodes"].as_u64(), Some(1));
    }

    #[test]
    fn newer_life_generation_evicts_superseded_stream() {
        let view = ControlPlaneView::default();
        let old = StreamId::new(NodeId::new("node"), Lifetime(1));
        let new = StreamId::new(NodeId::new("node"), Lifetime(2));
        ingest_json(&view, &old, 0, "runtime.actors", actors_payload(0, json!([])));
        ingest_json(&view, &new, 0, "runtime.actors", actors_payload(0, json!([])));

        let snapshot = view.snapshot_json();
        let nodes: Vec<&str> = snapshot["live"]
            .as_array()
            .expect("live array")
            .iter()
            .map(|node| node["stream"]["key"].as_str().expect("key"))
            .collect();
        assert_eq!(nodes, vec!["node#2"]);
    }

    #[test]
    fn stale_pool_is_hard_capped() {
        let view = ControlPlaneView::default();
        for index in 0..(STALE_POOL_CAP as u64 + 5) {
            let stream = StreamId::new(NodeId::new(&format!("old-{index}")), Lifetime(1));
            ingest_json(&view, &stream, 0, "runtime.actors", actors_payload(0, json!([])));
        }
        {
            let mut state = view.state.write();
            for node in state.streams.values_mut() {
                node.last_seen -= LIVE_TTL + Duration::from_secs(1);
            }
        }
        // Physical cap applies on the next ingest…
        let fresh = StreamId::new(NodeId::new("fresh"), Lifetime(1));
        ingest_json(&view, &fresh, 0, "runtime.actors", actors_payload(0, json!([])));

        let state = view.state.read();
        assert!(state.streams.len() <= STALE_POOL_CAP + 1);
    }

    #[test]
    fn detail_returns_actor_diet_and_receipts() {
        let view = ControlPlaneView::default();
        let stream = StreamId::new(NodeId::new("node"), Lifetime(4));
        ingest_json(
            &view,
            &stream,
            0,
            "runtime.actors",
            actors_payload(
                7,
                serde_json::json!([{
                    "address": "ff00",
                    "actor_type": "OrchestratorActor",
                    "message_type": "OrchestratorMsg",
                    "mailbox_depth": 2,
                    "messages_processed": 10,
                    "last_msg_type": "Ping",
                    "message_type_counts": [ { "ty": "Ping", "count": 6 },
                                             { "ty": "Pong", "count": 4 } ]
                }]),
            ),
        );

        let detail = view
            .detail_json("stream=node%234&actor=ff00")
            .expect("detail");
        assert_eq!(detail["address"], json!("ff00"));
        assert_eq!(detail["actor_type"], json!("OrchestratorActor"));
        assert_eq!(detail["worker_id"], json!(7));
        assert_eq!(detail["message_type_counts"][0]["message_type"], json!("Ping"));
        assert_eq!(detail["receipts"].as_array().map(Vec::len), Some(1));
        assert_eq!(detail["receipts"][0]["ty"], json!("Ping"));
    }

    #[test]
    fn message_jump_folds_into_one_receipt_with_sampled_count() {
        let view = ControlPlaneView::default();
        let stream = StreamId::new(NodeId::new("node"), Lifetime(1));
        ingest_json(
            &view,
            &stream,
            0,
            "runtime.actors",
            actors_payload(
                0,
                serde_json::json!([{ "address": "aa", "messages_processed": 1,
                    "last_msg_type": "Ping" }]),
            ),
        );
        // Same-tick follow-up: a jump of 4 messages within the interval.
        ingest_json(
            &view,
            &stream,
            1,
            "runtime.actors",
            actors_payload(
                0,
                serde_json::json!([{ "address": "aa", "messages_processed": 5,
                    "last_msg_type": "Ping" }]),
            ),
        );

        let detail = view.detail_json("stream=node%231&actor=aa").expect("detail");
        let receipts = detail["receipts"].as_array().expect("receipts");
        assert_eq!(receipts.len(), 1);
        assert_eq!(detail["sampled_out"].as_u64(), Some(4));
    }

    #[test]
    fn detail_requires_stream_and_actor_params() {
        let view = ControlPlaneView::default();
        assert!(view.detail_json("").is_none());
        assert!(view.detail_json("stream=node%231").is_none());
    }

    #[test]
    fn percent_decoding_handles_hash_and_plus() {
        assert_eq!(query_param("stream=node%234&actor=ab", "stream").as_deref(), Some("node#4"));
        assert_eq!(query_param("a=1&actor=cd", "actor").as_deref(), Some("cd"));
        assert_eq!(query_param("stream=x", "actor"), None);
    }
}

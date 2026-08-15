use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use telemetry::frame::{Frame, StreamId};
use telemetry::hardware::cpu::{
    CpuCoreSample, CpuHostSample, CpuProcessSample, HOST_CPU_CHANNEL, HostCpuSample,
};
use telemetry::hardware::gpu::{
    GpuDeviceSample, GpuProcessSample, HOST_GPU_CHANNEL, HostGpuSample,
};
use telemetry::hardware::net::{HOST_NET_CHANNEL, HostNetSample, NetInterfaceSample};
use telemetry::Record;
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{Value, json};

use crate::view::DashboardView;
use crate::{FrameEvent, StreamEvent};

const HARDWARE_HTML: &str = include_str!("hardware_page.html");
const CHANNELS: &[&str] = &[];
const HISTORY_CAP: usize = 300;
const HISTORY_MIN_INTERVAL: Duration = Duration::from_millis(900);
const LIVE_TTL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub struct HardwareDashboardView {
    state: RwLock<HardwareDashboardState>,
}

#[derive(Default)]
struct HardwareDashboardState {
    nodes: BTreeMap<String, NodeHardwareState>,
}

struct NodeHardwareState {
    stream: StreamEvent,
    last_seen: Instant,
    decode_errors: BTreeMap<&'static str, String>,
    cpu: Option<HostCpuSample>,
    gpu: Option<HostGpuSample>,
    net: Option<NetSnapshot>,
    history: VecDeque<HardwareHistoryState>,
}

impl NodeHardwareState {
    fn new(stream: StreamEvent, now: Instant) -> Self {
        Self {
            stream,
            last_seen: now,
            decode_errors: BTreeMap::new(),
            cpu: None,
            gpu: None,
            net: None,
            history: VecDeque::with_capacity(HISTORY_CAP),
        }
    }

    fn update(&mut self, channel: &str, payload: &[u8], now: Instant) {
        self.last_seen = now;
        match channel {
            HOST_CPU_CHANNEL => match HostCpuSample::decode(payload) {
                Ok(sample) => {
                    self.cpu = Some(sample);
                    self.decode_errors.remove(HOST_CPU_CHANNEL);
                    self.update_history(now);
                }
                Err(error) => self.store_decode_error(HOST_CPU_CHANNEL, error),
            },
            HOST_GPU_CHANNEL => match HostGpuSample::decode(payload) {
                Ok(sample) => {
                    self.gpu = Some(sample);
                    self.decode_errors.remove(HOST_GPU_CHANNEL);
                    self.update_history(now);
                }
                Err(error) => self.store_decode_error(HOST_GPU_CHANNEL, error),
            },
            HOST_NET_CHANNEL => match HostNetSample::decode(payload) {
                Ok(sample) => {
                    self.net = Some(NetSnapshot::from_sample(sample, self.net.as_ref()));
                    self.decode_errors.remove(HOST_NET_CHANNEL);
                    self.update_history(now);
                }
                Err(error) => self.store_decode_error(HOST_NET_CHANNEL, error),
            },
            _ => {}
        }
    }

    fn store_decode_error(&mut self, channel: &'static str, error: serde_json::Error) {
        self.decode_errors
            .insert(channel, format!("{channel} decode error: {error}"));
    }

    fn summary(&self) -> HardwareSummary {
        let cpu_total_percent = self
            .cpu
            .as_ref()
            .and_then(|sample| sample.host.as_ref())
            .and_then(|host| host.total_percent);

        let mut gpu_max_percent = None;
        let mut gpu_memory_used_mib = 0_u64;
        let mut gpu_memory_total_mib = 0_u64;
        if let Some(gpu) = &self.gpu {
            for device in &gpu.gpus {
                if let Some(percent) = device.utilization_gpu_percent {
                    gpu_max_percent =
                        Some(gpu_max_percent.map_or(percent, |current: u64| current.max(percent)));
                }
                gpu_memory_used_mib =
                    gpu_memory_used_mib.saturating_add(device.memory_used_mib.unwrap_or_default());
                gpu_memory_total_mib = gpu_memory_total_mib
                    .saturating_add(device.memory_total_mib.unwrap_or_default());
            }
        }

        let (net_rx_bps, net_tx_bps) = self.net.as_ref().map_or((0.0, 0.0), |net| {
            net.interfaces.iter().fold((0.0, 0.0), |totals, interface| {
                (
                    totals.0 + interface.rx_bps.unwrap_or_default(),
                    totals.1 + interface.tx_bps.unwrap_or_default(),
                )
            })
        });

        HardwareSummary {
            sample_unix_ms: [
                self.cpu.as_ref().map(|sample| sample.sample_unix_ms),
                self.gpu.as_ref().map(|sample| sample.sample_unix_ms),
                self.net.as_ref().map(|sample| sample.sample_unix_ms),
            ]
            .into_iter()
            .flatten()
            .max(),
            cpu_total_percent,
            gpu_max_percent,
            gpu_memory_used_mib,
            gpu_memory_total_mib,
            net_rx_bps,
            net_tx_bps,
        }
    }

    fn update_history(&mut self, now: Instant) {
        let summary = self.summary();
        if let Some(last) = self.history.back_mut()
            && now.duration_since(last.at) < HISTORY_MIN_INTERVAL
        {
            last.sample_unix_ms = summary.sample_unix_ms;
            last.cpu_total_percent = summary.cpu_total_percent;
            last.gpu_max_percent = summary.gpu_max_percent;
            last.gpu_memory_used_mib = summary.gpu_memory_used_mib;
            last.gpu_memory_total_mib = summary.gpu_memory_total_mib;
            last.net_rx_bps = summary.net_rx_bps;
            last.net_tx_bps = summary.net_tx_bps;
            return;
        }

        if self.history.len() == HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back(HardwareHistoryState {
            at: now,
            sample_unix_ms: summary.sample_unix_ms,
            cpu_total_percent: summary.cpu_total_percent,
            gpu_max_percent: summary.gpu_max_percent,
            gpu_memory_used_mib: summary.gpu_memory_used_mib,
            gpu_memory_total_mib: summary.gpu_memory_total_mib,
            net_rx_bps: summary.net_rx_bps,
            net_tx_bps: summary.net_tx_bps,
        });
    }

    fn errors(&self) -> Vec<String> {
        let mut errors = self.decode_errors.values().cloned().collect::<Vec<_>>();
        if let Some(error) = self.cpu.as_ref().and_then(|sample| sample.error.as_ref()) {
            errors.push(format!("{HOST_CPU_CHANNEL}: {error}"));
        }
        if let Some(error) = self.gpu.as_ref().and_then(|sample| sample.error.as_ref()) {
            errors.push(format!("{HOST_GPU_CHANNEL}: {error}"));
        }
        if let Some(error) = self.net.as_ref().and_then(|sample| sample.error.as_ref()) {
            errors.push(format!("{HOST_NET_CHANNEL}: {error}"));
        }
        errors
    }
}

#[derive(Clone, Serialize)]
struct NetSnapshot {
    seq: u64,
    sample_unix_ms: u64,
    interfaces: Vec<NetInterfaceSnapshot>,
    error: Option<String>,
}

impl NetSnapshot {
    fn from_sample(sample: HostNetSample, previous: Option<&Self>) -> Self {
        let previous_interfaces = previous
            .map(|net| {
                net.interfaces
                    .iter()
                    .map(|interface| (interface.name.as_str(), interface))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let elapsed_secs = previous.and_then(|net| {
            sample
                .sample_unix_ms
                .checked_sub(net.sample_unix_ms)
                .filter(|elapsed_ms| *elapsed_ms > 0)
                .map(|elapsed_ms| elapsed_ms as f64 / 1000.0)
        });
        let interfaces = sample
            .interfaces
            .into_iter()
            .map(|interface| {
                let previous = previous_interfaces.get(interface.name.as_str()).copied();
                NetInterfaceSnapshot::from_sample(interface, previous, elapsed_secs)
            })
            .collect();

        Self {
            seq: sample.seq,
            sample_unix_ms: sample.sample_unix_ms,
            interfaces,
            error: sample.error,
        }
    }
}

#[derive(Clone, Serialize)]
struct NetInterfaceSnapshot {
    name: String,
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
    rx_errors: u64,
    tx_errors: u64,
    rx_dropped: u64,
    tx_dropped: u64,
    rx_bps: Option<f64>,
    tx_bps: Option<f64>,
    rx_pps: Option<f64>,
    tx_pps: Option<f64>,
}

impl NetInterfaceSnapshot {
    fn from_sample(
        sample: NetInterfaceSample,
        previous: Option<&Self>,
        elapsed_secs: Option<f64>,
    ) -> Self {
        let rate = |new: u64, old: u64| {
            elapsed_secs.and_then(|elapsed| {
                new.checked_sub(old)
                    .map(|difference| difference as f64 / elapsed)
            })
        };
        let (rx_bps, tx_bps, rx_pps, tx_pps) =
            previous.map_or((None, None, None, None), |previous| {
                (
                    rate(sample.rx_bytes, previous.rx_bytes),
                    rate(sample.tx_bytes, previous.tx_bytes),
                    rate(sample.rx_packets, previous.rx_packets),
                    rate(sample.tx_packets, previous.tx_packets),
                )
            });

        Self {
            name: sample.name,
            rx_bytes: sample.rx_bytes,
            tx_bytes: sample.tx_bytes,
            rx_packets: sample.rx_packets,
            tx_packets: sample.tx_packets,
            rx_errors: sample.rx_errors,
            tx_errors: sample.tx_errors,
            rx_dropped: sample.rx_dropped,
            tx_dropped: sample.tx_dropped,
            rx_bps,
            tx_bps,
            rx_pps,
            tx_pps,
        }
    }
}

#[derive(Clone, Copy)]
struct HardwareSummary {
    sample_unix_ms: Option<u64>,
    cpu_total_percent: Option<f64>,
    gpu_max_percent: Option<u64>,
    gpu_memory_used_mib: u64,
    gpu_memory_total_mib: u64,
    net_rx_bps: f64,
    net_tx_bps: f64,
}

struct HardwareHistoryState {
    at: Instant,
    sample_unix_ms: Option<u64>,
    cpu_total_percent: Option<f64>,
    gpu_max_percent: Option<u64>,
    gpu_memory_used_mib: u64,
    gpu_memory_total_mib: u64,
    net_rx_bps: f64,
    net_tx_bps: f64,
}

#[derive(Default, Serialize)]
struct FleetTotals {
    nodes: u32,
    live_nodes: u32,
    stale_nodes: u32,
    gpu_count: u32,
    cpu_avg_percent: Option<f64>,
    gpu_max_percent: Option<u64>,
    gpu_memory_used_mib: u64,
    gpu_memory_total_mib: u64,
    net_rx_bps: f64,
    net_tx_bps: f64,
    errors: u32,
}

#[derive(Serialize)]
struct HardwareDashboardSnapshot {
    totals: FleetTotals,
    nodes: Vec<NodeHardwareSnapshot>,
}

#[derive(Serialize)]
struct NodeHardwareSnapshot {
    stream: StreamSnapshot,
    live: bool,
    last_seen_ms_ago: u64,
    last_sample_unix_ms: Option<u64>,
    errors: Vec<String>,
    cpu: Option<CpuSnapshot>,
    gpu: Option<GpuSnapshot>,
    net: Option<NetSnapshot>,
    history: Vec<HardwareHistorySnapshot>,
}

#[derive(Serialize)]
struct StreamSnapshot {
    key: String,
    node: String,
    life: u64,
}

#[derive(Serialize)]
struct CpuSnapshot {
    seq: u64,
    sample_unix_ms: u64,
    query_elapsed_ms: Option<u64>,
    host: Option<CpuHostSample>,
    cores: Vec<CpuCoreSample>,
    processes: Vec<CpuProcessSample>,
    error: Option<String>,
}

impl From<&HostCpuSample> for CpuSnapshot {
    fn from(sample: &HostCpuSample) -> Self {
        Self {
            seq: sample.seq,
            sample_unix_ms: sample.sample_unix_ms,
            query_elapsed_ms: sample.query_elapsed_ms,
            host: sample.host.clone(),
            cores: sample.cores.clone(),
            processes: sample.processes.clone(),
            error: sample.error.clone(),
        }
    }
}

#[derive(Serialize)]
struct GpuSnapshot {
    seq: u64,
    sample_unix_ms: u64,
    query_elapsed_ms: Option<u64>,
    gpus: Vec<GpuDeviceSample>,
    processes: Vec<GpuProcessSample>,
    error: Option<String>,
}

impl From<&HostGpuSample> for GpuSnapshot {
    fn from(sample: &HostGpuSample) -> Self {
        Self {
            seq: sample.seq,
            sample_unix_ms: sample.sample_unix_ms,
            query_elapsed_ms: sample.query_elapsed_ms,
            gpus: sample.gpus.clone(),
            processes: sample.processes.clone(),
            error: sample.error.clone(),
        }
    }
}

#[derive(Serialize)]
struct HardwareHistorySnapshot {
    ms_ago: u64,
    sample_unix_ms: Option<u64>,
    cpu_total_percent: Option<f64>,
    gpu_max_percent: Option<u64>,
    gpu_memory_used_mib: u64,
    gpu_memory_total_mib: u64,
    net_rx_bps: f64,
    net_tx_bps: f64,
}

impl DashboardView for HardwareDashboardView {
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
        CHANNELS
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        let now = Instant::now();
        let key = stream_key(&event.stream);
        self.state
            .write()
            .nodes
            .entry(key)
            .or_insert_with(|| NodeHardwareState::new(event.stream.clone(), now))
            .update(&event.channel, &event.payload, now);
    }

    fn snapshot_json(&self) -> Value {
        let now = Instant::now();
        let mut nodes = self
            .state
            .read()
            .nodes
            .values()
            .map(|node| node_snapshot(node, now))
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            right
                .live
                .cmp(&left.live)
                .then_with(|| left.stream.node.cmp(&right.stream.node))
                .then_with(|| left.stream.life.cmp(&right.stream.life))
        });
        let snapshot = HardwareDashboardSnapshot {
            totals: fleet_totals(&nodes),
            nodes,
        };
        serde_json::to_value(snapshot).unwrap_or_else(|_| {
            json!({
                "totals": FleetTotals::default(),
                "nodes": []
            })
        })
    }

    fn html(&self) -> Option<&'static str> {
        Some(HARDWARE_HTML)
    }
}

fn node_snapshot(node: &NodeHardwareState, now: Instant) -> NodeHardwareSnapshot {
    let summary = node.summary();
    NodeHardwareSnapshot {
        stream: StreamSnapshot {
            key: stream_key(&node.stream),
            node: node.stream.node.clone(),
            life: node.stream.life,
        },
        live: now.duration_since(node.last_seen) <= LIVE_TTL,
        last_seen_ms_ago: duration_ms(now.duration_since(node.last_seen)),
        last_sample_unix_ms: summary.sample_unix_ms,
        errors: node.errors(),
        cpu: node.cpu.as_ref().map(CpuSnapshot::from),
        gpu: node.gpu.as_ref().map(GpuSnapshot::from),
        net: node.net.clone(),
        history: node
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
    }
}

fn fleet_totals(nodes: &[NodeHardwareSnapshot]) -> FleetTotals {
    let mut totals = FleetTotals {
        nodes: saturating_u32(nodes.len()),
        ..FleetTotals::default()
    };
    let mut cpu_total = 0.0;
    let mut cpu_count = 0_u32;

    for node in nodes {
        if node.live {
            totals.live_nodes = totals.live_nodes.saturating_add(1);
        } else {
            totals.stale_nodes = totals.stale_nodes.saturating_add(1);
        }
        totals.errors = totals
            .errors
            .saturating_add(saturating_u32(node.errors.len()));

        if let Some(cpu_percent) = node
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.host.as_ref())
            .and_then(|host| host.total_percent)
        {
            cpu_total += cpu_percent;
            cpu_count = cpu_count.saturating_add(1);
        }

        if let Some(gpu) = &node.gpu {
            totals.gpu_count = totals
                .gpu_count
                .saturating_add(saturating_u32(gpu.gpus.len()));
            for device in &gpu.gpus {
                if let Some(percent) = device.utilization_gpu_percent {
                    totals.gpu_max_percent = Some(
                        totals
                            .gpu_max_percent
                            .map_or(percent, |current| current.max(percent)),
                    );
                }
                totals.gpu_memory_used_mib = totals
                    .gpu_memory_used_mib
                    .saturating_add(device.memory_used_mib.unwrap_or_default());
                totals.gpu_memory_total_mib = totals
                    .gpu_memory_total_mib
                    .saturating_add(device.memory_total_mib.unwrap_or_default());
            }
        }

        if let Some(net) = &node.net {
            for interface in &net.interfaces {
                totals.net_rx_bps += interface.rx_bps.unwrap_or_default();
                totals.net_tx_bps += interface.tx_bps.unwrap_or_default();
            }
        }
    }

    if cpu_count > 0 {
        totals.cpu_avg_percent = Some(cpu_total / f64::from(cpu_count));
    }
    totals
}

fn stream_key(stream: &StreamEvent) -> String {
    format!("{}#{}", stream.node, stream.life)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry::frame::{ChannelId, Lifetime, NodeId, Position};
    use telemetry::hardware::gpu::GpuDeviceSample;

    #[test]
    fn hardware_view_groups_samples_by_node_and_derives_network_rates() {
        let view = HardwareDashboardView::default();
        let node_a = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        let node_b = StreamId::new(NodeId::new("node-b"), Lifetime(1));

        ingest_record(&view, &node_a, Position(0), &cpu_sample(1, 1_000, 25.0));
        ingest_record(&view, &node_a, Position(1), &gpu_sample(1, 1_000));
        ingest_record(
            &view,
            &node_a,
            Position(2),
            &net_sample(1, 1_000, 1_000, 2_000, 10, 20),
        );
        ingest_record(
            &view,
            &node_a,
            Position(3),
            &net_sample(2, 2_000, 3_048, 6_096, 12, 24),
        );
        ingest_record(&view, &node_b, Position(0), &cpu_sample(1, 1_000, 75.0));

        let snapshot = view.snapshot_json();
        let nodes = snapshot["nodes"].as_array().expect("nodes array");
        assert_eq!(nodes.len(), 2);
        assert_eq!(snapshot["totals"]["nodes"].as_u64(), Some(2));

        let node_a = nodes
            .iter()
            .find(|node| node["stream"]["key"] == "node-a#1")
            .expect("node-a snapshot");
        let interface = &node_a["net"]["interfaces"][0];
        assert_eq!(interface["rx_bps"].as_f64(), Some(2_048.0));
        assert_eq!(interface["tx_bps"].as_f64(), Some(4_096.0));
        assert_eq!(interface["rx_pps"].as_f64(), Some(2.0));
        assert_eq!(interface["tx_pps"].as_f64(), Some(4.0));
        assert!(
            !node_a["history"]
                .as_array()
                .expect("history array")
                .is_empty()
        );
    }

    #[test]
    fn hardware_view_reports_decode_errors_without_dropping_other_state() {
        let view = HardwareDashboardView::default();
        let stream = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        ingest_record(&view, &stream, Position(0), &cpu_sample(1, 1_000, 25.0));
        let invalid_gpu = Frame::new(ChannelId(2), Position(1), b"not valid json".to_vec());
        let event = FrameEvent {
            stream: StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
            },
            channel: HOST_GPU_CHANNEL.to_owned(),
            position: invalid_gpu.position.0,
            payload: invalid_gpu.payload.clone(),
        };
        view.ingest(&stream, &invalid_gpu, &event);

        let snapshot = view.snapshot_json();
        let node = &snapshot["nodes"][0];
        assert!(node["cpu"].is_object());
        assert!(
            node["errors"]
                .as_array()
                .expect("errors array")
                .iter()
                .filter_map(Value::as_str)
                .any(|error| error.starts_with("host.gpu decode error:"))
        );
    }

    #[test]
    fn hardware_view_subscribes_to_all_channels() {
        assert!(HardwareDashboardView::default().channels().is_empty());
    }

    #[test]
    fn fleet_tracks_non_hardware_frames_without_fabricating_hardware() {
        let view = HardwareDashboardView::default();
        let stream = StreamId::new(NodeId::new("runtime-only"), Lifetime(3));
        let frame = Frame::new(ChannelId(9), Position(0), br#"{"kind":"ready"}"#.to_vec());
        let event = FrameEvent {
            stream: StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
            },
            channel: "mvp.lifecycle".to_owned(),
            position: frame.position.0,
            payload: frame.payload.clone(),
        };
        view.ingest(&stream, &frame, &event);

        let snapshot = view.snapshot_json();
        let nodes = snapshot["nodes"].as_array().expect("nodes array");
        assert_eq!(nodes.len(), 1);
        let node = &nodes[0];
        assert_eq!(node["stream"]["key"], "runtime-only#3");
        assert_eq!(node["live"].as_bool(), Some(true));
        assert!(node["cpu"].is_null());
        assert!(node["gpu"].is_null());
        assert!(node["net"].is_null());
        assert!(node["last_sample_unix_ms"].is_null());
        assert!(
            node["history"]
                .as_array()
                .expect("history array")
                .is_empty()
        );
        assert!(node["errors"].as_array().expect("errors array").is_empty());
    }

    #[test]
    fn hardware_html_links_to_fleet_view_and_api() {
        let view = HardwareDashboardView::default();
        assert_eq!(view.id(), "fleet");
        assert_eq!(view.path(), "fleet");
        assert_eq!(view.title(), "Fleet");
        assert!(HARDWARE_HTML.contains("/view/fleet"));
        assert!(HARDWARE_HTML.contains("/api/view/fleet"));
    }

    fn ingest_record<R: Record>(
        view: &HardwareDashboardView,
        stream: &StreamId,
        position: Position,
        record: &R,
    ) {
        let frame = Frame::new(ChannelId(1), position, record.encode());
        let event = FrameEvent {
            stream: StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
            },
            channel: R::channel_name().to_owned(),
            position: frame.position.0,
            payload: frame.payload.clone(),
        };
        view.ingest(stream, &frame, &event);
    }

    fn cpu_sample(seq: u64, sample_unix_ms: u64, total_percent: f64) -> HostCpuSample {
        HostCpuSample {
            schema: "host.cpu.v1".to_string(),
            seq,
            sample_unix_ms,
            query_elapsed_ms: Some(1),
            host: Some(CpuHostSample {
                logical_cpus: 1,
                total_percent: Some(total_percent),
                idle_percent: Some(100.0 - total_percent),
                iowait_percent: Some(0.0),
                steal_percent: Some(0.0),
                load1: Some(0.5),
                load5: Some(0.4),
                load15: Some(0.3),
            }),
            cores: Vec::new(),
            processes: Vec::new(),
            error: None,
        }
    }

    fn gpu_sample(seq: u64, sample_unix_ms: u64) -> HostGpuSample {
        HostGpuSample {
            schema: "host.gpu.v1".to_string(),
            seq,
            sample_unix_ms,
            query_elapsed_ms: Some(2),
            gpus: vec![GpuDeviceSample {
                index: Some(0),
                uuid: "GPU-test".to_string(),
                name: "Test GPU".to_string(),
                memory_used_mib: Some(1_024),
                memory_total_mib: Some(8_192),
                utilization_gpu_percent: Some(50),
                utilization_memory_percent: Some(12),
                temperature_c: Some(55),
                power_draw_w: Some(100.0),
            }],
            processes: Vec::new(),
            error: None,
        }
    }

    fn net_sample(
        seq: u64,
        sample_unix_ms: u64,
        rx_bytes: u64,
        tx_bytes: u64,
        rx_packets: u64,
        tx_packets: u64,
    ) -> HostNetSample {
        HostNetSample {
            schema: "host.net.v1".to_string(),
            seq,
            sample_unix_ms,
            interfaces: vec![NetInterfaceSample {
                name: "eth0".to_string(),
                rx_bytes,
                tx_bytes,
                rx_packets,
                tx_packets,
                rx_errors: 0,
                tx_errors: 0,
                rx_dropped: 0,
                tx_dropped: 0,
            }],
            error: None,
        }
    }
}

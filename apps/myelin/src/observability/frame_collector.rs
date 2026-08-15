//! Frame collection and load-progress extraction for the control loop.
//!
//! [`FrameCollector`] wraps the mpsc channel that buffers telemetry frames
//! drained from the iroh driver. It exposes two drain methods that forward
//! queued frames to sinks (dashboard/archive) via closures; the control loop
//! never names [`Frame`] or [`TelemetryEvent`] directly — frames reach sinks
//! only through these closures. Load-progress extraction
//! ([`StageLoadProgress`]) is co-located here because it is the one legitimate
//! read of telemetry content for control decisions (weight-load liveness).

use telemetry::frame::{ChannelRef, TelemetryEvent, Frame, StreamId};
use iroh_driver::IrohDriver;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::Instant;

use crate::observability::orch_telemetry::DashboardSupport;

/// A telemetry frame queued from a remote node, with its resolved channel name.
#[derive(Clone, Debug)]
struct CollectedTelemetryFrame {
    stream: StreamId,
    channel_name: String,
    frame: Frame,
}

/// Per-node load progress distilled from telemetry frames (control input).
#[derive(Clone, Debug, Default)]
pub(crate) struct StageLoadProgress {
    pub(crate) node_id: u64,
    pub(crate) stage_index: Option<u32>,
    pub(crate) phase: Option<String>,
    pub(crate) bytes_done: Option<u64>,
    pub(crate) bytes_total: Option<u64>,
    pub(crate) last_progress: Option<Instant>,
    pub(crate) last_worker_event: Option<String>,
    pub(crate) failure_reason: Option<String>,
    pub(crate) host_gpu_samples: u64,
}

impl StageLoadProgress {
    pub(crate) fn to_json(&self) -> Value {
        serde_json::json!({
            "node_id": self.node_id,
            "stage_index": self.stage_index,
            "phase": self.phase.as_deref().unwrap_or("unknown"),
            "bytes_done": self.bytes_done,
            "bytes_total": self.bytes_total,
            "last_progress_age_ms": self.last_progress.map(|at| at.elapsed().as_millis()),
            "last_worker_event": self.last_worker_event,
            "failure_reason": self.failure_reason,
            "host_gpu_samples": self.host_gpu_samples,
            "host_gpu_missing": self.host_gpu_samples == 0,
        })
    }
}

/// Buffers telemetry frames drained from the driver and forwards them to sinks.
pub(crate) struct FrameCollector {
    tx: mpsc::Sender<CollectedTelemetryFrame>,
    rx: mpsc::Receiver<CollectedTelemetryFrame>,
}

impl FrameCollector {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self { tx, rx }
    }

    /// Drain iroh telemetry connections into the internal queue.
    pub(crate) fn pump(&self, driver: &IrohDriver) {
        drain_telemetry_connections(driver, &self.tx);
    }

    /// Drain queued frames, forwarding each via the closure. No progress extraction.
    pub(crate) fn drain<F>(&self, forward: F)
    where
        F: FnMut(&StreamId, &str, &Frame),
    {
        let mut forward = forward;
        while let Ok(collected) = self.rx.try_recv() {
            forward(&collected.stream, &collected.channel_name, &collected.frame);
        }
    }

    /// Drain queued frames, extracting load progress and forwarding each via the closure.
    pub(crate) fn drain_with_progress<F>(
        &self,
        progress: &mut BTreeMap<u64, StageLoadProgress>,
        forward: F,
    ) where
        F: FnMut(&StreamId, &str, &Frame),
    {
        let mut forward = forward;
        let now = Instant::now();
        while let Ok(collected) = self.rx.try_recv() {
            update_load_progress_from_frame(progress, &collected, now);
            forward(&collected.stream, &collected.channel_name, &collected.frame);
        }
    }
}

fn update_load_progress_from_frame(
    progress: &mut BTreeMap<u64, StageLoadProgress>,
    collected: &CollectedTelemetryFrame,
    now: Instant,
) {
    let stream_node_id = collected.stream.node.as_str().parse::<u64>().ok();
    if collected.channel_name == "host.gpu" {
        if let Some(node_id) = stream_node_id {
            let entry = progress
                .entry(node_id)
                .or_insert_with(|| StageLoadProgress {
                    node_id,
                    ..StageLoadProgress::default()
                });
            entry.host_gpu_samples = entry.host_gpu_samples.saturating_add(1);
        }
        return;
    }

    let Ok(value) = serde_json::from_slice::<Value>(&collected.frame.payload) else {
        return;
    };
    if value.get("type").and_then(Value::as_str) == Some("NodeEvent") {
        update_load_progress_from_node_event(progress, &value, now);
        return;
    }
    if collected.channel_name == "myelin.worker.weights" {
        let Some(node_id) = stream_node_id else {
            return;
        };
        update_load_progress_from_worker_event(progress, node_id, None, &value, now);
    }
}

fn update_load_progress_from_node_event(
    progress: &mut BTreeMap<u64, StageLoadProgress>,
    value: &Value,
    now: Instant,
) {
    let Some(node_id) = numeric_json_field(value, "node_id") else {
        return;
    };
    let stage_index =
        numeric_json_field(value, "stage_index").and_then(|stage| u32::try_from(stage).ok());
    let phase = value.get("phase").and_then(Value::as_str);
    let status = value.get("status").and_then(Value::as_str);
    let detail = value.get("detail").unwrap_or(&Value::Null);
    if phase == Some("load_weights") {
        let load_phase = match status {
            Some("started") => Some("loading_weights"),
            Some("ready") => Some("weights_loaded"),
            Some("failed") => Some("failed"),
            _ => None,
        };
        if let Some(load_phase) = load_phase {
            let entry = progress
                .entry(node_id)
                .or_insert_with(|| StageLoadProgress {
                    node_id,
                    ..StageLoadProgress::default()
                });
            entry.stage_index = stage_index.or(entry.stage_index);
            entry.phase = Some(load_phase.to_owned());
            entry.last_progress = Some(now);
            if status == Some("failed") {
                entry.failure_reason = detail
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
        }
    }
    if let Some(worker_event) = detail.get("event") {
        update_load_progress_from_worker_event(progress, node_id, stage_index, worker_event, now);
    }
}

fn update_load_progress_from_worker_event(
    progress: &mut BTreeMap<u64, StageLoadProgress>,
    node_id: u64,
    stage_index: Option<u32>,
    event: &Value,
    now: Instant,
) {
    let Some(event_type) = event.get("type").and_then(Value::as_str) else {
        return;
    };
    let Some(phase) = load_phase_for_worker_event(event_type) else {
        return;
    };
    let entry = progress
        .entry(node_id)
        .or_insert_with(|| StageLoadProgress {
            node_id,
            ..StageLoadProgress::default()
        });
    entry.stage_index = stage_index.or(entry.stage_index);
    entry.phase = Some(phase.to_owned());
    entry.last_worker_event = Some(event_type.to_owned());
    entry.last_progress = Some(now);
    if let Some(bytes_done) =
        numeric_json_field(event, "bytes_done").or_else(|| numeric_json_field(event, "bytes"))
    {
        entry.bytes_done = Some(bytes_done);
    }
    if let Some(bytes_total) = numeric_json_field(event, "bytes_total") {
        entry.bytes_total = Some(bytes_total);
    }
}

fn numeric_json_field(value: &Value, field: &str) -> Option<u64> {
    value
        .get(field)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn load_phase_for_worker_event(event_type: &str) -> Option<&'static str> {
    match event_type {
        "GgufDownloadStarted" | "GgufDownloadProgress" => Some("prefetching_model"),
        "GgufCacheReady" => Some("cache_ready"),
        "StageShardFetchStarted"
        | "StageShardRangeFetchStarted"
        | "StageShardRangeFetchReady"
        | "StageShardTensorFetchStarted"
        | "StageShardTensorFetchReady" => Some("fetching_stage_shard"),
        "StageShardCacheReady" => Some("stage_shard_cache_ready"),
        "StageShardReady" => Some("stage_shard_ready"),
        "StageShardFetchFailed" => Some("failed"),
        "PipelineStageFromGgufStarted" => Some("constructing_stage"),
        "PipelineStageFromGgufReady" => Some("stage_constructed"),
        "TokenizerBuildStarted" => Some("building_tokenizer"),
        "TokenizerBuildReady" => Some("tokenizer_ready"),
        "WeightsLoaded" => Some("weights_loaded"),
        "WorkerFatal" => Some("failed"),
        _ => None,
    }
}

fn drain_telemetry_connections(
    driver: &IrohDriver,
    frame_tx: &mpsc::Sender<CollectedTelemetryFrame>,
) {
    for read in driver.drain_telemetry_reads() {
        let mut channels = read
            .header
            .channels
            .iter()
            .map(|descriptor| {
                (
                    ChannelRef {
                        stream: descriptor.stream.clone(),
                        channel: descriptor.id,
                    },
                    descriptor.name.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for event in read.events {
            match event {
                TelemetryEvent::ChannelDeclared(descriptor) => {
                    channels.insert(
                        ChannelRef {
                            stream: descriptor.stream.clone(),
                            channel: descriptor.id,
                        },
                        descriptor.name,
                    );
                }
                TelemetryEvent::Frame(delivery) => {
                    let channel_name = channels
                        .get(&delivery.channel)
                        .cloned()
                        .unwrap_or_else(|| format!("channel#{}", delivery.channel.channel.0));
                    let frame = Frame::new(
                        delivery.channel.channel,
                        delivery.position,
                        delivery.payload,
                    );
                    if frame_tx
                        .send(CollectedTelemetryFrame {
                            stream: delivery.channel.stream,
                            channel_name,
                            frame,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                TelemetryEvent::StreamDeclared(_) | TelemetryEvent::StreamEnded(_) => {}
            }
        }
    }
}

/// Publish a frame to the dashboard, if one is attached. Used by the producer
/// flush path as well as the control-loop drain closures.
pub(crate) fn ingest_dashboard_frame(
    dashboard: Option<&DashboardSupport>,
    stream: &StreamId,
    channel: &str,
    frame: &Frame,
) {
    if let Some(dashboard) = dashboard {
        dashboard.publish_frame(stream, channel, frame);
    }
}

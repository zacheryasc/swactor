use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde_json::{Value, json};
use telemetry::frame::{Frame, StreamId};

use crate::FrameEvent;
use crate::view::DashboardView;

const LIVE_EXPLORER_HTML: &str = include_str!("live_explorer_page.html");
const FRAME_HISTORY_CAP: usize = 500;
const SNAPSHOT_FRAME_CAP: usize = 500;
const SNAPSHOT_PAYLOAD_BYTE_CAP: usize = 256 * 1024;
const LIVE_TTL: Duration = Duration::from_secs(8);
const STALE_STREAM_CAP: usize = 50;

#[derive(Default)]
pub struct LiveTelemetryExplorer {
    state: RwLock<ExplorerState>,
}

#[derive(Default)]
struct ExplorerState {
    streams: BTreeMap<(String, u64), ExplorerStream>,
}

struct ExplorerStream {
    last_seen: Instant,
    channels: BTreeMap<String, VecDeque<FrameEvent>>,
}

impl LiveTelemetryExplorer {
    fn ingest_at(&self, event: &FrameEvent, now: Instant) {
        let mut state = self.state.write();
        let newest_life = state
            .streams
            .keys()
            .filter(|(node, _)| node == &event.stream.node)
            .map(|(_, life)| *life)
            .max();
        if newest_life.is_some_and(|life| event.stream.life < life) {
            return;
        }
        if newest_life.is_none_or(|life| event.stream.life > life) {
            state
                .streams
                .retain(|(node, _), _| node != &event.stream.node);
        }

        let stream = state
            .streams
            .entry((event.stream.node.clone(), event.stream.life))
            .or_insert_with(|| ExplorerStream {
                last_seen: now,
                channels: BTreeMap::new(),
            });
        stream.last_seen = now;
        let frames = stream.channels.entry(event.channel.clone()).or_default();
        if frames.len() >= FRAME_HISTORY_CAP {
            frames.pop_front();
        }
        frames.push_back(event.clone());
        prune_stale(&mut state.streams, now);
    }

    fn snapshot_at(&self, now: Instant) -> Value {
        let mut state = self.state.write();
        prune_stale(&mut state.streams, now);

        // Take recent frames round-robin across channels so a busy channel
        // cannot crowd quiet channels out of the bounded bootstrap snapshot.
        let mut channels = state
            .streams
            .values()
            .flat_map(|stream| stream.channels.values())
            .map(|frames| frames.iter().rev())
            .collect::<Vec<_>>();
        let mut frames = Vec::with_capacity(SNAPSHOT_FRAME_CAP.min(channels.len()));
        let mut payload_bytes = 0;
        'snapshot: loop {
            let mut found_frame = false;
            for channel in &mut channels {
                let Some(frame) = channel.next() else {
                    continue;
                };
                found_frame = true;
                if frame.payload.len() > SNAPSHOT_PAYLOAD_BYTE_CAP - payload_bytes {
                    continue;
                }
                payload_bytes += frame.payload.len();
                frames.push(frame.clone());
                if frames.len() == SNAPSHOT_FRAME_CAP {
                    break 'snapshot;
                }
            }
            if !found_frame || payload_bytes == SNAPSHOT_PAYLOAD_BYTE_CAP {
                break;
            }
        }
        drop(state);

        frames.sort_by(|left, right| {
            left.stream
                .node
                .cmp(&right.stream.node)
                .then_with(|| left.stream.life.cmp(&right.stream.life))
                .then_with(|| left.position.cmp(&right.position))
        });
        json!({ "frames": frames })
    }
}

fn prune_stale(streams: &mut BTreeMap<(String, u64), ExplorerStream>, now: Instant) {
    let mut stale = streams
        .iter()
        .filter(|(_, stream)| now.duration_since(stream.last_seen) > LIVE_TTL)
        .map(|(key, stream)| (key.clone(), stream.last_seen))
        .collect::<Vec<_>>();
    if stale.len() <= STALE_STREAM_CAP {
        return;
    }
    let excess = stale.len() - STALE_STREAM_CAP;
    stale.sort_by_key(|(_, last_seen)| *last_seen);
    for (key, _) in stale.into_iter().take(excess) {
        streams.remove(&key);
    }
}

impl DashboardView for LiveTelemetryExplorer {
    fn id(&self) -> &'static str {
        "telemetry/live"
    }

    fn title(&self) -> &'static str {
        "Telemetry live explorer"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[]
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        self.ingest_at(event, Instant::now());
    }

    fn snapshot_json(&self) -> Value {
        self.snapshot_at(Instant::now())
    }

    fn html(&self) -> Option<&'static str> {
        Some(LIVE_EXPLORER_HTML)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry::frame::{ChannelId, Lifetime, NodeId, Position};

    #[test]
    fn retains_quiet_channels_and_bounds_each_channel_independently() {
        let view = LiveTelemetryExplorer::default();
        let stream = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        ingest(&view, &stream, "quiet", 0);

        for position in 0..=FRAME_HISTORY_CAP as u64 {
            ingest(&view, &stream, "busy", position);
        }
        {
            let state = view.state.read();
            let busy = &state.streams.values().next().expect("stream").channels["busy"];
            assert_eq!(busy.len(), FRAME_HISTORY_CAP);
            assert_eq!(busy.front().map(|frame| frame.position), Some(1));
            assert_eq!(
                busy.back().map(|frame| frame.position),
                Some(FRAME_HISTORY_CAP as u64)
            );
        }

        let snapshot = view.snapshot_json();
        let frames = snapshot["frames"].as_array().expect("frames array");
        let quiet = frames
            .iter()
            .filter(|frame| frame["channel"] == "quiet")
            .collect::<Vec<_>>();
        let busy = frames
            .iter()
            .filter(|frame| frame["channel"] == "busy")
            .collect::<Vec<_>>();

        assert_eq!(quiet.len(), 1, "a quiet channel remains discoverable");
        assert_eq!(busy.len(), SNAPSHOT_FRAME_CAP - quiet.len());
        assert_eq!(
            busy.last().and_then(|frame| frame["position"].as_u64()),
            Some(FRAME_HISTORY_CAP as u64)
        );
    }

    #[test]
    fn newer_lifetime_evicts_older_lifetime_and_rejects_late_frames() {
        let view = LiveTelemetryExplorer::default();
        let first = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        let second = StreamId::new(NodeId::new("node-a"), Lifetime(2));
        ingest(&view, &first, "host.cpu", 7);
        ingest(&view, &second, "host.cpu", 0);
        ingest(&view, &first, "host.cpu", 8);

        let snapshot = view.snapshot_json();
        let frames = snapshot["frames"].as_array().expect("frames array");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["stream"]["life"].as_u64(), Some(2));
        assert_eq!(frames[0]["position"].as_u64(), Some(0));
    }

    #[test]
    fn retains_all_live_streams_and_only_fifty_stale_streams() {
        let view = LiveTelemetryExplorer::default();
        let start = Instant::now();
        for node in 0..60 {
            let stream = StreamId::new(NodeId::new(format!("node-{node}")), Lifetime(1));
            let frame = test_frame(node);
            let event = test_event(&stream, "host.cpu", node, &frame);
            view.ingest_at(&event, start + Duration::from_millis(node));
        }

        let snapshot = view.snapshot_at(start + LIVE_TTL + Duration::from_secs(1));
        let streams = snapshot["frames"]
            .as_array()
            .expect("frames array")
            .iter()
            .map(|frame| frame["stream"]["node"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(streams.len(), STALE_STREAM_CAP);
        assert!(!streams.contains("node-0"));
        assert!(streams.contains("node-59"));
    }

    #[test]
    fn snapshot_payload_is_bounded() {
        let view = LiveTelemetryExplorer::default();
        let stream = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        let payload = vec![7; SNAPSHOT_PAYLOAD_BYTE_CAP / 2];
        for (position, channel) in ["first", "second", "third"].into_iter().enumerate() {
            let event = FrameEvent {
                stream: crate::StreamEvent {
                    node: stream.node.as_str().to_owned(),
                    life: stream.life.0,
                    origin: None,
                    label: None,
                },
                channel: channel.to_owned(),
                position: position as u64,
                payload: payload.clone(),
            };
            view.ingest_at(&event, Instant::now());
        }

        let snapshot = view.snapshot_json();
        let frames = snapshot["frames"].as_array().expect("frames array");
        let payload_bytes = frames
            .iter()
            .map(|frame| frame["payload"].as_array().expect("payload").len())
            .sum::<usize>();
        assert!(frames.len() <= SNAPSHOT_FRAME_CAP);
        assert!(payload_bytes <= SNAPSHOT_PAYLOAD_BYTE_CAP);
        assert_eq!(frames.len(), 2, "the payload byte cap bounds the snapshot");
    }

    fn test_frame(position: u64) -> Frame {
        Frame::new(
            ChannelId(position as u32 + 1),
            Position(position),
            position.to_le_bytes().to_vec(),
        )
    }

    fn test_event(stream: &StreamId, channel: &str, position: u64, frame: &Frame) -> FrameEvent {
        FrameEvent {
            stream: crate::StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
                origin: None,
                label: None,
            },
            channel: channel.to_owned(),
            position,
            payload: frame.payload.clone(),
        }
    }

    fn ingest(view: &LiveTelemetryExplorer, stream: &StreamId, channel: &str, position: u64) {
        let frame = test_frame(position);
        let event = test_event(stream, channel, position, &frame);
        view.ingest(stream, &frame, &event);
    }
}

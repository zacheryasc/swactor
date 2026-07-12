use std::collections::{BTreeMap, VecDeque};

use datastream::frame::{Frame, StreamId};
use parking_lot::RwLock;
use serde_json::{Value, json};

use crate::FrameEvent;
use crate::view::DashboardView;

const LIVE_EXPLORER_HTML: &str = include_str!("live_explorer_page.html");

const FRAME_HISTORY_CAP: usize = 500;

#[derive(Default)]
pub struct LiveDatastreamExplorer {
    state: RwLock<BTreeMap<(String, u64, String), VecDeque<FrameEvent>>>,
}

impl DashboardView for LiveDatastreamExplorer {
    fn id(&self) -> &'static str {
        "datastream/live"
    }

    fn title(&self) -> &'static str {
        "Datastream live explorer"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[]
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        let key = (
            event.stream.node.clone(),
            event.stream.life,
            event.channel.clone(),
        );
        let mut state = self.state.write();
        let frames = state.entry(key).or_default();
        if frames.len() >= FRAME_HISTORY_CAP {
            frames.pop_front();
        }
        frames.push_back(event.clone());
    }

    fn snapshot_json(&self) -> Value {
        let state = self.state.read();
        let mut frames = state
            .values()
            .flat_map(|frames| frames.iter())
            .collect::<Vec<_>>();
        frames.sort_by(|left, right| {
            left.stream
                .node
                .cmp(&right.stream.node)
                .then_with(|| left.stream.life.cmp(&right.stream.life))
                .then_with(|| left.position.cmp(&right.position))
        });

        json!({ "frames": frames })
    }

    fn html(&self) -> Option<&'static str> {
        Some(LIVE_EXPLORER_HTML)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream::frame::{ChannelId, Lifetime, NodeId, Position};

    #[test]
    fn retains_quiet_channels_and_bounds_each_channel_independently() {
        let view = LiveDatastreamExplorer::default();
        let stream = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        ingest(&view, &stream, "quiet", 0);

        for position in 0..=FRAME_HISTORY_CAP as u64 {
            ingest(&view, &stream, "busy", position);
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
        assert_eq!(busy.len(), FRAME_HISTORY_CAP);
        assert_eq!(
            busy.first().and_then(|frame| frame["position"].as_u64()),
            Some(1)
        );
        assert_eq!(
            busy.last().and_then(|frame| frame["position"].as_u64()),
            Some(FRAME_HISTORY_CAP as u64)
        );
    }

    #[test]
    fn retains_same_channel_separately_across_stream_lifetimes() {
        let view = LiveDatastreamExplorer::default();
        let first = StreamId::new(NodeId::new("node-a"), Lifetime(1));
        let second = StreamId::new(NodeId::new("node-a"), Lifetime(2));
        ingest(&view, &first, "host.cpu", 7);
        ingest(&view, &second, "host.cpu", 0);

        let snapshot = view.snapshot_json();
        let frames = snapshot["frames"].as_array().expect("frames array");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["stream"]["life"].as_u64(), Some(1));
        assert_eq!(frames[0]["position"].as_u64(), Some(7));
        assert_eq!(frames[1]["stream"]["life"].as_u64(), Some(2));
        assert_eq!(frames[1]["position"].as_u64(), Some(0));
    }

    fn ingest(view: &LiveDatastreamExplorer, stream: &StreamId, channel: &str, position: u64) {
        let frame = Frame::new(
            ChannelId(position as u32 + 1),
            Position(position),
            position.to_le_bytes().to_vec(),
        );
        let event = FrameEvent {
            stream: crate::StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
            },
            channel: channel.to_owned(),
            position,
            payload: frame.payload.clone(),
        };
        view.ingest(stream, &frame, &event);
    }
}

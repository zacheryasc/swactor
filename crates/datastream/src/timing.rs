//! Optional frame-construction timing sidecar records.

use serde::{Deserialize, Serialize};

use crate::frame::ChannelId;

use crate::frame::Position;
use crate::record::Record;

/// Reserved channel carrying optional timing samples for frames in the same stream.
pub const FRAME_TIME_CHANNEL: &str = "datastream.frame_time";
pub const FRAME_TIME_CHANNEL_ID: ChannelId = ChannelId(0);

/// Sidecar timing sample keyed by the target frame's stream-local position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameTimeSample {
    pub target_position: u64,
    pub created_at_unix_ns: u64,
}

impl FrameTimeSample {
    pub fn new(target_position: Position, created_at_unix_ns: u64) -> Self {
        Self {
            target_position: target_position.0,
            created_at_unix_ns,
        }
    }
}

impl Record for FrameTimeSample {
    const CHANNEL: &'static str = FRAME_TIME_CHANNEL;
}

//! Views: read-time projections over a stored stream (spec §9).

use std::fmt;

use super::frame::{ChannelId, Frame, Position};
use super::record::{ChannelClassifier, ChannelKind, Record};
use super::store::{GapSpan, StoredStream};

/// One entry on the merged timeline: either a frame or a surfaced gap.
#[derive(Debug, Clone, PartialEq)]
pub enum LogEntry {
    /// A stored frame, its payload decoded for display.
    Frame(MergedFrame),
    /// A run of positions that were assigned but never delivered.
    Gap(GapSpan),
}

/// A frame as the merged log presents it.
#[derive(Debug, Clone, PartialEq)]
pub struct MergedFrame {
    /// The frame's position on the node's single timeline.
    pub position: Position,
    /// The stream-local numeric channel id.
    pub channel: ChannelId,
    /// The payload, decoded per caller metadata or degraded to bytes.
    pub body: Body,
}

/// A payload decoded as far as the caller's classifier allows.
#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    /// A typed channel decoded to a structured value.
    Record(serde_json::Value),
    /// A raw-text channel as its line(s).
    Text(String),
    /// Unknown or invalid payload bytes.
    Raw(Vec<u8>),
}

/// Decode a payload for display with a caller-owned name classifier.
pub fn decode_body_with<C: ChannelClassifier + ?Sized>(
    channel_name: &str,
    payload: &[u8],
    classifier: &C,
) -> Body {
    match classifier.classify(channel_name) {
        ChannelKind::Typed => match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(value) => Body::Record(value),
            Err(_) => Body::Raw(payload.to_vec()),
        },
        ChannelKind::Text => match std::str::from_utf8(payload) {
            Ok(text) => Body::Text(text.to_string()),
            Err(_) => Body::Raw(payload.to_vec()),
        },
        ChannelKind::Opaque => Body::Raw(payload.to_vec()),
    }
}

/// Decode a payload with no caller registry. Unknown/raw is the safe default.
pub fn decode_body(_channel: &ChannelId, payload: &[u8]) -> Body {
    Body::Raw(payload.to_vec())
}

fn timeline_with_resolver<C, R>(
    stream: &StoredStream,
    classifier: &C,
    resolve_name: R,
) -> Vec<LogEntry>
where
    C: ChannelClassifier + ?Sized,
    R: Fn(ChannelId) -> Option<String>,
{
    let mut out = Vec::with_capacity(stream.len());
    let mut prev: Option<u64> = None;
    for frame in stream.frames() {
        let pos = frame.position.0;
        if let Some(p) = prev
            && pos > p + 1
        {
            out.push(LogEntry::Gap(GapSpan {
                start: p + 1,
                end: pos - 1,
            }));
        }
        let body = match resolve_name(frame.channel) {
            Some(name) => decode_body_with(&name, &frame.payload, classifier),
            None => Body::Raw(frame.payload.clone()),
        };
        out.push(LogEntry::Frame(MergedFrame {
            position: frame.position,
            channel: frame.channel,
            body,
        }));
        prev = Some(pos);
    }
    out
}

/// Full merged log view with raw payload bodies.
pub fn merged_log(stream: &StoredStream) -> Vec<LogEntry> {
    timeline_with_resolver(stream, &|_: &str| ChannelKind::Opaque, |_| None)
}

/// Full merged log using a caller-owned classifier and name resolver.
pub fn merged_log_with_names<C, R>(
    stream: &StoredStream,
    classifier: &C,
    resolve_name: R,
) -> Vec<LogEntry>
where
    C: ChannelClassifier + ?Sized,
    R: Fn(ChannelId) -> Option<String>,
{
    timeline_with_resolver(stream, classifier, resolve_name)
}

/// Transitional merged log using numeric channel ids as strings for classifier lookup.
pub fn merged_log_with<C: ChannelClassifier + ?Sized>(
    stream: &StoredStream,
    classifier: &C,
) -> Vec<LogEntry> {
    timeline_with_resolver(stream, classifier, |channel| Some(channel.to_string()))
}

/// Replay the timeline with raw payload bodies.
pub fn replay(stream: &StoredStream) -> impl Iterator<Item = LogEntry> {
    merged_log(stream).into_iter()
}

/// Decode one typed channel into a time series.
pub fn metric_series_on<R: Record>(
    stream: &StoredStream,
    channel: ChannelId,
) -> Vec<(Position, R)> {
    stream
        .frames()
        .filter(|f| f.channel == channel)
        .filter_map(|f| {
            R::decode(&f.payload)
                .ok()
                .map(|record| (f.position, record))
        })
        .collect()
}

/// Decode every frame whose payload parses as `R`.
pub fn metric_series<R: Record>(stream: &StoredStream) -> Vec<(Position, R)> {
    stream
        .frames()
        .filter_map(|f| {
            R::decode(&f.payload)
                .ok()
                .map(|record| (f.position, record))
        })
        .collect()
}

/// The last `n` frames in position order.
pub fn tail(stream: &StoredStream, n: usize) -> Vec<Frame> {
    let all = stream.to_vec();
    let start = all.len().saturating_sub(n);
    all[start..].to_vec()
}

/// Frames matching a predicate, in position order.
pub fn filter<F>(stream: &StoredStream, predicate: F) -> Vec<Frame>
where
    F: Fn(&Frame) -> bool,
{
    stream.frames().filter(|f| predicate(f)).cloned().collect()
}

/// Frames whose payload text contains `needle`.
pub fn grep(stream: &StoredStream, needle: &str) -> Vec<Frame> {
    filter(stream, |f| {
        String::from_utf8_lossy(&f.payload).contains(needle)
    })
}

impl fmt::Display for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Body::Record(value) => write!(f, "{value}"),
            Body::Text(text) => f.write_str(text),
            Body::Raw(bytes) => write!(f, "<{} opaque bytes>", bytes.len()),
        }
    }
}

impl fmt::Display for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogEntry::Frame(frame) => {
                write!(
                    f,
                    "#{:<4} [{}] {}",
                    frame.position, frame.channel, frame.body
                )
            }
            LogEntry::Gap(span) => {
                write!(
                    f,
                    "#{:<4} ── gap: {} position(s) missing ──",
                    span.start,
                    span.count()
                )
            }
        }
    }
}

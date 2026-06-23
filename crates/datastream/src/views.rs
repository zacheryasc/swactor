//! Views: read-time projections over a stored stream (spec §9).
//!
//! Everything a consumer shows is computed here, at read time, from the
//! stored stream alone (spec §9.1) — nothing is pre-projected at ingest.
//! That is what lets a merged log, a metric series, a tail/grep, and a
//! replay all be views of the same complete record. Adding or changing a
//! view changes nothing in producers, channels, or storage (spec §9.3):
//! every function here takes only a [`StoredStream`].
//!
//! A view decodes bytes with a caller-owned classifier. Over a channel it cannot
//! decode — an unknown id, or typed bytes that do not parse — it **degrades to
//! raw bytes** rather than failing (spec §9.3).
//!
//! The four named projections of spec §9.2 are:
//!
//! * [`merged_log`] / [`replay`] — the single timeline across all channels,
//!   in position order, with surfaced gaps;
//! * [`metric_series`] — decode one typed channel into a time series;
//! * [`tail`], [`grep`], [`filter`] — windowed / predicate-restricted views.

use std::fmt;

use super::frame::{ChannelId, Frame, Position};
use super::record::{ChannelClassifier, ChannelKind, Record};
use super::store::{GapSpan, StoredStream};

/// One entry on the merged timeline: either a frame or a surfaced gap.
#[derive(Debug, Clone, PartialEq)]
pub enum LogEntry {
    /// A stored frame, its payload decoded for display.
    Frame(MergedFrame),
    /// A run of positions that were assigned but never delivered (spec
    /// §7.5), surfaced rather than silently concatenated across.
    Gap(GapSpan),
}

/// A frame as the merged log presents it: where it sits on the timeline,
/// which channel it came from, and its payload decoded as far as the caller's
/// classifier allows.
#[derive(Debug, Clone, PartialEq)]
pub struct MergedFrame {
    /// The frame's position on the node's single timeline.
    pub position: Position,
    /// The channel the bytes belong to.
    pub channel: ChannelId,
    /// The payload, decoded per the channel's codec (or degraded to bytes).
    pub body: Body,
}

/// A payload decoded as far as the caller's classifier allows (spec §9.3).
#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    /// A typed channel decoded to a structured value.
    Record(serde_json::Value),
    /// A raw-text channel as its line(s).
    Text(String),
    /// A channel the consumer cannot decode — an unknown id, or typed bytes
    /// that failed to parse — kept as raw bytes (graceful degradation).
    Raw(Vec<u8>),
}

/// Decode a payload for display with a caller-owned classifier, degrading
/// gracefully (spec §9.3): a typed channel whose bytes do not parse, and any
/// unknown channel, fall back to raw bytes instead of failing.
pub fn decode_body_with<C: ChannelClassifier + ?Sized>(
    channel: &ChannelId,
    payload: &[u8],
    classifier: &C,
) -> Body {
    match classifier.classify(channel) {
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

/// Decode a payload with no caller registry. Unknown is the safe default.
pub fn decode_body(channel: &ChannelId, payload: &[u8]) -> Body {
    decode_body_with(channel, payload, &|_: &ChannelId| ChannelKind::Opaque)
}

/// Build the merged timeline with a caller-owned classifier.
fn timeline_with<C: ChannelClassifier + ?Sized>(
    stream: &StoredStream,
    classifier: &C,
) -> Vec<LogEntry> {
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
        out.push(LogEntry::Frame(MergedFrame {
            position: frame.position,
            channel: frame.channel.clone(),
            body: decode_body_with(&frame.channel, &frame.payload, classifier),
        }));
        prev = Some(pos);
    }
    out
}

/// **Full merged log view** (spec §9.2): the single timeline across all
/// channels, in position order, with gaps surfaced. Without a classifier,
/// payloads render as raw bytes.
pub fn merged_log(stream: &StoredStream) -> Vec<LogEntry> {
    timeline_with(stream, &|_: &ChannelId| ChannelKind::Opaque)
}

/// Full merged log using a caller-owned classifier for display decoding.
pub fn merged_log_with<C: ChannelClassifier + ?Sized>(
    stream: &StoredStream,
    classifier: &C,
) -> Vec<LogEntry> {
    timeline_with(stream, classifier)
}

/// **Replay** (spec §9.2): reconstruct the timeline after the fact, as if
/// observed live. Without a classifier, payloads render as raw bytes.
pub fn replay(stream: &StoredStream) -> impl Iterator<Item = LogEntry> {
    timeline_with(stream, &|_: &ChannelId| ChannelKind::Opaque).into_iter()
}

/// **Typed / metric projection** (spec §9.2): decode one typed channel into
/// a time series of `(position, record)`, in position order. Frames whose
/// bytes do not parse as `R` are skipped — the projection degrades rather
/// than failing (spec §9.3); they remain visible as raw bytes in the merged
/// log.
pub fn metric_series<R: Record>(stream: &StoredStream) -> Vec<(Position, R)> {
    let channel = R::channel();
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

/// **Tail** (spec §9.2): the last `n` frames in position order (fewer if
/// the stream is shorter).
pub fn tail(stream: &StoredStream, n: usize) -> Vec<Frame> {
    let all = stream.to_vec();
    let start = all.len().saturating_sub(n);
    all[start..].to_vec()
}

/// **Filter** (spec §9.2): the frames matching a predicate, in position
/// order.
pub fn filter<F>(stream: &StoredStream, predicate: F) -> Vec<Frame>
where
    F: Fn(&Frame) -> bool,
{
    stream.frames().filter(|f| predicate(f)).cloned().collect()
}

/// **Grep** (spec §9.2): the frames whose payload, read as text, contains
/// `needle`. Works uniformly across typed channels (their JSON bytes) and
/// text channels (their lines); binary payloads simply do not match.
pub fn grep(stream: &StoredStream, needle: &str) -> Vec<Frame> {
    filter(stream, |f| {
        String::from_utf8_lossy(&f.payload).contains(needle)
    })
}

// ── Human-readable rendering ───────────────────────────────────────────

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

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use protocol_telemetry::{TransitionRecord, consumer::ObservedTransition};
use serde_json::json;
use telemetry::frame::{ChannelContent, ChannelRef, Frame, Lifetime, NodeId, Position, StreamId};
use telemetry::{Record, decode_record_value};

/// Harness-owned JSONL evidence for real telemetry deliveries.
pub(crate) struct FrameArchive {
    file: File,
    next_seq: u64,
    path: PathBuf,
    label: &'static str,
    positions: BTreeMap<StreamId, Position>,
}

impl FrameArchive {
    pub(crate) fn open_with_label(path: &Path, label: &'static str) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create {label} dir {}: {e}", parent.display()))?;
        }
        let mut positions = BTreeMap::new();
        let mut next_seq = 0;
        let mut committed = 0;
        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let mut line = Vec::new();
                loop {
                    line.clear();
                    let count = reader
                        .read_until(b'\n', &mut line)
                        .map_err(|error| format!("read {label} {}: {error}", path.display()))?;
                    if count == 0 || !line.ends_with(b"\n") {
                        break;
                    }
                    let record: serde_json::Value =
                        serde_json::from_slice(&line).map_err(|error| {
                            format!("decode {label} {} at {committed}: {error}", path.display())
                        })?;
                    let stream = record["stream"]
                        .as_str()
                        .ok_or("archived frame omitted stream")?;
                    let (node, life) = stream.rsplit_once('#').ok_or("invalid archived stream")?;
                    let stream = StreamId::new(
                        NodeId::new(node),
                        Lifetime(life.parse().map_err(|_| "invalid archived lifetime")?),
                    );
                    let position = Position(
                        record["position"]
                            .as_u64()
                            .ok_or("archived frame omitted position")?,
                    );
                    positions
                        .entry(stream)
                        .and_modify(|previous: &mut Position| *previous = (*previous).max(position))
                        .or_insert(position);
                    next_seq = record["arrival_seq"]
                        .as_u64()
                        .ok_or("archived frame omitted arrival sequence")?
                        .checked_add(1)
                        .ok_or("archive sequence exhausted")?;
                    committed += count as u64;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("read {label} {}: {error}", path.display())),
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open {label} {}: {e}", path.display()))?;
        // A killed writer can leave a partial last record; only complete lines
        // define durable cursors. Replay recovers the uncommitted suffix.
        file.set_len(committed)
            .map_err(|error| format!("recover {label} {}: {error}", path.display()))?;
        Ok(Self {
            file,
            next_seq,
            path: path.to_path_buf(),
            label,
            positions,
        })
    }

    pub(crate) fn record(
        &mut self,
        source: &str,
        stream: &StreamId,
        channel: &str,
        content: &ChannelContent,
        frame: &Frame,
    ) -> Result<(), String> {
        if self
            .positions
            .get(stream)
            .is_some_and(|position| frame.position <= *position)
        {
            return Ok(());
        }
        let transition = if channel == TransitionRecord::CHANNEL
            && matches!(
                content,
                ChannelContent::MessagePackRecord { schema }
                    if schema.as_deref() == Some(TransitionRecord::CHANNEL)
            ) {
            Some(
                TransitionRecord::decode(&frame.payload).map(|record| ObservedTransition {
                    channel: ChannelRef {
                        stream: stream.clone(),
                        channel: frame.channel,
                    },
                    position: frame.position,
                    record,
                }),
            )
        } else {
            None
        };
        let payload = match content {
            ChannelContent::MessagePackRecord { .. } => match &transition {
                Some(Ok(transition)) => {
                    json!({"encoding": "messagepack", "value": transition.record})
                }
                _ => decode_record_value(&frame.payload)
                    .map(|value| json!({"encoding": "messagepack", "value": value}))
                    .unwrap_or_else(|_| json!({"encoding": "bytes", "value": frame.payload})),
            },
            ChannelContent::JsonRecord { .. } => serde_json::from_slice(&frame.payload)
                .map(|value: serde_json::Value| json!({"encoding": "json", "value": value}))
                .unwrap_or_else(|_| json!({"encoding": "bytes", "value": frame.payload})),
            ChannelContent::TextStream => match std::str::from_utf8(&frame.payload) {
                Ok(text) => json!({"encoding": "utf8", "value": text}),
                Err(_) => json!({"encoding": "bytes", "value": frame.payload}),
            },
            ChannelContent::Bytes => json!({"encoding": "bytes", "value": frame.payload}),
        };
        let mut record = json!({
            "arrival_seq": self.next_seq,
            "arrival_unix_ms": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis(),
            "source": source,
            "stream": stream.to_string(),
            "channel": channel,
            "channel_id": frame.channel.0,
            "channel_content": content,
            "position": frame.position.0,
            "payload": payload,
        });
        if let Some(transition) = transition {
            record["protocol_transition"] = match transition {
                Ok(transition) => json!({
                    "classification": if transition.record.outcome.is_some() {
                        "terminal"
                    } else {
                        "progress"
                    },
                    "rendered": transition.to_string(),
                }),
                Err(error) => json!({
                    "classification": "invalid",
                    "error": format!(
                        "invalid protocol transition at {stream} #{} [{}]: {error}",
                        frame.position, frame.channel,
                    ),
                }),
            };
        }
        let mut line =
            serde_json::to_vec(&record).map_err(|e| format!("serialize {}: {e}", self.label))?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .map_err(|e| format!("write {} {}: {e}", self.label, self.path.display()))?;
        self.file
            .flush()
            .map_err(|e| format!("flush {} {}: {e}", self.label, self.path.display()))?;
        self.positions.insert(stream.clone(), frame.position);
        self.next_seq += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use telemetry::ChannelId;

    #[test]
    fn recovery_discards_partial_tail_and_deduplicates_each_producer_lifetime() {
        let log = tempfile::NamedTempFile::new().unwrap();
        let retained = StreamId::new(NodeId::new("7"), Lifetime(11));
        let restarted = StreamId::new(NodeId::new("7"), Lifetime(12));
        let orchestrator = StreamId::new(NodeId::new("myelin-orchestrator"), Lifetime(11));
        let record = |archive: &mut FrameArchive, stream: &StreamId, position| {
            archive
                .record(
                    if stream == &orchestrator {
                        "orchestrator"
                    } else {
                        "node"
                    },
                    stream,
                    "runtime.log",
                    &ChannelContent::TextStream,
                    &Frame::new(ChannelId(1), Position(position), b"evidence".to_vec()),
                )
                .unwrap();
        };
        {
            let mut archive = FrameArchive::open_with_label(log.path(), "test").unwrap();
            record(&mut archive, &retained, 0);
            record(&mut archive, &retained, 1);
            record(&mut archive, &orchestrator, 0);
        }
        OpenOptions::new()
            .append(true)
            .open(log.path())
            .unwrap()
            .write_all(b"{\"arrival_seq\":3")
            .unwrap();
        {
            let mut archive = FrameArchive::open_with_label(log.path(), "test").unwrap();
            record(&mut archive, &retained, 0);
            record(&mut archive, &orchestrator, 0);
            record(&mut archive, &retained, 1);
            record(&mut archive, &retained, 2);
            record(&mut archive, &retained, 2);
            record(&mut archive, &restarted, 0);
        }
        let observed = fs::read_to_string(log.path())
            .unwrap()
            .lines()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                (
                    value["stream"].as_str().unwrap().to_owned(),
                    value["position"].as_u64().unwrap(),
                    value["arrival_seq"].as_u64().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            observed,
            vec![
                (retained.to_string(), 0, 0),
                (retained.to_string(), 1, 1),
                (orchestrator.to_string(), 0, 2),
                (retained.to_string(), 2, 3),
                (restarted.to_string(), 0, 4),
            ]
        );
    }
}

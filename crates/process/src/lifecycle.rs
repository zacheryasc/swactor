use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use datastream::{ChannelContent, DatastreamProducer, StreamId};
use serde_json::json;
use swactor::actor::ActorAddress;

use crate::message::ProcessOutput;
use crate::types::{ExitStatus, ProcessSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLifecycleObservability {
    Disabled,
    DatastreamMirror,
}

#[derive(Clone)]
pub struct ProcessOutputConfig {
    upstream: ActorAddress,
    observability: ProcessLifecycleObservability,
    datastream: Option<DatastreamProducer>,
}

impl ProcessOutputConfig {
    pub fn disabled(upstream: ActorAddress) -> Self {
        Self {
            upstream,
            observability: ProcessLifecycleObservability::Disabled,
            datastream: None,
        }
    }

    pub fn datastream_mirror(upstream: ActorAddress, producer: DatastreamProducer) -> Self {
        Self {
            upstream,
            observability: ProcessLifecycleObservability::DatastreamMirror,
            datastream: Some(producer),
        }
    }

    pub fn upstream(&self) -> ActorAddress {
        self.upstream
    }

    pub fn observability(&self) -> ProcessLifecycleObservability {
        self.observability
    }
}

pub(crate) struct PreparedProcessOutput {
    pub(crate) upstream: ActorAddress,
    pub(crate) mirror: Option<LifecycleDatastreamMirror>,
    pub(crate) _label_reservation: Option<LifecycleLabelReservation>,
}

pub(crate) struct LifecycleDatastreamMirror {
    channel: datastream::ChannelId,
    producer: DatastreamProducer,
}

impl LifecycleDatastreamMirror {
    pub(crate) fn submit(&self, output: &ProcessOutput) {
        let payload = match output {
            ProcessOutput::Started { pid } => json!({"event": "started", "pid": pid}),
            ProcessOutput::SpawnFailed { error } => {
                json!({"event": "spawn_failed", "error": error})
            }
            ProcessOutput::Exited { status } => match status {
                ExitStatus::Code(value) => {
                    json!({"event": "exited", "status": {"kind": "code", "value": value}})
                }
                ExitStatus::Signal(value) => {
                    json!({"event": "exited", "status": {"kind": "signal", "value": value}})
                }
                ExitStatus::Unknown => json!({"event": "exited", "status": {"kind": "unknown"}}),
            },
            ProcessOutput::Error { error } => json!({"event": "error", "error": error}),
        };
        let bytes = serde_json::to_vec(&payload).expect("process lifecycle record serializes");
        let _ = self.producer.submit_bytes(self.channel, bytes);
    }
}

pub(crate) fn prepare_process_output(
    spec: &ProcessSpec,
    config: ProcessOutputConfig,
) -> Result<PreparedProcessOutput, swactor::Error> {
    let label = derive_lifecycle_label(spec)?;
    match config.observability {
        ProcessLifecycleObservability::Disabled => Ok(PreparedProcessOutput {
            upstream: config.upstream,
            mirror: None,
            _label_reservation: None,
        }),
        ProcessLifecycleObservability::DatastreamMirror => {
            let producer = config
                .datastream
                .expect("datastream mirror config stores producer");
            let reservation =
                LifecycleLabelReservation::reserve(producer.stream_id().clone(), &label)?;
            let channel_name = label.channel_name();
            let channel = producer
                .try_register_channel(
                    channel_name,
                    ChannelContent::JsonRecord {
                        schema: Some("swactor_process.lifecycle.v1".to_owned()),
                    },
                )
                .map_err(|err| match err {
                    datastream::ChannelRegistrationError::ConflictingName { name } => {
                        swactor::Error::from(format!(
                            "conflicting datastream channel registration for {name}"
                        ))
                    }
                })?;

            Ok(PreparedProcessOutput {
                upstream: config.upstream,
                mirror: Some(LifecycleDatastreamMirror { channel, producer }),
                _label_reservation: Some(reservation),
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct LifecycleLabel(String);

impl LifecycleLabel {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn channel_name(&self) -> String {
        format!("proc.{}.lifecycle", self.0)
    }
}

pub(crate) fn derive_lifecycle_label(spec: &ProcessSpec) -> Result<LifecycleLabel, swactor::Error> {
    let source = match spec.label.as_deref() {
        Some(label) => label,
        None => spec
            .command
            .rsplit(['/', '\\'])
            .find(|segment| !segment.is_empty())
            .unwrap_or(&spec.command),
    };
    sanitize_lifecycle_label(source)
}

fn sanitize_lifecycle_label(source: &str) -> Result<LifecycleLabel, swactor::Error> {
    let mut sanitized = String::new();
    let mut last_was_underscore = false;

    for ch in source.trim().chars() {
        let next = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else if ch == '_' || ch == '-' {
            ch
        } else {
            '_'
        };

        if next == '_' {
            if !last_was_underscore {
                sanitized.push(next);
            }
            last_was_underscore = true;
        } else {
            sanitized.push(next);
            last_was_underscore = false;
        }
    }

    let sanitized = sanitized.trim_matches('_').to_owned();
    if sanitized.is_empty() {
        return Err(swactor::Error::from(
            "invalid process lifecycle label: empty segment",
        ));
    }

    Ok(LifecycleLabel(sanitized))
}

static LIFECYCLE_LABELS: OnceLock<Mutex<HashSet<(StreamId, String)>>> = OnceLock::new();

pub(crate) struct LifecycleLabelReservation {
    key: Option<(StreamId, String)>,
}

impl LifecycleLabelReservation {
    fn reserve(stream: StreamId, label: &LifecycleLabel) -> Result<Self, swactor::Error> {
        let key = (stream, label.as_str().to_owned());
        let mut labels = LIFECYCLE_LABELS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .expect("process lifecycle label registry poisoned");
        if !labels.insert(key.clone()) {
            return Err(swactor::Error::from(format!(
                "duplicate process lifecycle datastream channel: {} on stream {}",
                label.channel_name(),
                key.0
            )));
        }
        Ok(Self { key: Some(key) })
    }
}

impl Drop for LifecycleLabelReservation {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            LIFECYCLE_LABELS
                .get_or_init(|| Mutex::new(HashSet::new()))
                .lock()
                .expect("process lifecycle label registry poisoned")
                .remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datastream::{DatastreamEndpoint, Lifetime, NodeId, StreamId};
    use swactor::actor::ActorAddress;

    use super::*;

    fn spec(command: &str, label: Option<&str>) -> ProcessSpec {
        ProcessSpec {
            command: command.to_owned(),
            args: Vec::new(),
            env: HashMap::new(),
            working_dir: None,
            label: label.map(str::to_owned),
        }
    }

    #[test]
    fn explicit_labels_are_sanitized() {
        assert_eq!(
            derive_lifecycle_label(&spec("ignored", Some("trainer.0/foo")))
                .unwrap()
                .as_str(),
            "trainer_0_foo"
        );
        assert_eq!(
            derive_lifecycle_label(&spec("ignored", Some("  GPU Worker  ")))
                .unwrap()
                .as_str(),
            "gpu_worker"
        );
    }

    #[test]
    fn command_basename_labels_are_sanitized() {
        assert_eq!(
            derive_lifecycle_label(&spec("/usr/bin/python3", None))
                .unwrap()
                .as_str(),
            "python3"
        );
        assert_eq!(
            derive_lifecycle_label(&spec("./bin/train.v2", None))
                .unwrap()
                .as_str(),
            "train_v2"
        );
    }

    #[test]
    fn empty_lifecycle_labels_are_rejected() {
        assert_eq!(
            derive_lifecycle_label(&spec("ignored", Some("../")))
                .unwrap_err()
                .to_string(),
            "invalid process lifecycle label: empty segment"
        );
        assert_eq!(
            derive_lifecycle_label(&spec("", None))
                .unwrap_err()
                .to_string(),
            "invalid process lifecycle label: empty segment"
        );
    }

    #[test]
    fn duplicate_datastream_label_reservations_are_released_on_drop() {
        let endpoint = DatastreamEndpoint::new(StreamId::new(NodeId::new("stage2"), Lifetime(1)));
        let upstream = ActorAddress::new_random();
        let spec = spec("sh", Some("trainer.0/foo"));

        let first = prepare_process_output(
            &spec,
            ProcessOutputConfig::datastream_mirror(upstream, endpoint.producer()),
        )
        .unwrap();

        let err = match prepare_process_output(
            &spec,
            ProcessOutputConfig::datastream_mirror(upstream, endpoint.producer()),
        ) {
            Ok(_) => panic!("duplicate lifecycle label should be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "duplicate process lifecycle datastream channel: proc.trainer_0_foo.lifecycle on stream stage2#1"
        );

        drop(first);

        let second = prepare_process_output(
            &spec,
            ProcessOutputConfig::datastream_mirror(upstream, endpoint.producer()),
        )
        .unwrap();
        drop(second);
    }
}

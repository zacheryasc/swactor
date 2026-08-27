use swactor::actor::ActorAddress;
use swactor_process::ProcessOutputConfig;
use telemetry::TelemetryProducer;

#[derive(Clone)]
pub struct ContextualProcessOutputConfig {
    upstream: ActorAddress,
    telemetry: Option<TelemetryProducer>,
}

impl ContextualProcessOutputConfig {
    pub fn disabled(upstream: ActorAddress) -> Self {
        Self {
            upstream,
            telemetry: None,
        }
    }

    pub fn telemetry_mirror(upstream: ActorAddress, producer: TelemetryProducer) -> Self {
        Self {
            upstream,
            telemetry: Some(producer),
        }
    }

    pub fn upstream(&self) -> ActorAddress {
        self.upstream
    }

    pub(crate) fn process_output(&self, relay: ActorAddress) -> ProcessOutputConfig {
        self.telemetry.clone().map_or_else(
            || ProcessOutputConfig::disabled(relay),
            |producer| ProcessOutputConfig::telemetry_mirror(relay, producer),
        )
    }
}

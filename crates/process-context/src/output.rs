use std::sync::Arc;

use crate::model::ContextualProcessOutput;
use swactor::actor::ActorAddress;
use swactor_process::ProcessOutputConfig;
use telemetry::TelemetryProducer;

#[derive(Clone)]
pub struct ContextualProcessOutputConfig {
    upstream: ActorAddress,
    telemetry: Option<TelemetryProducer>,
    lifecycle_observer: Option<Arc<dyn Fn(ContextualProcessOutput) + Send + Sync>>,
}

impl ContextualProcessOutputConfig {
    pub fn disabled(upstream: ActorAddress) -> Self {
        Self {
            upstream,
            telemetry: None,
            lifecycle_observer: None,
        }
    }

    pub fn telemetry_mirror(upstream: ActorAddress, producer: TelemetryProducer) -> Self {
        Self {
            upstream,
            telemetry: Some(producer),
            lifecycle_observer: None,
        }
    }

    /// Observe lifecycle independently of a transient byte-output relay.
    pub fn with_lifecycle_observer(
        mut self,
        observer: Arc<dyn Fn(ContextualProcessOutput) + Send + Sync>,
    ) -> Self {
        self.lifecycle_observer = Some(observer);
        self
    }

    pub(crate) fn emit(&self, ctx: &swactor::actor::Ctx<'_>, output: ContextualProcessOutput) {
        if !matches!(
            &output,
            ContextualProcessOutput::Process(
                swactor_process::ProcessOutput::Stdout(_)
                    | swactor_process::ProcessOutput::Stderr(_)
            )
        ) && let Some(observer) = &self.lifecycle_observer
        {
            observer(output.clone());
        }
        let _ = ctx.send(self.upstream, output);
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

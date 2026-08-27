use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use swactor::Error;
use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::EngineHandle;

use crate::actor::{
    ContextualActorMessage, ContextualProcessActor, ContextualProcessActorConfig,
    ContextualProcessCommand,
};
use crate::model::{ContextualProcessSpec, ExecutionIdentity};
use crate::output::ContextualProcessOutputConfig;
use crate::ports::ContextProvisioner;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpawnedContextualProcess {
    pub actor: ActorAddress,
    pub identity: ExecutionIdentity,
}

pub struct ContextualProcessSpawner {
    engine: EngineHandle,
    provisioner: Arc<dyn ContextProvisioner>,
    next_execution_id: AtomicU64,
    default_kill_after: Option<Duration>,
}

impl ContextualProcessSpawner {
    pub fn new(engine: EngineHandle, provisioner: Arc<dyn ContextProvisioner>) -> Self {
        Self {
            engine,
            provisioner,
            next_execution_id: AtomicU64::new(1),
            default_kill_after: Some(Duration::from_secs(5)),
        }
    }

    pub fn with_default_kill_after(mut self, kill_after: Option<Duration>) -> Self {
        self.default_kill_after = kill_after;
        self
    }

    pub fn spawn(
        &self,
        ctx: &Ctx<'_>,
        sender: &ExternalSender,
        spec: ContextualProcessSpec,
        output: ContextualProcessOutputConfig,
    ) -> Result<SpawnedContextualProcess, Error> {
        if spec.process.command.is_empty() {
            return Err(Error::from("contextual process command must not be empty"));
        }
        if spec.attach_deadline.is_zero() {
            return Err(Error::from(
                "contextual process attachment deadline must be nonzero",
            ));
        }
        spec.access
            .validate()
            .map_err(|error| Error::from(format!("invalid contextual session access: {error}")))?;
        let execution_id = self.next_execution_id.fetch_add(1, Ordering::Relaxed);
        if execution_id == 0 {
            return Err(Error::from("contextual process execution ids exhausted"));
        }
        let identity = ExecutionIdentity {
            execution_id,
            generation: 1,
        };
        let actor = ctx.spawn(ContextualProcessActor::new(ContextualProcessActorConfig {
            identity,
            spec,
            output,
            provisioner: Arc::clone(&self.provisioner),
            engine: self.engine.clone(),
            sender: sender.clone(),
            default_kill_after: self.default_kill_after,
        }))?;
        Ok(SpawnedContextualProcess { actor, identity })
    }
}

pub fn send_contextual_process_command(
    sender: &ExternalSender,
    process: ActorAddress,
    command: ContextualProcessCommand,
) -> Result<(), Error> {
    sender.send_to(process, ContextualActorMessage::Command(command))
}

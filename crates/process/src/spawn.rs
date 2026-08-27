use std::sync::{Arc, OnceLock};

use swactor::Error;
use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::ExternalSender;

use crate::actor::ProcessActor;
use crate::lifecycle::{ProcessOutputConfig, prepare_process_output};
use crate::message::{ProcessActorCommand, ProcessCommand};
#[cfg(unix)]
use crate::resources::ProcessSpawnResources;
use crate::types::ProcessSpec;

/// Spawn a process actor using the OS subprocess supervisor.
pub fn spawn_local_process(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    output: ProcessOutputConfig,
) -> Result<ActorAddress, Error> {
    #[cfg(unix)]
    {
        spawn_local_process_with_resources(ctx, sender, spec, ProcessSpawnResources::new(), output)
    }

    #[cfg(not(unix))]
    {
        let prepared_output = prepare_process_output(&spec, output)?;
        let addr_slot = Arc::new(OnceLock::new());
        let actor = ProcessActor::new(spec, prepared_output, sender.clone(), addr_slot.clone());
        let addr = ctx.spawn(actor)?;
        addr_slot
            .set(addr)
            .expect("process actor address already set");
        let _ = sender.send_to(addr, ProcessActorCommand::SupervisorWake);
        Ok(addr)
    }
}

/// Spawn a process actor with explicitly owned child descriptor mappings.
#[cfg(unix)]
pub fn spawn_local_process_with_resources(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    resources: ProcessSpawnResources,
    output: ProcessOutputConfig,
) -> Result<ActorAddress, Error> {
    let prepared_output = prepare_process_output(&spec, output)?;
    let addr_slot = Arc::new(OnceLock::new());
    let actor = ProcessActor::new(
        spec,
        resources,
        prepared_output,
        sender.clone(),
        addr_slot.clone(),
    );
    let addr = ctx.spawn(actor)?;
    addr_slot
        .set(addr)
        .expect("process actor address already set");
    let _ = sender.send_to(addr, ProcessActorCommand::SupervisorWake);
    Ok(addr)
}

pub fn send_process_command(
    sender: &ExternalSender,
    process: ActorAddress,
    command: ProcessCommand,
) -> Result<(), Error> {
    sender.send_to(process, ProcessActorCommand::Command(command))
}

use std::sync::{Arc, OnceLock};

use swactor::Error;
use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::BlockingWorkSender;

use crate::actor::ProcessActor;
use crate::lifecycle::{ProcessOutputConfig, prepare_process_output};
use crate::message::{ProcessActorCommand, ProcessCommand};
#[cfg(unix)]
use crate::resources::ProcessSpawnResources;
use crate::stdin::{ProcessInput, ProcessStdin, process_input_channel, terminal_input_channel};
use crate::types::ProcessSpec;
use crate::terminal::{TerminalInput, TerminalSize};

/// Spawn a process actor using the OS subprocess supervisor.
pub fn spawn_local_process(
    blocking: &BlockingWorkSender,
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    output: ProcessOutputConfig,
) -> Result<ActorAddress, Error> {
    spawn_process(
        blocking,
        ctx,
        sender,
        spec,
        #[cfg(unix)]
        ProcessSpawnResources::new(),
        output,
        None,
    )
}

/// Spawn a process actor with ordered, bounded, asynchronously acknowledged stdin.
///
/// OS spawn success or failure is reported through the existing process outputs.
pub fn spawn_local_process_with_stdin(
    blocking: &BlockingWorkSender,
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    output: ProcessOutputConfig,
) -> Result<(ActorAddress, ProcessStdin), Error> {
    let (stdin, input) = process_input_channel();
    let addr = spawn_process(
        blocking,
        ctx,
        sender,
        spec,
        #[cfg(unix)]
        ProcessSpawnResources::new(),
        output,
        Some(input),
    )?;
    Ok((addr, stdin))
}

/// Spawn a managed native terminal with merged output through `ProcessOutput::Stdout`.
/// Child spawn success or failure is reported through the existing lifecycle events.
pub fn spawn_local_terminal(
    blocking: &BlockingWorkSender,
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    size: TerminalSize,
    output: ProcessOutputConfig,
) -> Result<(ActorAddress, TerminalInput), Error> {
    size.validate().map_err(|error| Error::from(error.to_string()))?;
    let (terminal, input) = terminal_input_channel(size);
    let addr = spawn_process(
        blocking,
        ctx,
        sender,
        spec,
        #[cfg(unix)]
        ProcessSpawnResources::new(),
        output,
        Some(input),
    )?;
    Ok((addr, terminal))
}

/// Spawn a process actor with explicitly owned child descriptor mappings.
#[cfg(unix)]
pub fn spawn_local_process_with_resources(
    blocking: &BlockingWorkSender,
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    resources: ProcessSpawnResources,
    output: ProcessOutputConfig,
) -> Result<ActorAddress, Error> {
    spawn_process(blocking, ctx, sender, spec, resources, output, None)
}

fn spawn_process(
    blocking: &BlockingWorkSender,
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    #[cfg(unix)] resources: ProcessSpawnResources,
    output: ProcessOutputConfig,
    input: Option<ProcessInput>,
) -> Result<ActorAddress, Error> {
    let prepared_output = prepare_process_output(&spec, output)?;
    let addr_slot = Arc::new(OnceLock::new());
    let actor = ProcessActor::new(
        blocking.clone(),
        spec,
        #[cfg(unix)]
        resources,
        prepared_output,
        input,
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

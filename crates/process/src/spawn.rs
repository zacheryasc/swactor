use std::sync::{Arc, OnceLock};

use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::ExternalSender;
use swactor::Error;

use crate::actor::ProcessActor;
use crate::local::LocalDriver;
use crate::message::ProcessCommand;
use crate::session::ProcessSession;
use crate::types::{EventQueue, ProcessDriver, ProcessSpec, ProcessWaker};

/// Spawn a process actor using the real `LocalDriver` (OS subprocess).
///
/// Creates a `ProcessActor<LocalDriver>`, spawns it in the runtime, and
/// wires up the waker so that I/O thread events automatically wake the actor.
///
/// Returns the actor's address. Send `ProcessCommand` messages to control it.
pub fn spawn_local_process(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
) -> Result<ActorAddress, Error> {
    let waker_slot = Arc::new(OnceLock::new());
    let queue = EventQueue::new();
    let driver = LocalDriver::new(queue, waker_slot.clone());
    spawn_process_inner(ctx, sender, spec, driver, waker_slot)
}

/// Spawn a process actor with a custom driver.
///
/// Useful for testing with `MockDriver` or other custom drivers while
/// still getting the full actor integration (waker, lifecycle, etc.).
pub fn spawn_process<D: ProcessDriver + 'static>(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    driver: D,
    waker_slot: Arc<OnceLock<ProcessWaker>>,
) -> Result<ActorAddress, Error> {
    spawn_process_inner(ctx, sender, spec, driver, waker_slot)
}

/// Spawn a process actor using the `SshDriver` (remote host via SSH).
///
/// Requires a tokio runtime handle (e.g. from `IrohDriver::tokio_handle()`)
/// and SSH connection config.
#[cfg(feature = "ssh")]
pub fn spawn_ssh_process(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    tokio_handle: tokio::runtime::Handle,
    ssh_config: crate::ssh::SshConfig,
) -> Result<ActorAddress, Error> {
    let waker_slot = Arc::new(OnceLock::new());
    let queue = EventQueue::new();
    let driver = crate::ssh::SshDriver::new(queue, waker_slot.clone(), tokio_handle, ssh_config);
    spawn_process_inner(ctx, sender, spec, driver, waker_slot)
}

fn spawn_process_inner<D: ProcessDriver + 'static>(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    driver: D,
    waker_slot: Arc<OnceLock<ProcessWaker>>,
) -> Result<ActorAddress, Error> {
    let (session, initial_actions) = ProcessSession::new(spec);
    let actor = ProcessActor::new(session, driver, initial_actions, waker_slot.clone());
    let addr = ctx.spawn(actor)?;

    // Now that we have the address, fill the waker
    let sender = sender.clone();
    let waker = ProcessWaker::new(move || {
        let _ = sender.send_to(addr, ProcessCommand::PollTick);
    });
    waker_slot
        .set(waker.clone())
        .expect("waker slot already set");

    // Flush any events from the startup race window
    waker.wake();

    Ok(addr)
}

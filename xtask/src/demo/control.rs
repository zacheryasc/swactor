//! Control plumbing: dashboard control commands → supervisor actor.
//!
//! The dashboard (under `demo-control`) dispatches into a std mpsc channel;
//! a blocking-pool task forwards each command as a `SupervisorMsg::Control`
//! message to the supervisor actor.

use std::sync::mpsc as std_mpsc;

use swactor::runtime::ExternalSender;
use swactor_engine::EngineHandle;

use crate::demo::feed::SupervisorMsg;

/// Wire the dashboard control channel to the supervisor actor.
pub fn install(
    engine: &EngineHandle,
    sender: ExternalSender,
    supervisor: std::sync::Arc<std::sync::OnceLock<swactor::actor::ActorAddress>>,
) {
    let (control_tx, control_rx) = std_mpsc::channel::<dashboard::control::ControlCommand>();
    dashboard::control::set_control_sender(control_tx);

    let engine = engine.clone();
    // A std-mpsc recv blocks its thread, so this forwarder must live on the
    // engine's blocking pool — as an async task it would park one of the
    // (two) Tokio workers indefinitely and starve the reconciler ticks.
    engine.spawn_blocking(move || {
        loop {
            match control_rx.recv() {
                Ok(command) => {
                    if let Some(addr) = supervisor.get() {
                        let _ = sender.send_to(addr.clone(), SupervisorMsg::Control(command));
                    }
                }
                Err(_) => return,
            }
        }
    });
}

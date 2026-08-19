//! Demo-only control plane: write actions out of the dashboard.
//!
//! This module exists only under the `demo-control` feature. The dashboard's
//! data path stays read-only in every regular build; the provisioning
//! reconciler demo turns this feature on so a human can kill provisioned
//! processes and request new ones from the fleet view.
//!
//! Routes (only present when the feature is enabled and a control sink is
//! installed):
//! - `POST /control/kill` body `{"Kill":{"command_id":"...","node":"..."}}`
//! - `POST /control/provision` body `{"Provision":{"command_id":"...","count":1}}`

use std::sync::OnceLock;
use std::sync::mpsc::Sender;

use serde::Deserialize;

/// A control command issued from the dashboard UI.
#[derive(Clone, Debug, Deserialize)]
pub enum ControlCommand {
    /// Kill the process backing the fleet card identified by its stream node.
    Kill { command_id: String, node: String },
    /// Ask the reconciler to provision `count` additional nodes.
    Provision { command_id: String, count: u32 },
    /// Lower the desired cluster size by `count` nodes (graceful scale
    /// down: teardown through the reconciler, not a kill).
    Remove { command_id: String, count: u32 },
    /// Establish (or replace) the data-plane edge toward one node.
    EstablishEdge { command_id: String, node: String },
}

impl ControlCommand {
    pub fn command_id(&self) -> &str {
        match self {
            Self::Kill { command_id, .. }
            | Self::Provision { command_id, .. }
            | Self::Remove { command_id, .. }
            | Self::EstablishEdge { command_id, .. } => command_id,
        }
    }
}

static CONTROL_SENDER: OnceLock<Sender<ControlCommand>> = OnceLock::new();

/// Install the sink that receives dashboard-issued control commands.
///
/// Called once by the embedding demo before the HTTP server starts. Without a
/// sink the control routes answer `503 Service Unavailable`.
pub fn set_control_sender(sender: Sender<ControlCommand>) {
    let _ = CONTROL_SENDER.set(sender);
}

/// Install an actor destination for dashboard-issued control commands.
///
/// The dashboard owns the blocking channel reader; each command becomes one
/// typed actor observation.
pub fn install_actor_sink(
    sender: swactor::runtime::ExternalSender,
    actor: swactor::actor::ActorAddress,
) {
    let (control_sender, receiver) = std::sync::mpsc::channel();
    set_control_sender(control_sender);
    drop(spawn_actor_sink_forwarder(receiver, sender, actor));
}

fn spawn_actor_sink_forwarder(
    receiver: std::sync::mpsc::Receiver<ControlCommand>,
    sender: swactor::runtime::ExternalSender,
    actor: swactor::actor::ActorAddress,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while let Ok(command) = receiver.recv() {
            if sender.send_to(actor, command).is_err() {
                return;
            }
        }
    })
}

pub(crate) fn dispatch(command: ControlCommand) -> bool {
    CONTROL_SENDER
        .get()
        .is_some_and(|sender| sender.send(command).is_ok())
}

#[cfg(test)]
mod properties {
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;
    use proptest::prelude::*;
    use swactor::actor::{ActorInterface, Ctx};
    use swactor::config::RuntimeConfig;
    use swactor::runtime::{Runtime, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};

    use super::*;

    const STEP_BUDGET: usize = 64;
    const FORWARDER_BUDGET: Duration = Duration::from_secs(1);

    struct CommandProbe {
        observed: Arc<Mutex<Vec<String>>>,
    }

    impl ActorInterface for CommandProbe {
        type Incoming = ControlCommand;
        type Response = ();

        fn handle(&mut self, _ctx: &Ctx, command: Self::Incoming) {
            self.observed.lock().push(format!("{command:?}"));
        }
    }

    fn command(kind: u8, value: u8) -> ControlCommand {
        let command_id = format!("repeated-{}", value % 4);
        match kind % 4 {
            0 => ControlCommand::Kill {
                command_id,
                node: format!("node-{value}"),
            },
            1 => ControlCommand::Provision {
                command_id,
                count: u32::from(value),
            },
            2 => ControlCommand::Remove {
                command_id,
                count: u32::from(value),
            },
            _ => ControlCommand::EstablishEdge {
                command_id,
                node: format!("node-{value}"),
            },
        }
    }

    fn bridge_invariant_failure(
        expected: &[String],
        observed: &[String],
        runtime: &Runtime,
        expected_actor_count: usize,
    ) -> Option<String> {
        let stats = runtime.stats();
        let panics = stats
            .workers
            .iter()
            .map(|worker| worker.panics)
            .sum::<u64>();
        let mailbox_depth = stats
            .workers
            .iter()
            .map(|worker| worker.mailbox_depth)
            .sum::<usize>()
            + stats
                .actor_details
                .iter()
                .map(|actor| actor.mailbox_depth)
                .sum::<usize>();
        if observed != expected
            || stats.actors.len() != expected_actor_count
            || stats.actor_details.iter().any(|actor| actor.poisoned)
            || panics != 0
            || mailbox_depth != 0
        {
            Some(format!(
                "expected={expected:?}, observed={observed:?}, \
                 expected_actor_count={expected_actor_count}, mailbox_depth={mailbox_depth}, \
                 actor_census={stats:?}"
            ))
        } else {
            None
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_concurrent_bridge_commands_forward_once_and_shutdown(
            inputs in prop::collection::vec((any::<u8>(), any::<u8>()), 0..=32),
            split in 0_usize..=32,
            concurrent in any::<bool>(),
            destination_disappears in any::<bool>(),
        ) {
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let _engine =
                Engine::new(parts, backend.clone()).expect("dashboard bridge stepping engine");
            let observed = Arc::new(Mutex::new(Vec::new()));
            let probe = runtime
                .spawn(CommandProbe {
                    observed: Arc::clone(&observed),
                })
                .expect("spawn dashboard control probe");
            if destination_disappears {
                runtime
                    .stop_actor(probe)
                    .expect("stop dashboard destination before forwarding");
                for _ in 0..STEP_BUDGET {
                    backend.step();
                }
            }

            let (tx, rx) = std::sync::mpsc::channel();
            let forwarder =
                spawn_actor_sink_forwarder(rx, runtime.create_sender(), probe);
            let commands = inputs
                .iter()
                .map(|(kind, value)| command(*kind, *value))
                .collect::<Vec<_>>();
            let command_log = commands
                .iter()
                .map(|command| format!("{command:?}"))
                .collect::<Vec<_>>();

            if concurrent {
                let split = split.min(commands.len());
                let left = commands[..split].to_vec();
                let right = commands[split..].to_vec();
                let left_tx = tx.clone();
                let left_sender = std::thread::spawn(move || {
                    left.into_iter()
                        .map(|command| left_tx.send(command).is_ok())
                        .collect::<Vec<_>>()
                });
                let right_tx = tx.clone();
                let right_sender = std::thread::spawn(move || {
                    right
                        .into_iter()
                        .map(|command| right_tx.send(command).is_ok())
                        .collect::<Vec<_>>()
                });
                let _ = left_sender.join().expect("join left dashboard sender");
                let _ = right_sender.join().expect("join right dashboard sender");
            } else {
                for command in commands {
                    if !destination_disappears {
                        tx.send(command).expect("send dashboard command");
                    } else {
                        let _ = tx.send(command);
                    }
                }
            }
            drop(tx);

            let (done_tx, done_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = done_tx.send(forwarder.join());
            });
            let forwarder_result = done_rx.recv_timeout(FORWARDER_BUDGET).unwrap_or_else(|error| {
                panic!(
                    "dashboard bridge did not terminate after disconnect: {error}; \
                     actions={command_log:?}; actor_census={:?}",
                    runtime.stats(),
                )
            });
            forwarder_result.expect("dashboard bridge forwarder panicked");
            for _ in 0..STEP_BUDGET {
                backend.step();
            }

            let mut actual = observed.lock().clone();
            actual.sort();
            let mut expected = if destination_disappears {
                Vec::new()
            } else {
                command_log.clone()
            };
            expected.sort();
            let expected_actor_count = usize::from(!destination_disappears);
            prop_assert!(
                bridge_invariant_failure(
                    &expected,
                    &actual,
                    &runtime,
                    expected_actor_count,
                )
                .is_none(),
                "dashboard bridge invariant failed; actions={:?}; disconnect={}; failure={}",
                command_log,
                destination_disappears,
                bridge_invariant_failure(
                    &expected,
                    &actual,
                    &runtime,
                    expected_actor_count,
                )
                .unwrap_or_default(),
            );

            if !destination_disappears {
                runtime
                    .stop_actor(probe)
                    .expect("stop dashboard control probe");
                for _ in 0..STEP_BUDGET {
                    backend.step();
                }
            }
            prop_assert!(
                bridge_invariant_failure(&expected, &actual, &runtime, 0).is_none(),
                "dashboard bridge teardown leaked forwarding actors; actions={:?}; failure={}",
                command_log,
                bridge_invariant_failure(&expected, &actual, &runtime, 0)
                    .unwrap_or_default(),
            );
        }
    }

    #[test]
    fn bridge_invariant_rejects_a_controlled_duplicate_delivery() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let expected = vec!["Provision repeated-0".to_owned()];
        let duplicated = vec![
            "Provision repeated-0".to_owned(),
            "Provision repeated-0".to_owned(),
        ];

        assert!(
            bridge_invariant_failure(&expected, &duplicated, &runtime, 0).is_some(),
            "bridge invariant accepted a controlled duplicate delivery"
        );
    }
}

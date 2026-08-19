//! Control plumbing: dashboard control commands → supervisor actor.

use std::sync::{Arc, OnceLock};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime};

use crate::demo::feed::SupervisorMsg;

struct ControlForwarder {
    supervisor: Arc<OnceLock<ActorAddress>>,
}

impl ActorInterface for ControlForwarder {
    type Incoming = dashboard::control::ControlCommand;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, command: Self::Incoming) {
        if let Some(supervisor) = self.supervisor.get() {
            let _ = ctx.send(*supervisor, SupervisorMsg::Control(command));
        }
    }
}

/// Wire the dashboard control channel to a typed actor relay.
pub fn install(runtime: &Runtime, supervisor: Arc<OnceLock<ActorAddress>>) {
    let actor = runtime
        .spawn(ControlForwarder { supervisor })
        .expect("spawn dashboard control forwarder actor");
    dashboard::control::install_actor_sink(runtime.create_sender(), actor);
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend};

    use super::*;

    fn command(code: u8, index: usize) -> dashboard::control::ControlCommand {
        let command_id = format!("command-{index}");
        match code % 4 {
            0 => dashboard::control::ControlCommand::Kill {
                command_id,
                node: format!("node-{code}"),
            },
            1 => dashboard::control::ControlCommand::Provision {
                command_id,
                count: u32::from(code),
            },
            2 => dashboard::control::ControlCommand::Remove {
                command_id,
                count: u32::from(code),
            },
            _ => dashboard::control::ControlCommand::EstablishEdge {
                command_id,
                node: format!("node-{code}"),
            },
        }
    }

    fn drive(backend: &SteppingBackend) {
        for _ in 0..16 {
            backend.step();
        }
    }

    fn check_control_invariants(
        received: &[String],
        expected: &[String],
        active_actors: usize,
        final_actors: usize,
        worker_panics: u64,
    ) -> Result<(), String> {
        if received != expected {
            return Err(format!(
                "forwarded command mismatch: received={received:?} expected={expected:?}"
            ));
        }
        if active_actors != 1 {
            return Err(format!(
                "one control route must own one actor: observed={active_actors}"
            ));
        }
        if final_actors != 0 {
            return Err(format!(
                "control actors did not return to baseline: observed={final_actors}"
            ));
        }
        if worker_panics != 0 {
            return Err(format!("control worker panicked {worker_panics} time(s)"));
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_control_commands_forward_only_after_supervisor_registration(
            codes in prop::collection::vec(any::<u8>(), 0..=32),
            registration in 0_usize..=32,
        ) {
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let _engine =
                Engine::new(parts, backend.clone()).expect("demo control stepping engine");
            let supervisor = runtime
                .new_inbox::<SupervisorMsg>()
                .expect("create demo supervisor inbox");
            let supervisor_slot = Arc::new(OnceLock::new());
            let forwarder = runtime
                .spawn(ControlForwarder {
                    supervisor: Arc::clone(&supervisor_slot),
                })
                .expect("spawn demo control forwarder");
            let registration = registration.min(codes.len());
            let active_actors = runtime.stats().actors.len();
            prop_assert_eq!(
                active_actors,
                1,
                "control identity created the wrong actor set; codes={:?} \
                 registration={} census={:?}",
                codes,
                registration,
                runtime.stats()
            );

            for (index, code) in codes.iter().take(registration).enumerate() {
                runtime
                    .send_to(forwarder, command(*code, index))
                    .expect("send pre-registration control command");
            }
            drive(&backend);
            prop_assert!(
                supervisor.try_recv().is_none(),
                "control command crossed an uninitialized supervisor route; \
                 codes={:?} registration={} census={:?}",
                codes,
                registration,
                runtime.stats()
            );

            supervisor_slot
                .set(*supervisor.addr())
                .expect("register supervisor address");
            for (index, code) in codes.iter().enumerate().skip(registration) {
                runtime
                    .send_to(forwarder, command(*code, index))
                    .expect("send registered control command");
            }
            drive(&backend);

            let mut received_ids = Vec::new();
            while let Some(message) = supervisor.try_recv() {
                if let SupervisorMsg::Control(command) = message {
                    received_ids.push(command.command_id().to_owned());
                }
            }
            let expected_ids = (registration..codes.len())
                .map(|index| format!("command-{index}"))
                .collect::<Vec<_>>();

            runtime
                .stop_actor(forwarder)
                .expect("stop demo control forwarder");
            drive(&backend);
            let stats = runtime.stats();
            let worker_panics = stats
                .workers
                .iter()
                .map(|worker| worker.panics)
                .sum::<u64>();
            prop_assert!(
                check_control_invariants(
                    &received_ids,
                    &expected_ids,
                    active_actors,
                    stats.actors.len(),
                    worker_panics,
                )
                .is_ok(),
                "demo control invariant failed; codes={:?} registration={} \
                 received={:?} expected={:?} census={:?}",
                codes,
                registration,
                received_ids,
                expected_ids,
                stats
            );
        }
    }

    #[test]
    fn control_transition_oracle_rejects_duplicate_forwarding() {
        let rejected = check_control_invariants(
            &["command-0".to_owned(), "command-0".to_owned()],
            &["command-0".to_owned()],
            1,
            0,
            0,
        );
        assert!(
            rejected.is_err(),
            "control property oracle accepted a controlled duplicate forwarding defect"
        );
    }
}

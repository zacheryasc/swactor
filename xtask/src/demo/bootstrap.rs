//! Demo bootstrap logic #1: local OS foreign process.
//!
//! Implements [`BootstrapLogic`] for `"process"`-kind specs: the node is a
//! re-exec'd child of this binary supervised by a `swactor-process` actor
//! (stdio null). Readiness arrives over the control plane: the child's node
//! role joins the supervisor's iroh endpoint and announces itself, and the
//! supervisor's announce relay delivers [`BootstrapMsg::Announce`] to the
//! owning bootstrap actor. This logic only owns process lifecycle (spawn,
//! exit observation, termination). The docker variant sits beside it in the
//! registry — same interface, different foreign process.

use std::time::SystemTime;

use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{ProcessOutputConfig, ProcessSpec, spawn_local_process};

use provisioning::bootstrap::{BootstrapLogic, LogicProbe, NodeLaunchSpec};

use crate::demo::provider::{NodeManager, NodeRelayActor, NodeRuntime};

/// Local-process bootstrap logic. The spawned process actor's lifecycle
/// reports fold into the shared [`NodeManager`] registry via
/// [`NodeRelayActor`]; probes read that registry only — readiness is the
/// wire announce, not a probe observation.
pub struct LocalProcessLogic {
    spec: NodeLaunchSpec,
    manager: NodeManager,
    process_actor: Option<ActorAddress>,
}

impl LocalProcessLogic {
    pub fn new(spec: NodeLaunchSpec, manager: NodeManager) -> Self {
        Self {
            spec,
            manager,
            process_actor: None,
        }
    }
}

impl BootstrapLogic for LocalProcessLogic {
    fn start(
        &mut self,
        ctx: &Ctx,
        owner: ActorAddress,
        sender: &ExternalSender,
    ) -> Result<(), String> {
        let attempt = self.spec.attempt;
        let relay = ctx
            .spawn(NodeRelayActor::new(self.manager.clone(), attempt))
            .map_err(|error| format!("spawn relay actor: {error}"))?;

        let (command, args) = self
            .spec
            .argv
            .split_first()
            .map(|(head, tail)| (head.clone(), tail.to_vec()))
            .ok_or_else(|| "process spec missing argv".to_owned())?;
        let spec = ProcessSpec {
            command,
            args,
            env: self.spec.env.clone().into_iter().collect(),
            working_dir: self.spec.workdir.clone(),
            label: self.spec.label.clone(),
        };
        let output = ProcessOutputConfig::disabled(relay);
        let process_actor = spawn_local_process(ctx, sender, spec, output)
            .map_err(|error| format!("spawn process actor: {error}"))?;
        self.process_actor = Some(process_actor);

        self.manager.register(NodeRuntime {
            attempt,
            logical_node: self.spec.logical_node.clone(),
            bootstrap: owner,
            pid: None,
            exited: None,
            spawn_failed: None,
            last_announce_ms: None,
            endpoint_addr: None,
        });
        Ok(())
    }

    fn probe(&mut self, _now: SystemTime) -> LogicProbe {
        let Some(runtime) = self.manager.get(self.spec.attempt) else {
            // Deregistered (lease destroyed / supervisor teardown): the
            // node is gone by definition. The relay's exit observation can
            // lose the race against deregistration, so this is the
            // reliable terminal report. The actor's phase machine turns it
            // into Failed (before join) or Exited (after).
            return LogicProbe::Exited("deregistered (lease destroyed)".to_owned());
        };
        if let Some(error) = &runtime.spawn_failed {
            return LogicProbe::Failed(format!("node process failed to spawn: {error}"));
        }
        if let Some(status) = &runtime.exited {
            return LogicProbe::Exited(format!("{status:?}"));
        }
        // Still coming up; readiness is the wire announce.
        LogicProbe::Pending
    }

    fn terminate(&mut self, sender: &ExternalSender, kill_after: Option<std::time::Duration>) {
        if let Some(process_actor) = self.process_actor {
            let _ = swactor_process::send_process_command(
                sender,
                process_actor,
                swactor_process::ProcessCommand::Stop { kill_after },
            );
        }
    }
}

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::stage_controller as core;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageControllerMsg {
    Observe(core::StageEvent),
    Snapshot { reply_to: ActorAddress },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageControllerReport {
    Command(core::StageCommand),
    Lifecycle(core::StageLifecycleEvent),
    Snapshot {
        commands: Vec<core::StageCommand>,
        events: Vec<core::StageLifecycleEvent>,
    },
}

pub struct StageControllerActor {
    core: core::StageController,
    report_to: Option<ActorAddress>,
    command_cursor: usize,
    event_cursor: usize,
}

impl StageControllerActor {
    pub fn new(local_node_id: core::NodeId, report_to: Option<ActorAddress>) -> Self {
        Self {
            core: core::StageController::new(local_node_id),
            report_to,
            command_cursor: 0,
            event_cursor: 0,
        }
    }

    fn drain_outputs(&mut self, ctx: &Ctx) {
        let Some(report_to) = self.report_to else {
            self.command_cursor = self.core.commands().len();
            self.event_cursor = self.core.events().len();
            return;
        };

        for command in &self.core.commands()[self.command_cursor..] {
            let _ = ctx.send(report_to, StageControllerReport::Command(command.clone()));
        }
        self.command_cursor = self.core.commands().len();

        for event in &self.core.events()[self.event_cursor..] {
            let _ = ctx.send(report_to, StageControllerReport::Lifecycle(event.clone()));
        }
        self.event_cursor = self.core.events().len();
    }
}

impl ActorInterface for StageControllerActor {
    type Incoming = StageControllerMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            StageControllerMsg::Observe(event) => {
                self.core.observe(event);
                self.drain_outputs(ctx);
            }
            StageControllerMsg::Snapshot { reply_to } => {
                let _ = ctx.send(
                    reply_to,
                    StageControllerReport::Snapshot {
                        commands: self.core.commands().to_vec(),
                        events: self.core.events().to_vec(),
                    },
                );
            }
        }
    }
}

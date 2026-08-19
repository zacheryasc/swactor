use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::EngineHandle;

#[derive(Clone)]
pub struct Tick {
    pub generation: u64,
}

struct Child;

impl ActorInterface for Child {
    type Incoming = Tick;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _message: Tick) {}
}

pub struct Parent;

impl Parent {
    fn spawn_helper(&self, ctx: &Ctx) {
        let _ = ctx.spawn(Child);
    }
}

impl ActorInterface for Parent {
    type Incoming = Tick;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _message: Tick) {
        self.spawn_helper(ctx);
    }
}

pub fn schedule_actor_tick(
    engine: &EngineHandle,
    sender: ExternalSender,
    actor: ActorAddress,
    generation: u64,
) {
    engine.send_after(
        Duration::from_millis(10),
        sender,
        actor,
        Tick { generation },
    );
}

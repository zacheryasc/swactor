use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
#[derive(Clone)]
pub struct Message { tasks: TaskSpawner }
pub struct Worker;
impl ActorInterface for Worker {
    type Incoming = Message;
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: Message) {}
}

use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
pub struct Worker { tasks: TaskSpawner }
impl ActorInterface for Worker {
    type Incoming = ();
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: ()) {}
}

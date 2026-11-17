use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
struct Adapter { tasks: TaskSpawner }
struct Envelope { adapter: Option<std::sync::Arc<Adapter>> }
pub struct Worker { state: Box<Envelope> }
impl ActorInterface for Worker {
    type Incoming = ();
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: ()) {}
}

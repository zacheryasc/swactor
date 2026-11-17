use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
pub struct Worker<T> { state: T }
impl<T: Send + 'static> ActorInterface for Worker<T> {
    type Incoming = ();
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: ()) {}
}
pub fn construct(tasks: TaskSpawner) {
    let _actor = Worker { state: Some(tasks) };
}

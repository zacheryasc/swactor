use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
static TASKS: std::sync::OnceLock<TaskSpawner> = std::sync::OnceLock::new();
pub struct Worker;
impl ActorInterface for Worker {
    type Incoming = ();
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: ()) {
        let work = || TASKS.get().unwrap().spawn(async {});
        work();
    }
}

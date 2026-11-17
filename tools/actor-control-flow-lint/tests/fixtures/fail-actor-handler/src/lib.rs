use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
type Authority = TaskSpawner;
static TASKS: std::sync::OnceLock<Authority> = std::sync::OnceLock::new();
pub struct Worker;
impl ActorInterface for Worker {
    type Incoming = ();
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: ()) {
        let tasks: Authority = TASKS.get().unwrap().clone();
        tasks.spawn(async {});
    }
}

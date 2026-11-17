use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend, TaskSpawner};
struct Adapter { tasks: TaskSpawner }
impl Adapter {
    fn new(tasks: TaskSpawner) -> Self { Self { tasks } }
    fn start(&self) { self.tasks.clone().spawn(async {}); }
}
fn main() {
    let backend = SteppingBackend::new();
    let engine = Engine::new(RuntimeParts::new(RuntimeConfig::default()), backend.clone()).unwrap();
    Adapter::new(engine.task_spawner()).start();
    backend.step();
}

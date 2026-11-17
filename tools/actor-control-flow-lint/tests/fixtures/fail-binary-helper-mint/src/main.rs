use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend, TaskSpawner};
fn main() {
    fn helper(engine: &Engine) -> TaskSpawner { engine.task_spawner() }
    let engine = Engine::new(RuntimeParts::new(RuntimeConfig::default()), SteppingBackend::new()).unwrap();
    let _ = helper(&engine);
}

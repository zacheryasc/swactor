use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend, TaskSpawner};
fn helper(engine: &Engine) -> TaskSpawner { engine.task_spawner() }
#[test]
fn composition() {
    let engine = Engine::new(RuntimeParts::new(RuntimeConfig::default()), SteppingBackend::new()).unwrap();
    let _ = helper(&engine);
}

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend, TaskSpawner};
fn install(tasks: TaskSpawner, observed: Arc<AtomicBool>) {
    tasks.spawn(async move { observed.store(true, Ordering::SeqCst); });
}
#[test]
fn deterministic_composition() {
    let backend = SteppingBackend::new();
    let engine = Engine::new(RuntimeParts::new(RuntimeConfig::default()), backend.clone()).unwrap();
    let observed = Arc::new(AtomicBool::new(false));
    let compose = || install(engine.task_spawner(), observed.clone());
    compose();
    backend.step();
    assert!(observed.load(Ordering::SeqCst));
    let (sender, receiver) = std::sync::mpsc::channel();
    sender.send(()).unwrap();
    receiver.recv().unwrap();
}
#[test]
fn process_coordination_at_root() {
    let _ = std::process::Command::new("true").status();
}

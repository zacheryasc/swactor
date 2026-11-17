use swactor_engine::{ExecutionBackend, SteppingBackend};
pub fn concrete(backend: &SteppingBackend) {
    backend.spawn(Box::pin(async {}));
    <SteppingBackend as ExecutionBackend>::spawn_blocking(backend, Box::new(|| {}));
}
pub fn generic(backend: &impl ExecutionBackend) {
    backend.spawn(Box::pin(async {}));
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeImageSpec;

impl NodeImageSpec {
    pub fn new(_image: impl Into<String>) -> Self {
        Self
    }

    pub fn worker_runtime(self, _worker_runtime: WorkerRuntimeSpec) -> Self {
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerRuntimeSpec {
    DumbProcess,
}

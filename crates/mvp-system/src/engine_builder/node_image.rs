#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeImageSpec {
    pub image: String,
    pub binary: String,
    pub worker_runtime: WorkerRuntimeSpec,
}

impl NodeImageSpec {
    pub fn new(image: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            binary: "mvp-node".to_owned(),
            worker_runtime: WorkerRuntimeSpec::External {
                name: "node-image-default".to_owned(),
            },
        }
    }

    pub fn binary(mut self, binary: impl Into<String>) -> Self {
        self.binary = binary.into();
        self
    }

    pub fn worker_runtime(mut self, worker_runtime: WorkerRuntimeSpec) -> Self {
        self.worker_runtime = worker_runtime;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerRuntimeSpec {
    DumbProcess,
    TinygradCuda {
        worker_script: String,
        device_env: String,
    },
    External {
        name: String,
    },
}

use swactor_engine::TaskSpawner;

pub struct TransportPump {
    tasks: TaskSpawner,
}

impl TransportPump {
    pub fn new(tasks: TaskSpawner) -> Self {
        Self { tasks }
    }

    pub fn start(&self) {
        let tasks = self.tasks.clone();
        tasks.spawn(async {
            let _io_type: Option<tokio::net::TcpStream> = None;
        });
    }
}

pub fn owned_io(
    blocking: &swactor_engine::BlockingWorkSender,
) -> std::io::Result<swactor_engine::OwnedIoThread<()>> {
    blocking.spawn_owned_thread("fixture-owned-io", || {})
}

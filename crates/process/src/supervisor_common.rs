use std::io::{self, Read};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_queue::SegQueue;
use swactor_engine::{BlockingWorkSender, OwnedIoThread};

use crate::types::ExitStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ThreadCommand {
    Stop { kill_after: Option<Duration> },
    ShutdownNow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ThreadEvent {
    Started { pid: u32 },
    SpawnFailed { error: String },
    Exited { status: ExitStatus },
    Output { stderr: bool, bytes: Vec<u8> },
    Error { error: String },
    ThreadFinished,
}

#[derive(Clone)]
pub(crate) struct ThreadEventSink {
    queue: Arc<SegQueue<ThreadEvent>>,
    wake: Arc<dyn Fn() + Send + Sync>,
}

pub(crate) struct ThreadEventReceiver {
    queue: Arc<SegQueue<ThreadEvent>>,
}

pub(crate) fn thread_event_channel(
    wake: impl Fn() + Send + Sync + 'static,
) -> (ThreadEventSink, ThreadEventReceiver) {
    let queue = Arc::new(SegQueue::new());
    (
        ThreadEventSink {
            queue: queue.clone(),
            wake: Arc::new(wake),
        },
        ThreadEventReceiver { queue },
    )
}

impl ThreadEventSink {
    pub(crate) fn push(&self, event: ThreadEvent) {
        self.queue.push(event);
        (self.wake)();
    }
}

impl ThreadEventReceiver {
    pub(crate) fn drain(&self) -> Vec<ThreadEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.queue.pop() {
            events.push(event);
        }
        events
    }
}

pub(crate) fn spawn_output_reader(
    blocking: &BlockingWorkSender,
    mut reader: impl Read + Send + 'static,
    events: ThreadEventSink,
    stderr: bool,
) -> io::Result<OwnedIoThread<()>> {
    blocking.spawn_owned_thread("process-output-reader", move || {
        let mut buffer = vec![0_u8; 4_096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => events.push(ThreadEvent::Output {
                    stderr,
                    bytes: buffer[..read].to_vec(),
                }),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    })
}

pub(crate) fn join_output_readers(readers: &mut Vec<OwnedIoThread<()>>) {
    for reader in readers.drain(..) {
        let _ = reader.join();
    }
}

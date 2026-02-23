pub mod action;
pub mod actor;
pub mod driver;
pub mod event;
pub mod local;
pub mod message;
pub mod mock;
pub mod queue;
pub mod session;
pub mod spawn;
pub mod subscriber;
pub mod types;
pub mod waker;

#[cfg(feature = "ssh")]
pub mod ssh;

pub use action::{OutputStream, ProcessAction};
pub use actor::ProcessActor;
pub use driver::ProcessDriver;
pub use event::ProcessEvent;
pub use local::LocalDriver;
pub use message::{ProcessCommand, ProcessNotification};
pub use mock::MockDriver;
pub use queue::EventQueue;
pub use session::{ProcessSession, ProcessState};
pub use spawn::{spawn_local_process, spawn_process};
pub use subscriber::SubscriberSet;
pub use types::{ExitStatus, FlowControl, ProcessError, ProcessMode, ProcessSpec, PtySize, Signal};
pub use waker::ProcessWaker;

#[cfg(feature = "ssh")]
pub use ssh::{SshConfig, SshDriver};
#[cfg(feature = "ssh")]
pub use spawn::spawn_ssh_process;

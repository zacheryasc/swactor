#![cfg_attr(windows, feature(windows_process_extensions_raw_attribute))]

mod actor;
mod lifecycle;
mod message;
mod operations;
#[cfg(unix)]
mod resources;
mod spawn;
mod stdin;
#[cfg(not(windows))]
mod supervisor;
#[cfg(windows)]
#[path = "supervisor_windows.rs"]
mod supervisor;
mod supervisor_common;
mod terminal;
mod types;

pub use lifecycle::{ProcessLifecycleObservability, ProcessOutputConfig};
pub use message::{ProcessCommand, ProcessOutput};
#[cfg(unix)]
pub use operations::request_child_termination;
#[cfg(target_os = "linux")]
pub use operations::spawn_os_stop_signal_wait;
#[cfg(unix)]
pub use operations::spawn_unix_stream_listener;
pub use operations::{
    CommandOutputObservation, LineReaderHandle, ProcessExitObservation,
    ProcessStopSignal, ProcessStream, ProcessStreamObservation,
    SupervisedProcessObservation, child_kill, child_try_wait, child_wait, child_wait_with_output,
    command_output, command_spawn, command_status, spawn_child_wait, spawn_command_output,
    spawn_detached_command_status, spawn_line_channel, spawn_line_reader,
    spawn_mapped_line_channel, spawn_mapped_line_reader, spawn_owned_line_reader,
    spawn_shared_child_wait, spawn_stdin_command_wait, spawn_stop_channel_wait, wait_for_path,
};
#[cfg(target_os = "linux")]
pub use operations::{
    FollowProcessFile, ProcessIdentity, ProcessWatch, SupervisedChild, cancel_shared_child,
    find_process_identities_by_environment, find_process_identities_with_retry,
    spawn_identity_exit_wait, stop_shared_child_with_input, wait_readable, watch_process,
};
#[cfg(target_os = "linux")]
pub use operations::{cancel_process_group, terminate_process_group};
#[cfg(target_os = "linux")]
pub use operations::{command_output_owned, command_output_until, command_status_observed};
#[cfg(unix)]
pub use resources::{ProcessResourceError, ProcessSpawnResources};
#[cfg(unix)]
pub use spawn::spawn_local_process_with_resources;
pub use spawn::{send_process_command, spawn_local_process, spawn_local_process_with_stdin, spawn_local_terminal};
pub use stdin::ProcessStdin;
pub use terminal::{TerminalInput, TerminalSize};
pub use types::{ExitStatus, ProcessSpec};

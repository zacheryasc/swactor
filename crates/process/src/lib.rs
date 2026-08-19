mod actor;
mod lifecycle;
mod message;
mod operations;
mod spawn;
mod supervisor;
mod types;

pub mod pipeline;
pub mod yaml;

pub use lifecycle::{ProcessLifecycleObservability, ProcessOutputConfig};
pub use message::{ProcessCommand, ProcessOutput};
#[cfg(unix)]
pub use operations::request_child_termination;
#[cfg(target_os = "linux")]
pub use operations::spawn_os_stop_signal_wait;
#[cfg(unix)]
pub use operations::spawn_unix_stream_listener;
#[cfg(target_os = "linux")]
pub use operations::terminate_process_group;
pub use operations::{
    CommandOutputObservation, FollowProcessFile, LineReaderHandle, ProcessExitObservation,
    ProcessIdentity, ProcessStopSignal, ProcessStream, ProcessStreamObservation, child_kill,
    child_try_wait, child_wait, child_wait_with_output, command_output, command_spawn,
    command_status, find_process_identities_by_environment, find_process_identities_with_retry,
    spawn_child_wait, spawn_command_output, spawn_detached_command_status,
    spawn_identity_exit_wait, spawn_line_channel, spawn_line_reader, spawn_mapped_line_channel,
    spawn_mapped_line_reader, spawn_shared_child_wait, spawn_stdin_command_wait,
    spawn_stop_channel_wait, wait_for_path, wait_shared_child_or_kill,
};
pub use pipeline::{
    JobComplete, JobDefinition, JobFailure, JobId, JobProgress, JobStatus, JobSuccess,
    LocalPipelineConfig, LocalStartJob, PipelineId, PipelineStatus,
};
pub use spawn::{send_process_command, spawn_local_process};
pub use types::{ExitStatus, ProcessSpec};

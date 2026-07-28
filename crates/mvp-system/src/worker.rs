//! MVP GPU worker control public surface.

pub mod control;
pub mod device_bridge;
pub mod egress;
pub mod process_adapter;

pub mod process {
    pub use swactor_process::{
        ProcessCommand, ProcessLifecycleObservability, ProcessOutput, ProcessOutputConfig,
        ProcessSpec, send_process_command, spawn_local_process,
    };
}

pub use control::*;

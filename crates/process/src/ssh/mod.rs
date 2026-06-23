pub mod config;
mod task;

use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;

use crate::action::ProcessAction;
use crate::event::ProcessEvent;
use crate::types::{EventQueue, ProcessDriver, ProcessWaker};

pub use config::SshConfig;
use task::SshCommand;

/// A `ProcessDriver` that runs processes on remote hosts over SSH.
///
/// Commands are sent via a tokio mpsc channel to a background async task
/// that manages the SSH connection. Events flow back through the shared
/// `EventQueue` + `ProcessWaker` (same pattern as `LocalDriver`).
pub struct SshDriver {
    queue: EventQueue,
    waker_slot: Arc<OnceLock<ProcessWaker>>,
    command_tx: Option<mpsc::UnboundedSender<SshCommand>>,
    command_rx: Option<mpsc::UnboundedReceiver<SshCommand>>,
    tokio_handle: tokio::runtime::Handle,
    ssh_config: SshConfig,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl SshDriver {
    pub fn new(
        queue: EventQueue,
        waker_slot: Arc<OnceLock<ProcessWaker>>,
        tokio_handle: tokio::runtime::Handle,
        ssh_config: SshConfig,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            queue,
            waker_slot,
            command_tx: Some(tx),
            command_rx: Some(rx),
            tokio_handle,
            ssh_config,
            task_handle: None,
        }
    }
}

impl ProcessDriver for SshDriver {
    fn execute(&mut self, action: ProcessAction) {
        match action {
            ProcessAction::SpawnProcess { spec } => {
                let Some(rx) = self.command_rx.take() else {
                    return;
                };
                let queue = self.queue.clone();
                let waker_slot = self.waker_slot.clone();
                // Move the ssh_config out — we only need it once for connection
                let config = SshConfig {
                    host: self.ssh_config.host.clone(),
                    port: self.ssh_config.port,
                    username: self.ssh_config.username.clone(),
                    key_file: self.ssh_config.key_file.clone(),
                    key_passphrase: self.ssh_config.key_passphrase.clone(),
                };
                self.task_handle = Some(
                    self.tokio_handle
                        .spawn(task::run_ssh_session(config, spec, queue, waker_slot, rx)),
                );
            }
            ProcessAction::WriteStdin { data } => {
                if let Some(ref tx) = self.command_tx {
                    let _ = tx.send(SshCommand::WriteStdin(data));
                }
            }
            ProcessAction::SendSignal { signal } => {
                if let Some(ref tx) = self.command_tx {
                    let _ = tx.send(SshCommand::SendSignal(signal));
                }
            }
            ProcessAction::ResizePty { size } => {
                if let Some(ref tx) = self.command_tx {
                    let _ = tx.send(SshCommand::ResizePty {
                        cols: size.cols,
                        rows: size.rows,
                    });
                }
            }
            ProcessAction::CloseStdin => {
                if let Some(ref tx) = self.command_tx {
                    let _ = tx.send(SshCommand::CloseStdin);
                }
            }
            ProcessAction::ScheduleKillTimeout { duration } => {
                if let Some(ref tx) = self.command_tx {
                    let _ = tx.send(SshCommand::ScheduleKillTimeout(duration));
                }
            }
            // Notification actions are not driver commands
            _ => {}
        }
    }

    fn poll(&mut self) -> Vec<ProcessEvent> {
        self.queue.drain()
    }
}

impl Drop for SshDriver {
    fn drop(&mut self) {
        // Drop sender to signal the task to shut down
        self.command_tx.take();
        // Abort the background task if still running
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}

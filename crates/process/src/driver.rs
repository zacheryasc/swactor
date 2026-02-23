use crate::action::ProcessAction;
use crate::event::ProcessEvent;

/// Abstraction over the mechanism that actually runs a process.
///
/// Implementations translate `ProcessAction` commands into real I/O (or mock I/O)
/// and produce `ProcessEvent`s by polling for state changes.
pub trait ProcessDriver: Send {
    /// Execute an action (spawn, write stdin, send signal, etc.).
    fn execute(&mut self, action: ProcessAction);

    /// Poll for new events from the underlying process.
    fn poll(&mut self) -> Vec<ProcessEvent>;
}

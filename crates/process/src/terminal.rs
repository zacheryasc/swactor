use std::io;

use crate::stdin::ProcessStdin;

#[cfg(unix)]
#[path = "terminal_unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "terminal_windows.rs"]
mod platform;
pub(crate) use platform::{TerminalSession, TerminalWriter};
#[cfg(target_os = "linux")]
pub(crate) use platform::cancel_session;
#[cfg(windows)]
pub(crate) use platform::TerminalChild;

/// The visible terminal dimensions, in character cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalSize {
    pub rows: u16,
    pub cols: u16,
}

impl TerminalSize {
    pub(crate) fn validate(self) -> io::Result<()> {
        if self.rows == 0 || self.cols == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "terminal dimensions must be nonzero"));
        }
        #[cfg(windows)]
        if self.rows > i16::MAX as u16 || self.cols > i16::MAX as u16 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "terminal dimensions exceed ConPTY limits"));
        }
        Ok(())
    }
}

/// Ordered, bounded input and window changes for one managed terminal.
///
/// Writes accept at most 16 KiB and acknowledge OS delivery, not shell execution.
/// Control bytes (including Ctrl-C) are interpreted by the native terminal.
/// Dropping this handle does not cancel the process: callers must send
/// `ProcessCommand::Cancel` when their terminal connection reaches EOF.
pub struct TerminalInput {
    pub(crate) input: ProcessStdin,
}

impl TerminalInput {
    pub async fn write_all(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        self.input.write_all(bytes).await
    }

    pub async fn resize(&mut self, size: TerminalSize) -> io::Result<()> {
        size.validate()?;
        self.input.resize(size).await
    }
}

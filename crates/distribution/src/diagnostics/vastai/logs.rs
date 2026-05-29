//! Live log forwarding for the vastai monitoring layer.
//!
//! Frames stdout/stderr lines into ordered [`LogLine`]s (a monotonic counter per
//! stream) and ships them as [`LogBatch`]es through a
//! [`VastaiShipper`](super::shipper::VastaiShipper), so worker output streams to
//! the collector live instead of only being recoverable after the fact. Lines are
//! truncated to a byte cap to keep a runaway logger from flooding the pipeline.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::diagnostics::wall_ms_now;

use super::record::{LogBatch, LogLine, LogStream};
use super::shipper::VastaiShipper;

/// Default per-line byte cap. Matches the stage actor's stderr ring-buffer cap so
/// the live stream and the crash tail agree on truncation.
pub const DEFAULT_MAX_LINE_BYTES: usize = 4 * 1024;
/// Flush when this many lines accumulate on a stream, regardless of the timer.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 64;

#[derive(Debug, Clone)]
pub struct LogForwarderConfig {
    pub max_line_bytes: usize,
    pub flush_threshold: usize,
    pub flush_interval: Duration,
}

impl Default for LogForwarderConfig {
    fn default() -> Self {
        Self {
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            flush_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Default)]
struct Buf {
    stdout: Vec<LogLine>,
    stderr: Vec<LogLine>,
    stdout_n: u64,
    stderr_n: u64,
}

/// Cheap, cloneable handle. Clones share the buffer and shipper, so the stage
/// actor can hold one and the flush task another.
#[derive(Clone)]
pub struct LogForwarder {
    inner: Arc<Mutex<Buf>>,
    shipper: VastaiShipper,
    config: LogForwarderConfig,
}

impl LogForwarder {
    pub fn new(shipper: VastaiShipper, config: LogForwarderConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Buf::default())),
            shipper,
            config,
        }
    }

    /// Record one line from `stream`. Non-blocking (locks a mutex only). Flushes
    /// inline when the stream's buffer crosses the threshold so a burst doesn't
    /// wait for the timer.
    pub fn push(&self, stream: LogStream, text: &str) {
        let text = truncate(text, self.config.max_line_bytes);
        let over = {
            let mut b = self.inner.lock().expect("log buf mutex poisoned");
            // stdout and stderr keep independent line counters; vast-request-log
            // lines are folded into the stderr stream.
            let to_stdout = matches!(stream, LogStream::Stdout);
            let line = if to_stdout {
                let l = b.stdout_n;
                b.stdout_n += 1;
                l
            } else {
                let l = b.stderr_n;
                b.stderr_n += 1;
                l
            };
            let entry = LogLine {
                wall_ms: wall_ms_now(),
                line,
                text,
            };
            let buf = if to_stdout { &mut b.stdout } else { &mut b.stderr };
            buf.push(entry);
            buf.len() >= self.config.flush_threshold
        };
        if over {
            self.flush();
        }
    }

    /// Ship any buffered lines now (one batch per non-empty stream).
    pub fn flush(&self) {
        let (stdout, stderr) = {
            let mut b = self.inner.lock().expect("log buf mutex poisoned");
            (std::mem::take(&mut b.stdout), std::mem::take(&mut b.stderr))
        };
        if !stdout.is_empty() {
            self.shipper.logs(LogBatch {
                stream: LogStream::Stdout,
                lines: stdout,
            });
        }
        if !stderr.is_empty() {
            self.shipper.logs(LogBatch {
                stream: LogStream::Stderr,
                lines: stderr,
            });
        }
    }

    /// Spawn a background task that flushes on the configured interval, catching
    /// trickles that never hit the threshold. Returns a handle; on
    /// [`LogForwarder`] drop the task ends when `stop` is notified.
    pub fn spawn_flusher(&self) -> LogFlusherHandle {
        let me = self.clone();
        let stop = Arc::new(tokio::sync::Notify::new());
        let stop2 = Arc::clone(&stop);
        let interval = self.config.flush_interval;
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = stop2.notified() => { me.flush(); break; }
                    _ = ticker.tick() => me.flush(),
                }
            }
        });
        LogFlusherHandle { task, stop }
    }
}

/// Handle for the periodic flush task.
pub struct LogFlusherHandle {
    task: tokio::task::JoinHandle<()>,
    stop: Arc<tokio::sync::Notify>,
}

impl LogFlusherHandle {
    /// Flush remaining lines and stop the task.
    pub async fn shutdown(self) {
        self.stop.notify_waiters();
        let _ = self.task.await;
    }
}

/// Truncate `text` to at most `max` bytes on a char boundary, trimming a
/// trailing newline so each `LogLine` is one logical line.
fn truncate(text: &str, max: usize) -> String {
    let text = text.strip_suffix('\n').unwrap_or(text);
    let text = text.strip_suffix('\r').unwrap_or(text);
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_caps_bytes_and_strips_trailing_newline() {
        assert_eq!(truncate("hello\n", 100), "hello");
        assert_eq!(truncate("hello\r\n", 100), "hello");
        assert_eq!(truncate("abcdef", 3), "abc");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        // "é" is two bytes; capping at 3 must not split it.
        let s = "aé";
        let out = truncate(s, 2);
        assert_eq!(out, "a");
        assert!(out.is_char_boundary(out.len()));
    }
}

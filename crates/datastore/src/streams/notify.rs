use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// Bit positions for notification flags.
pub const DATA_READY: u8 = 0b0000_0001;
pub const WRITE_READY: u8 = 0b0000_0010;
pub const CLOSED: u8 = 0b0000_0100;
pub const ERROR: u8 = 0b0000_1000;

/// Atomic bitflags for coalescing notifications to an actor.
///
/// Multiple data-plane tasks may set flags concurrently. The actor clears
/// flags after handling them. If a flag is already set when a task tries
/// to set it, the notification is coalesced (deduplicated).
pub struct NotifyFlag {
    flags: AtomicU8,
}

impl NotifyFlag {
    pub fn new() -> Self {
        NotifyFlag {
            flags: AtomicU8::new(0),
        }
    }

    /// Set a flag bit. Returns `true` if the bit was previously clear
    /// (i.e., this is a new notification that should trigger an inject).
    /// Returns `false` if already set (coalesced, no inject needed).
    pub fn set(&self, kind: u8) -> bool {
        let prev = self.flags.fetch_or(kind, Ordering::AcqRel);
        (prev & kind) == 0
    }

    /// Clear a flag bit. Called by the actor after handling.
    pub fn clear(&self, kind: u8) {
        self.flags.fetch_and(!kind, Ordering::AcqRel);
    }

    /// Read all currently-set flags.
    pub fn read(&self) -> u8 {
        self.flags.load(Ordering::Acquire)
    }

    /// Check if a specific flag is set.
    pub fn is_set(&self, kind: u8) -> bool {
        (self.read() & kind) != 0
    }
}

impl Default for NotifyFlag {
    fn default() -> Self {
        Self::new()
    }
}

/// The kind of stream event delivered to an actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEventKind {
    DataReady,
    WriteReady,
    Closed,
    Error,
}

/// A lightweight notification message injected into an actor's mailbox.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub stream_id: crate::streams::types::StreamId,
    pub kind: StreamEventKind,
}

/// Sink that data-plane tasks use to inject notifications into the actor system.
///
/// Holds the shared `NotifyFlag` for coalescing, and an inject closure
/// that sends a `StreamEvent` into the actor's mailbox when a truly new
/// notification needs to fire.
pub struct NotifySink {
    flag: Arc<NotifyFlag>,
    inject: Box<dyn Fn(StreamEvent) + Send + Sync>,
    stream_id: crate::streams::types::StreamId,
}

impl NotifySink {
    pub fn new(
        flag: Arc<NotifyFlag>,
        stream_id: crate::streams::types::StreamId,
        inject: impl Fn(StreamEvent) + Send + Sync + 'static,
    ) -> Self {
        NotifySink {
            flag,
            inject: Box::new(inject),
            stream_id,
        }
    }

    /// Notify the actor of a stream event. Coalesces duplicate notifications.
    pub fn notify(&self, kind_flag: u8, kind: StreamEventKind) {
        if self.flag.set(kind_flag) {
            (self.inject)(StreamEvent {
                stream_id: self.stream_id,
                kind,
            });
        }
    }

    /// Convenience: notify data ready.
    pub fn data_ready(&self) {
        self.notify(DATA_READY, StreamEventKind::DataReady);
    }

    /// Convenience: notify write ready.
    pub fn write_ready(&self) {
        self.notify(WRITE_READY, StreamEventKind::WriteReady);
    }

    /// Convenience: notify closed.
    pub fn closed(&self) {
        self.notify(CLOSED, StreamEventKind::Closed);
    }

    /// Convenience: notify error.
    pub fn error(&self) {
        self.notify(ERROR, StreamEventKind::Error);
    }

    /// Access the shared flag for the actor side to clear bits.
    pub fn flag(&self) -> &Arc<NotifyFlag> {
        &self.flag
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_returns_true_first_time_false_on_duplicate() {
        let flag = NotifyFlag::new();
        assert!(flag.set(DATA_READY));
        assert!(!flag.set(DATA_READY));
    }

    #[test]
    fn clear_allows_re_notification() {
        let flag = NotifyFlag::new();
        assert!(flag.set(DATA_READY));
        flag.clear(DATA_READY);
        assert!(flag.set(DATA_READY));
    }

    #[test]
    fn independent_flags_do_not_interfere() {
        let flag = NotifyFlag::new();
        assert!(flag.set(DATA_READY));
        assert!(flag.set(WRITE_READY));
        assert!(!flag.set(DATA_READY)); // still set
        flag.clear(DATA_READY);
        assert!(flag.is_set(WRITE_READY)); // unaffected
        assert!(!flag.is_set(DATA_READY));
    }

    #[test]
    fn read_shows_all_set_flags() {
        let flag = NotifyFlag::new();
        flag.set(DATA_READY);
        flag.set(ERROR);
        let bits = flag.read();
        assert_eq!(bits & DATA_READY, DATA_READY);
        assert_eq!(bits & ERROR, ERROR);
        assert_eq!(bits & WRITE_READY, 0);
    }
}

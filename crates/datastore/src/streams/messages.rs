use std::fmt;
use std::sync::{Arc, Mutex};

use iroh::endpoint::Connection;
use swactor::actor::ActorAddress;

use crate::streams::handle::StreamHandle;
use crate::streams::types::{StreamConfig, StreamError, StreamId, StreamMode};

// ─── OneShot ────────────────────────────────────────────────────────────

/// Clone-friendly wrapper for non-Clone data (StreamHandle, Connection).
///
/// The first `.take()` extracts the value; subsequent clones/takes get `None`.
/// This allows non-Clone payloads to live inside Clone message enums required
/// by the actor system's `Message` trait.
pub struct OneShot<T>(Arc<Mutex<Option<T>>>);

impl<T> OneShot<T> {
    pub fn new(val: T) -> Self {
        OneShot(Arc::new(Mutex::new(Some(val))))
    }

    /// Extract the value. Returns `Some` exactly once; all subsequent calls
    /// (including from clones) return `None`.
    pub fn take(&self) -> Option<T> {
        self.0.lock().unwrap().take()
    }
}

impl<T> Clone for OneShot<T> {
    fn clone(&self) -> Self {
        OneShot(Arc::clone(&self.0))
    }
}

impl<T> fmt::Debug for OneShot<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let has_value = self.0.lock().unwrap().is_some();
        write!(f, "OneShot({})", if has_value { "Some" } else { "None" })
    }
}

// SAFETY: OneShot<T> is Send+Sync because access is guarded by Mutex,
// and Arc provides shared ownership.
unsafe impl<T: Send> Send for OneShot<T> {}
unsafe impl<T: Send> Sync for OneShot<T> {}

// ─── StreamManagerMsg ───────────────────────────────────────────────────

/// Messages sent TO the StreamManager actor.
#[derive(Clone, Debug)]
pub enum StreamManagerMsg {
    /// Open a new stream to a remote node.
    Open {
        target_node: [u8; 32],
        mode: StreamMode,
        config: StreamConfig,
        /// The requesting actor's address; receives StreamNotification.
        reply_to: ActorAddress,
    },
    /// Accept an offered incoming stream.
    Accept {
        stream_id: StreamId,
        /// Receives StreamNotification::StreamReady.
        reply_to: ActorAddress,
    },
    /// Reject an offered incoming stream.
    Reject {
        stream_id: StreamId,
    },
    /// Register as a stream listener for a given mode.
    Listen {
        mode: StreamMode,
        /// Receives StreamNotification::StreamOffer.
        listener: ActorAddress,
    },
    /// Close a stream.
    Close {
        stream_id: StreamId,
    },

    // -- Internal (from tokio tasks back to StreamManager) --
    /// Incoming connection from the accept bridge task.
    IncomingConnection {
        node_id: [u8; 32],
        stream_id: StreamId,
        mode: StreamMode,
        config: StreamConfig,
        /// QUIC connection for data stripes.
        conn: OneShot<Connection>,
    },
    /// Async open task completed.
    OpenCompleted {
        stream_id: StreamId,
        reply_to: ActorAddress,
        result: OneShot<Result<StreamHandle, StreamError>>,
    },
    /// Async accept task completed (data-plane tasks spawned).
    AcceptCompleted {
        stream_id: StreamId,
        reply_to: ActorAddress,
        result: OneShot<Result<StreamHandle, StreamError>>,
    },
}

// ─── StreamNotification ─────────────────────────────────────────────────

/// Notifications sent FROM StreamManager TO user actors.
#[derive(Clone, Debug)]
pub enum StreamNotification {
    /// A stream is ready for use (open or accept completed successfully).
    StreamReady {
        stream_id: StreamId,
        handle: OneShot<StreamHandle>,
    },
    /// A remote node is offering a new stream.
    StreamOffer {
        stream_id: StreamId,
        mode: StreamMode,
        metadata: Vec<u8>,
        from_node: [u8; 32],
    },
    /// A stream was closed.
    StreamClosed {
        stream_id: StreamId,
        reason: Option<StreamError>,
    },
    /// A stream open/accept failed.
    StreamFailed {
        stream_id: StreamId,
        error: StreamError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oneshot_take_once_semantics() {
        let os = OneShot::new(42u64);
        assert_eq!(os.take(), Some(42));
        assert_eq!(os.take(), None);
    }

    #[test]
    fn oneshot_clone_shares_value() {
        let os = OneShot::new("hello".to_string());
        let clone = os.clone();
        // First take from clone succeeds
        assert_eq!(clone.take(), Some("hello".to_string()));
        // Original now gets None
        assert_eq!(os.take(), None);
    }

    #[test]
    fn oneshot_debug_format() {
        let os = OneShot::new(1);
        assert_eq!(format!("{os:?}"), "OneShot(Some)");
        os.take();
        assert_eq!(format!("{os:?}"), "OneShot(None)");
    }

    fn assert_message<T: 'static + Clone + Send + Sync>() {}

    #[test]
    fn stream_manager_msg_is_message() {
        assert_message::<StreamManagerMsg>();
    }

    #[test]
    fn stream_notification_is_message() {
        assert_message::<StreamNotification>();
    }
}

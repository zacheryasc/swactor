//! Convenience extension traits for actors that use streams.
//!
//! `CtxStreams` wraps StreamManager message construction for use inside actor
//! handlers (defaults `reply_to`/`listener` to `ctx.self_addr()`).
//!
//! `RuntimeStreams` provides the same operations from a `Runtime` handle,
//! requiring explicit addresses since there's no implicit "self".

use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::Runtime;
use swactor_std::CtxNaming;
use swactor_std::RuntimeNaming;

use crate::messages::StreamManagerMsg;
use crate::types::{StreamConfig, StreamError, StreamId, StreamMode};

fn mgr_not_found() -> StreamError {
    StreamError::BrokenPipe("StreamManager not found in name registry".into())
}

/// Stream operations available inside actor handlers via `Ctx`.
///
/// All methods default `reply_to` / `listener` to `ctx.self_addr()`.
pub trait CtxStreams {
    /// Open a new stream to a remote node.
    fn stream_open(
        &self,
        target_node: [u8; 32],
        mode: StreamMode,
        config: StreamConfig,
    ) -> Result<(), StreamError>;

    /// Register as a stream listener for the given mode.
    fn stream_listen(&self, mode: StreamMode) -> Result<(), StreamError>;

    /// Accept an offered incoming stream.
    fn stream_accept(&self, stream_id: StreamId) -> Result<(), StreamError>;

    /// Reject an offered incoming stream.
    fn stream_reject(&self, stream_id: StreamId) -> Result<(), StreamError>;

    /// Close a stream.
    fn stream_close(&self, stream_id: StreamId) -> Result<(), StreamError>;
}

impl CtxStreams for Ctx<'_> {
    fn stream_open(
        &self,
        target_node: [u8; 32],
        mode: StreamMode,
        config: StreamConfig,
    ) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send(
            mgr,
            StreamManagerMsg::Open {
                target_node,
                mode,
                config,
                reply_to: self.self_addr(),
            },
        )
        .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_listen(&self, mode: StreamMode) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send(
            mgr,
            StreamManagerMsg::Listen {
                mode,
                listener: self.self_addr(),
            },
        )
        .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_accept(&self, stream_id: StreamId) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send(
            mgr,
            StreamManagerMsg::Accept {
                stream_id,
                reply_to: self.self_addr(),
            },
        )
        .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_reject(&self, stream_id: StreamId) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send(mgr, StreamManagerMsg::Reject { stream_id })
            .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_close(&self, stream_id: StreamId) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send(mgr, StreamManagerMsg::Close { stream_id })
            .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }
}

/// Stream operations available from a `Runtime` handle (outside actor handlers).
///
/// Requires explicit `reply_to` / `listener` addresses.
pub trait RuntimeStreams {
    /// Open a new stream to a remote node.
    fn stream_open(
        &self,
        target_node: [u8; 32],
        mode: StreamMode,
        config: StreamConfig,
        reply_to: ActorAddress,
    ) -> Result<(), StreamError>;

    /// Register an address as a stream listener for the given mode.
    fn stream_listen(&self, mode: StreamMode, listener: ActorAddress) -> Result<(), StreamError>;

    /// Accept an offered incoming stream.
    fn stream_accept(
        &self,
        stream_id: StreamId,
        reply_to: ActorAddress,
    ) -> Result<(), StreamError>;

    /// Reject an offered incoming stream.
    fn stream_reject(&self, stream_id: StreamId) -> Result<(), StreamError>;

    /// Close a stream.
    fn stream_close(&self, stream_id: StreamId) -> Result<(), StreamError>;
}

impl RuntimeStreams for Runtime {
    fn stream_open(
        &self,
        target_node: [u8; 32],
        mode: StreamMode,
        config: StreamConfig,
        reply_to: ActorAddress,
    ) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send_to(
            mgr,
            StreamManagerMsg::Open {
                target_node,
                mode,
                config,
                reply_to,
            },
        )
        .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_listen(&self, mode: StreamMode, listener: ActorAddress) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send_to(mgr, StreamManagerMsg::Listen { mode, listener })
            .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_accept(
        &self,
        stream_id: StreamId,
        reply_to: ActorAddress,
    ) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send_to(mgr, StreamManagerMsg::Accept { stream_id, reply_to })
            .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_reject(&self, stream_id: StreamId) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send_to(mgr, StreamManagerMsg::Reject { stream_id })
            .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }

    fn stream_close(&self, stream_id: StreamId) -> Result<(), StreamError> {
        let mgr = self
            .where_is(crate::manager::STREAM_MANAGER_NAME)
            .ok_or_else(mgr_not_found)?;
        self.send_to(mgr, StreamManagerMsg::Close { stream_id })
            .map_err(|e| StreamError::BrokenPipe(e.to_string()))
    }
}

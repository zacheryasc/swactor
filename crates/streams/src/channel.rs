use crate::buffer::FrameBuf;
use crate::types::StreamError;

/// Commands sent from the actor to the send-side data-plane task.
pub enum SendCommand {
    /// A buffer of data to write to the wire.
    Data(FrameBuf),
    /// Flush any partially-filled buffers.
    Flush,
    /// Gracefully close the send side.
    Close,
}

/// Events sent from the send-side data-plane task back to the actor.
#[derive(Debug, Clone)]
pub enum SendEvent {
    /// The data-plane task is ready to accept more data.
    WriteReady,
    /// An error occurred on the send side.
    Error(StreamError),
    /// The send side has been closed.
    Closed,
}

/// Commands sent from the actor to the recv-side data-plane task.
pub enum RecvCommand {
    /// Return a consumed buffer to the pool.
    Consumed(FrameBuf),
    /// Close the receive side.
    Close,
}

/// Events sent from the recv-side data-plane task to the actor.
pub enum RecvEvent {
    /// A buffer of received data.
    Data(FrameBuf),
    /// An error occurred on the recv side.
    Error(StreamError),
    /// The recv side has been closed (all stripes finished).
    Closed,
}

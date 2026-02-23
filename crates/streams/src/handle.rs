use tokio::sync::mpsc;

use crate::buffer::{BufferPool, FrameBuf};
use crate::channel::{RecvCommand, RecvEvent, SendCommand, SendEvent};
use crate::types::{StreamConfig, StreamError, StreamId};

/// Sending half of a stream. Owned by the actor that sends data.
///
/// Uses `try_send`/`try_recv` for non-blocking operation on the actor thread.
pub struct SendHalf {
    stream_id: StreamId,
    cmd_tx: mpsc::Sender<SendCommand>,
    evt_rx: mpsc::Receiver<SendEvent>,
    pool: BufferPool,
    active_buf: Option<FrameBuf>,
}

impl SendHalf {
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Write data into the stream. Returns the number of bytes consumed.
    ///
    /// Fills the active buffer and sends full buffers to the data-plane task.
    /// Returns 0 if the channel is full (backpressure) or the buffer pool
    /// is exhausted. Does NOT block.
    pub fn try_write(&mut self, data: &[u8]) -> Result<usize, StreamError> {
        if data.is_empty() {
            return Ok(0);
        }

        // Ensure we have an active buffer
        if self.active_buf.is_none() {
            self.active_buf = self.pool.checkout();
            if self.active_buf.is_none() {
                return Err(StreamError::BufferExhausted);
            }
        }

        let buf = self.active_buf.as_mut().unwrap();
        let written = buf.write(data);

        // If the buffer is full, send it to the data-plane task
        if buf.is_full() {
            let full_buf = self.active_buf.take().unwrap();
            match self.cmd_tx.try_send(SendCommand::Data(full_buf)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(cmd)) => {
                    // Put the buffer back -- channel is full (backpressure).
                    // Data is already written into the buffer. Next call to
                    // try_write will attempt try_send again.
                    if let SendCommand::Data(buf) = cmd {
                        self.active_buf = Some(buf);
                    }
                    return Ok(written);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(StreamError::Disconnected);
                }
            }
        }

        Ok(written)
    }

    /// Flush any partially-filled buffer to the data-plane task.
    pub fn flush(&mut self) -> Result<(), StreamError> {
        if let Some(buf) = self.active_buf.take() {
            if buf.remaining() > 0 || buf.written().len() > 0 {
                self.cmd_tx
                    .try_send(SendCommand::Data(buf))
                    .map_err(|_| StreamError::Disconnected)?;
            } else {
                self.pool.checkin(buf);
            }
        }
        self.cmd_tx
            .try_send(SendCommand::Flush)
            .map_err(|_| StreamError::Disconnected)?;
        Ok(())
    }

    /// Close the send side of the stream.
    pub fn close(&mut self) -> Result<(), StreamError> {
        if let Some(buf) = self.active_buf.take() {
            if buf.written().len() > 0 {
                let _ = self.cmd_tx.try_send(SendCommand::Data(buf));
            } else {
                self.pool.checkin(buf);
            }
        }
        self.cmd_tx
            .try_send(SendCommand::Close)
            .map_err(|_| StreamError::Disconnected)?;
        Ok(())
    }

    /// Poll for events from the data-plane task (non-blocking).
    pub fn try_recv_event(&mut self) -> Option<SendEvent> {
        self.evt_rx.try_recv().ok()
    }
}

/// Receiving half of a stream. Owned by the actor that receives data.
pub struct RecvHalf {
    stream_id: StreamId,
    evt_rx: mpsc::Receiver<RecvEvent>,
    cmd_tx: mpsc::Sender<RecvCommand>,
    pool: BufferPool,
    active_buf: Option<FrameBuf>,
}

impl RecvHalf {
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Read data from the stream. Returns the number of bytes read.
    ///
    /// Drains the active buffer, then pulls new buffers from the channel.
    /// Returns 0 if no data is currently available. Does NOT block.
    pub fn try_read(&mut self, dst: &mut [u8]) -> Result<usize, StreamError> {
        if dst.is_empty() {
            return Ok(0);
        }

        // Drain any active buffer first
        if let Some(buf) = &mut self.active_buf {
            if buf.remaining() > 0 {
                let read = buf.read(dst);
                if buf.is_empty() {
                    let buf = self.active_buf.take().unwrap();
                    self.pool.checkin(buf);
                }
                return Ok(read);
            } else {
                let buf = self.active_buf.take().unwrap();
                self.pool.checkin(buf);
            }
        }

        // Try to pull a new buffer from the channel
        match self.evt_rx.try_recv() {
            Ok(RecvEvent::Data(mut buf)) => {
                let read = buf.read(dst);
                if buf.is_empty() {
                    self.pool.checkin(buf);
                } else {
                    self.active_buf = Some(buf);
                }
                Ok(read)
            }
            Ok(RecvEvent::Error(e)) => Err(e),
            Ok(RecvEvent::Closed) => Err(StreamError::Closed),
            Err(mpsc::error::TryRecvError::Empty) => Ok(0),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(StreamError::Disconnected),
        }
    }

    /// Check if data is available without consuming it.
    pub fn has_data(&self) -> bool {
        if let Some(buf) = &self.active_buf {
            if buf.remaining() > 0 {
                return true;
            }
        }
        !self.evt_rx.is_empty()
    }

    /// Close the receive side of the stream.
    pub fn close(&mut self) -> Result<(), StreamError> {
        if let Some(buf) = self.active_buf.take() {
            self.pool.checkin(buf);
        }
        self.cmd_tx
            .try_send(RecvCommand::Close)
            .map_err(|_| StreamError::Disconnected)?;
        Ok(())
    }
}

/// Combined stream handle with both send and receive halves.
///
/// `Send` but NOT `Clone` (mpsc::Receiver is not Clone).
pub struct StreamHandle {
    pub send: SendHalf,
    pub recv: RecvHalf,
}

/// Channel endpoints for the data-plane tasks.
pub struct DataPlaneEndpoints {
    /// Receive send commands from the actor.
    pub send_cmd_rx: mpsc::Receiver<SendCommand>,
    /// Send events back to the actor.
    pub send_evt_tx: mpsc::Sender<SendEvent>,
    /// Send received data to the actor.
    pub recv_evt_tx: mpsc::Sender<RecvEvent>,
    /// Receive consume/close commands from the actor.
    pub recv_cmd_rx: mpsc::Receiver<RecvCommand>,
    /// Shared buffer pool.
    pub pool: BufferPool,
}

/// Create a stream handle and its corresponding data-plane channel endpoints.
///
/// `channel_capacity` controls how many FrameBufs can be in-flight between
/// the actor and the data-plane tasks.
pub fn create_stream_handle(
    stream_id: StreamId,
    config: &StreamConfig,
    pool_size: usize,
    channel_capacity: usize,
) -> (StreamHandle, DataPlaneEndpoints) {
    let pool = BufferPool::new(pool_size, config.frame_size as usize);

    let (send_cmd_tx, send_cmd_rx) = mpsc::channel(channel_capacity);
    let (send_evt_tx, send_evt_rx) = mpsc::channel(channel_capacity);
    let (recv_evt_tx, recv_evt_rx) = mpsc::channel(channel_capacity);
    let (recv_cmd_tx, recv_cmd_rx) = mpsc::channel(channel_capacity);

    let handle = StreamHandle {
        send: SendHalf {
            stream_id,
            cmd_tx: send_cmd_tx,
            evt_rx: send_evt_rx,
            pool: pool.clone(),
            active_buf: None,
        },
        recv: RecvHalf {
            stream_id,
            evt_rx: recv_evt_rx,
            cmd_tx: recv_cmd_tx,
            pool: pool.clone(),
            active_buf: None,
        },
    };

    let endpoints = DataPlaneEndpoints {
        send_cmd_rx,
        send_evt_tx,
        recv_evt_tx,
        recv_cmd_rx,
        pool,
    };

    (handle, endpoints)
}

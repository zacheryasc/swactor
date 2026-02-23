use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

/// A pre-allocated frame buffer with read/write cursors for zero-alloc recycling.
///
/// Data is written starting at `write_pos` and read starting at `read_pos`.
/// When recycled via `reset()`, only the cursors are zeroed -- no memset.
pub struct FrameBuf {
    data: Box<[u8]>,
    read_pos: usize,
    write_pos: usize,
}

impl FrameBuf {
    /// Create a new buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        FrameBuf {
            data: vec![0u8; capacity].into_boxed_slice(),
            read_pos: 0,
            write_pos: 0,
        }
    }

    /// Write data into the buffer. Returns the number of bytes written.
    pub fn write(&mut self, src: &[u8]) -> usize {
        let available = self.data.len() - self.write_pos;
        let n = src.len().min(available);
        self.data[self.write_pos..self.write_pos + n].copy_from_slice(&src[..n]);
        self.write_pos += n;
        n
    }

    /// Read data from the buffer. Returns the number of bytes read.
    pub fn read(&mut self, dst: &mut [u8]) -> usize {
        let available = self.write_pos - self.read_pos;
        let n = dst.len().min(available);
        dst[..n].copy_from_slice(&self.data[self.read_pos..self.read_pos + n]);
        self.read_pos += n;
        n
    }

    /// Reset cursors for reuse. Does NOT zero the data.
    pub fn reset(&mut self) {
        self.read_pos = 0;
        self.write_pos = 0;
    }

    /// Number of unread bytes in the buffer.
    pub fn remaining(&self) -> usize {
        self.write_pos - self.read_pos
    }

    /// Available space for writing.
    pub fn available(&self) -> usize {
        self.data.len() - self.write_pos
    }

    /// Whether the buffer is full (no more write space).
    pub fn is_full(&self) -> bool {
        self.write_pos == self.data.len()
    }

    /// Whether all written data has been read.
    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    /// Total capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// The written portion of the buffer as a slice.
    pub fn written(&self) -> &[u8] {
        &self.data[..self.write_pos]
    }

    /// Load data directly into the buffer, replacing any existing content.
    pub fn load(&mut self, src: &[u8]) {
        assert!(
            src.len() <= self.data.len(),
            "source data exceeds buffer capacity"
        );
        self.data[..src.len()].copy_from_slice(src);
        self.read_pos = 0;
        self.write_pos = src.len();
    }
}

/// A fixed-size lock-free pool of `FrameBuf`s backed by `crossbeam::ArrayQueue`.
///
/// Supports concurrent checkout/checkin between actor threads and tokio tasks.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<ArrayQueue<FrameBuf>>,
    buf_capacity: usize,
}

impl BufferPool {
    /// Create a new pool with `count` buffers, each of `buf_capacity` bytes.
    pub fn new(count: usize, buf_capacity: usize) -> Self {
        let queue = ArrayQueue::new(count);
        for _ in 0..count {
            let _ = queue.push(FrameBuf::new(buf_capacity));
        }
        BufferPool {
            inner: Arc::new(queue),
            buf_capacity,
        }
    }

    /// Check out a buffer from the pool. Returns `None` if exhausted.
    pub fn checkout(&self) -> Option<FrameBuf> {
        self.inner.pop()
    }

    /// Return a buffer to the pool. The buffer is reset before being made available.
    pub fn checkin(&self, mut buf: FrameBuf) {
        buf.reset();
        // If push fails (pool full), the buffer is dropped -- this is fine.
        let _ = self.inner.push(buf);
    }

    /// Number of buffers currently available in the pool.
    pub fn available(&self) -> usize {
        self.inner.len()
    }

    /// The capacity of each buffer in the pool.
    pub fn buf_capacity(&self) -> usize {
        self.buf_capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_buf_write_then_read_returns_same_data() {
        let mut buf = FrameBuf::new(64);
        let data = b"hello, streams!";

        let written = buf.write(data);
        assert_eq!(written, data.len());
        assert_eq!(buf.remaining(), data.len());

        let mut out = vec![0u8; data.len()];
        let read = buf.read(&mut out);
        assert_eq!(read, data.len());
        assert_eq!(&out, data);
        assert!(buf.is_empty());
    }

    #[test]
    fn frame_buf_partial_write_when_full() {
        let mut buf = FrameBuf::new(8);
        let written = buf.write(b"twelve chars");
        assert_eq!(written, 8);
        assert!(buf.is_full());
        assert_eq!(buf.available(), 0);
    }

    #[test]
    fn frame_buf_reset_allows_reuse() {
        let mut buf = FrameBuf::new(16);
        buf.write(b"first");
        buf.reset();

        assert!(buf.is_empty());
        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.available(), 16);

        let written = buf.write(b"second");
        assert_eq!(written, 6);

        let mut out = vec![0u8; 6];
        buf.read(&mut out);
        assert_eq!(&out, b"second");
    }

    #[test]
    fn frame_buf_load_replaces_content() {
        let mut buf = FrameBuf::new(32);
        buf.write(b"old data");
        buf.load(b"new data here");
        assert_eq!(buf.remaining(), 13);
        let mut out = vec![0u8; 13];
        buf.read(&mut out);
        assert_eq!(&out, b"new data here");
    }

    #[test]
    fn pool_checkout_checkin_cycle() {
        let pool = BufferPool::new(4, 1024);
        assert_eq!(pool.available(), 4);

        let b1 = pool.checkout().unwrap();
        let b2 = pool.checkout().unwrap();
        assert_eq!(pool.available(), 2);

        pool.checkin(b1);
        assert_eq!(pool.available(), 3);

        pool.checkin(b2);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn pool_exhausted_returns_none() {
        let pool = BufferPool::new(2, 64);

        let _b1 = pool.checkout().unwrap();
        let _b2 = pool.checkout().unwrap();
        assert!(pool.checkout().is_none());
    }

    #[test]
    fn pool_checkin_after_exhaustion_restores_availability() {
        let pool = BufferPool::new(1, 64);

        let buf = pool.checkout().unwrap();
        assert!(pool.checkout().is_none());

        pool.checkin(buf);
        assert!(pool.checkout().is_some());
    }

    #[test]
    fn pool_checkin_resets_buffer() {
        let pool = BufferPool::new(1, 64);
        let mut buf = pool.checkout().unwrap();
        buf.write(b"dirty data");
        assert_eq!(buf.remaining(), 10);

        pool.checkin(buf);

        let recycled = pool.checkout().unwrap();
        assert!(recycled.is_empty());
        assert_eq!(recycled.available(), 64);
    }

    #[test]
    fn pool_clone_shares_same_backing() {
        let pool = BufferPool::new(3, 128);
        let pool2 = pool.clone();

        let _b = pool.checkout().unwrap();
        assert_eq!(pool2.available(), 2);
    }
}

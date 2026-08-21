use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use crossbeam_queue::{ArrayQueue, SegQueue};

pub struct HybridChannel<T> {
    ring: ArrayQueue<T>,
    overflow: SegQueue<T>,
}

impl<T> HybridChannel<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: ArrayQueue::new(capacity),
            overflow: SegQueue::new(),
        }
    }

    pub fn push(&self, value: T) {
        if !self.overflow.is_empty() {
            self.overflow.push(value);
            return;
        }
        if let Err(v) = self.ring.push(value) {
            self.overflow.push(v);
        }
    }

    pub fn pop(&self) -> Option<T> {
        self.ring.pop().or_else(|| self.overflow.pop())
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty() && self.overflow.is_empty()
    }
}

struct AsyncInboxQueue<T> {
    queue: HybridChannel<T>,
    waker: AtomicWaker,
}

impl<T> AsyncInboxQueue<T> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: HybridChannel::new(capacity),
            waker: AtomicWaker::new(),
        }
    }

    fn push(&self, value: T) {
        self.queue.push(value);
        self.waker.wake();
    }

    fn try_pop(&self) -> Option<T> {
        self.queue.pop()
    }

    fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(value) = self.try_pop() {
            return Poll::Ready(value);
        }

        self.waker.register(cx.waker());

        match self.try_pop() {
            Some(value) => Poll::Ready(value),
            None => Poll::Pending,
        }
    }
}

/// Receiver used only by process-external inboxes. Actor and worker channels
/// retain the non-waking [`Receiver`] fast path below.
pub(crate) struct AsyncReceiver<T> {
    queue: Arc<AsyncInboxQueue<T>>,
}

impl<T> AsyncReceiver<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(AsyncInboxQueue::new(capacity)),
        }
    }

    pub fn try_recv(&self) -> Option<T> {
        self.queue.try_pop()
    }

    pub fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<T> {
        self.queue.poll_pop(cx)
    }

    pub fn new_sender(&self) -> AsyncSender<T> {
        AsyncSender {
            queue: self.queue.clone(),
        }
    }
}

pub(crate) struct AsyncSender<T> {
    queue: Arc<AsyncInboxQueue<T>>,
}

impl<T> AsyncSender<T> {
    pub fn send(&self, value: T) {
        self.queue.push(value);
    }
}

pub(crate) struct Receiver<T> {
    queue: Arc<HybridChannel<T>>,
}

impl<T> Receiver<T> {
    pub fn new(capacity: usize) -> Self {
        let queue = Arc::new(HybridChannel::new(capacity));

        Self { queue }
    }
    pub fn try_recv(&self) -> Option<T> {
        self.queue.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn new_sender(&self) -> Sender<T> {
        Sender {
            queue: self.queue.clone(),
        }
    }
}

pub(crate) struct Sender<T> {
    queue: Arc<HybridChannel<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) {
        self.queue.push(value)
    }
}

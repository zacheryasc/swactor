
use std::sync::Arc;

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

    pub fn push(&self, value: T) -> Result<(), T> {
        if !self.overflow.is_empty() {
            self.overflow.push(value);
            return Ok(());
        }
        match self.ring.push(value) {
            Ok(()) => Ok(()),
            Err(v) => {
                self.overflow.push(v);
                Ok(())
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        if let Some(value) = self.ring.pop() {
            return Some(value);
        }

        match self.overflow.pop() {
            Some(value) => {
                Some(value)
            }
            None => None,
        }
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
        return self.queue.pop();
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

impl<T> Sender<T> {
    pub fn try_send(&self, value: T) -> Result<(), T> {
        return self.queue.push(value);
    }
}


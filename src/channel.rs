use std::{collections::VecDeque, sync::{Arc, Mutex}};

use crossbeam_queue::ArrayQueue;

pub struct HybridChannel<T> {
    ring: ArrayQueue<T>,
    overflow: Mutex<VecDeque<T>>,
}

impl<T> HybridChannel<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: ArrayQueue::new(capacity),
            overflow: Mutex::new(VecDeque::new()),
        }
    }

    pub fn push(&self, value: T) -> Result<(), T> {
        match self.ring.push(value) {
            Ok(()) => Ok(()),
            Err(v) => {
                self.overflow.lock().unwrap().push_back(v);
                Ok(())
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        if let Some(value) = self.ring.pop() {
            return Some(value);
        }

        self.overflow.lock().unwrap().pop_front()
    }

    pub fn len(&self) -> usize {
        self.ring.len() + self.overflow.lock().unwrap().len()
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

    pub fn len(&self) -> usize {
        self.queue.len()
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


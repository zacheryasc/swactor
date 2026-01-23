pub use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

/// The receiving end of a `crossbeam_queue::ArrayQueue`, a lock-free mpsc queue.
/// The queue is constructed by the `Receiver::new()` method.
/// Responsible for creating the `Sender` ends of itself.
///
/// Notably: The `Receiver` provides no guarentees that a sending end of the channel exists.
pub(crate) struct Receiver<T> {
    queue: Arc<ArrayQueue<T>>,
}

impl<T> Receiver<T> {
    /// Constructs a new `ArrayQueue` with given capacity.
    ///
    /// # Panics
    /// Will panic if capacity is passed as 0
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity)),
        }
    }

    /// Returns the number of elements in the inner queue
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Attempt to retrieve a value from the queue. Returns `None` if empty
    pub fn try_recv(&self) -> Option<T> {
        self.queue.pop()
    }

    /// Construct a new `Sender` assosciated with this queue.
    pub fn new_sender(&self) -> Sender<T> {
        Sender {
            queue: self.queue.clone(),
        }
    }
}

/// The sending end of a `crossbeam_queue::ArrayQueue`, a lock free mpsc queue.
/// The queue is initialized via calling the corresponding `Receiver::<T>::new()` method,
/// and the sending end of the queue is constructed via calling `receiver.new_sender()`.
///
/// Notably: The `Sender` provides no guarentees that a receiving end of the channel exists.
pub(crate) struct Sender<T> {
    queue: Arc<ArrayQueue<T>>,
}

impl<T> Sender<T> {
    /// Attempt to push a value to the queue. Returns Err(value) if the queue is full.
    pub fn try_send(&self, value: T) -> Result<(), T> {
        self.queue.push(value)
    }
}

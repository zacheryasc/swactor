use swactor::actor::ActorAddress;

/// A deduplicated collection of subscriber addresses.
#[derive(Debug, Clone)]
pub struct SubscriberSet {
    inner: Vec<ActorAddress>,
}

impl SubscriberSet {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Add an address. No-op if already present.
    pub fn add(&mut self, address: ActorAddress) {
        if !self.inner.contains(&address) {
            self.inner.push(address);
        }
    }

    /// Remove an address. No-op if not present.
    pub fn remove(&mut self, address: &ActorAddress) {
        self.inner.retain(|a| a != address);
    }

    /// Snapshot of current subscribers.
    pub fn snapshot(&self) -> Vec<ActorAddress> {
        self.inner.clone()
    }

    /// Number of subscribers.
    pub fn count(&self) -> usize {
        self.inner.len()
    }
}

impl Default for SubscriberSet {
    fn default() -> Self {
        Self::new()
    }
}

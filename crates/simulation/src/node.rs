//! Generic simulation node trait.
//!
//! Defines the interface that any protocol node must implement to be
//! driven by the simulation runner. This allows the simulation
//! framework to work with different protocol implementations.

/// A message produced by a simulated node.
pub trait SimMessage {
    type NodeId;
    /// The target node for this message, if any.
    /// `None` means the message is a local notification (no delivery needed).
    fn target(&self) -> Option<&Self::NodeId>;
}

/// A simulated protocol node.
pub trait SimNode: Sized {
    type Config: Clone;
    type NodeId: Clone + Eq + std::hash::Hash + std::fmt::Debug;
    type Message: SimMessage<NodeId = Self::NodeId>;
    type Snapshot: serde::Serialize;
    type EventKind: serde::Serialize;

    fn new(config: Self::Config) -> Self;
    fn node_id(&self) -> Self::NodeId;
    fn tick(&mut self) -> Vec<Self::Message>;
    fn receive(&mut self, from: Self::NodeId, msg: Self::Message) -> Vec<Self::Message>;
    fn snapshot(&self) -> Self::Snapshot;
}

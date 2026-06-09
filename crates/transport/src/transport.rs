//! Delivery: the transport protocol, the address→transport router, an in-memory
//! transport for tests, and the [`CodecRemoteSink`] adapter that bridges this
//! crate's codec/router back to core's [`swactor::runtime::RemoteSink`] hook.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use swactor::actor::ActorAddress;
use swactor::{AddrBuildHasher, AddrMap};
use swactor::Error;

use crate::codec::{CodecRegistry, WireEnvelope};

// ─── Transport ──────────────────────────────────────────────────────────────

/// Pluggable transport protocol.
///
/// Implementations queue or send the envelope to a remote runtime.
/// `send` should not block the calling thread.
pub trait Transport: Send + Sync {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error>;
}

// ─── TransportRouter ────────────────────────────────────────────────────────

/// Maps remote actor addresses to their [`Transport`].
pub struct TransportRouter {
    routes: RwLock<AddrMap<Arc<dyn Transport>>>,
}

impl Default for TransportRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportRouter {
    pub fn new() -> Self {
        Self {
            routes: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
        }
    }

    /// Register a remote address as reachable via the given transport.
    pub fn add_route(&self, addr: ActorAddress, transport: Arc<dyn Transport>) {
        self.routes.write().unwrap().insert(addr, transport);
    }

    /// Look up which transport handles a given address.
    pub(crate) fn lookup(&self, addr: &ActorAddress) -> Option<Arc<dyn Transport>> {
        self.routes.read().unwrap().get(addr).cloned()
    }
}

// ─── InMemoryTransport ──────────────────────────────────────────────────────

/// In-process transport connecting two runtimes via an `mpsc` channel.
///
/// Use [`pair`](Self::pair) to create a linked transport + receiver.
pub struct InMemoryTransport {
    tx: std::sync::Mutex<std::sync::mpsc::Sender<WireEnvelope>>,
}

impl InMemoryTransport {
    /// Create a linked pair: the transport sends to the returned receiver.
    pub fn pair() -> (Arc<InMemoryTransport>, std::sync::mpsc::Receiver<WireEnvelope>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let transport = Arc::new(InMemoryTransport {
            tx: std::sync::Mutex::new(tx),
        });
        (transport, rx)
    }
}

impl Transport for InMemoryTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        self.tx
            .lock()
            .unwrap()
            .send(envelope)
            .map_err(|_| Error::from("InMemoryTransport: receiver dropped"))
    }
}

// ─── CodecRemoteSink ────────────────────────────────────────────────────────

/// Adapter implementing core's [`RemoteSink`](swactor::runtime::RemoteSink): it
/// encodes a type-erased message via the [`CodecRegistry`] and ships the
/// resulting [`WireEnvelope`] over the [`Transport`] the [`TransportRouter`]
/// holds for the destination.
///
/// This is the seam that keeps `swactor` core codec-free — the runtime knows
/// only the trait; all encode/route logic lives here.
pub struct CodecRemoteSink {
    registry: Arc<CodecRegistry>,
    router: Arc<TransportRouter>,
}

impl CodecRemoteSink {
    pub fn new(registry: Arc<CodecRegistry>, router: Arc<TransportRouter>) -> Self {
        Self { registry, router }
    }
}

impl swactor::runtime::RemoteSink for CodecRemoteSink {
    fn send(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        let transport = self
            .router
            .lookup(&addr)
            .ok_or_else(|| Error::from("Address not found"))?;

        // `(*msg).type_id()` resolves the inner message's TypeId, not `Box`'s.
        let type_id = (*msg).type_id();
        let (type_tag, payload) = self.registry.encode(type_id, msg)?;

        transport.send(WireEnvelope {
            dest: addr,
            type_tag,
            payload,
        })
    }
}

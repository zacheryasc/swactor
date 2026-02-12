use std::collections::HashMap;

use log::{debug, trace};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use super::trace::{GossipEvent, GossipEventKind, NodeSnapshot, TraceContext};

// ── VersionedValue ───────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub version: u64,
}

// ── GossipState ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct GossipState {
    entries: HashMap<String, VersionedValue>,
}

impl GossipState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a key. Auto-increments the version for that key.
    /// Returns the new version number.
    pub fn set(&mut self, key: String, value: Vec<u8>) -> u64 {
        let new_version = self
            .entries
            .get(&key)
            .map_or(1, |existing| existing.version + 1);
        self.entries.insert(
            key,
            VersionedValue {
                value,
                version: new_version,
            },
        );
        new_version
    }

    pub fn get(&self, key: &str) -> Option<&VersionedValue> {
        self.entries.get(key)
    }

    pub fn entries(&self) -> &HashMap<String, VersionedValue> {
        &self.entries
    }

    /// Merge a remote state into this one. For each key, keep the entry
    /// with the higher version (last-writer-wins). Returns the number of
    /// entries that were updated.
    pub fn merge(&mut self, remote: &GossipState) -> usize {
        let mut updated = 0;
        for (key, remote_val) in &remote.entries {
            let dominated = match self.entries.get(key) {
                Some(local_val) => remote_val.version > local_val.version,
                None => true,
            };
            if dominated {
                self.entries.insert(key.clone(), remote_val.clone());
                updated += 1;
            }
        }
        updated
    }
}

// ── GossipQueryResponse ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct GossipQueryResponse {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub version: Option<u64>,
}

// ── GossipMessage ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum GossipMessage {
    /// Register a peer to gossip with.
    AddPeer(ActorAddress),
    /// Remove a peer from the gossip set.
    RemovePeer(ActorAddress),
    /// Set a key-value pair in this node's local state.
    Set { key: String, value: Vec<u8> },
    /// Trigger a gossip round: pick a random peer and push our full state.
    DoGossipRound,
    /// Incoming state push from a peer.
    Push {
        from: ActorAddress,
        state: GossipState,
    },
    /// Query the current value for a key; response sent to `reply_to`.
    Query {
        key: String,
        reply_to: ActorAddress,
    },
    /// Ask the actor to dump its current state into the event log (tracing only).
    TakeSnapshot,
}

// ── GossipActor ──────────────────────────────────────────────────────────

pub struct GossipActor {
    state: GossipState,
    peers: Vec<ActorAddress>,
    trace: Option<TraceContext>,
}

impl GossipActor {
    pub fn new() -> Self {
        Self {
            state: GossipState::new(),
            peers: Vec::new(),
            trace: None,
        }
    }

    /// Create a traced actor that records events into the shared log.
    pub fn traced(
        log: super::trace::EventLog,
        tick: super::trace::TickCounter,
        names: super::trace::NameRegistry,
    ) -> Self {
        Self {
            state: GossipState::new(),
            peers: Vec::new(),
            trace: Some(TraceContext {
                event_log: log,
                tick_counter: tick,
                name_registry: names,
            }),
        }
    }

    fn pick_random_peer(&self) -> Option<ActorAddress> {
        if self.peers.is_empty() {
            return None;
        }
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        let idx = usize::from_ne_bytes(buf) % self.peers.len();
        Some(self.peers[idx])
    }

    fn record(&self, addr: ActorAddress, kind: GossipEventKind) {
        if let Some(trace) = &self.trace {
            let event = GossipEvent {
                tick: trace.current_tick(),
                node_name: trace.resolve_name(addr),
                node_addr: addr,
                thread_name: std::thread::current().name().map(|s| s.to_owned()),
                kind,
            };
            trace.record_event(event);
        }
    }

    fn self_name(&self, addr: ActorAddress) -> String {
        self.trace
            .as_ref()
            .map(|t| t.resolve_name(addr))
            .unwrap_or_else(|| format!("{:?}", &addr.0[..4]))
    }

    fn peer_name(&self, addr: ActorAddress) -> String {
        self.self_name(addr)
    }

    fn round(&self) -> u64 {
        self.trace
            .as_ref()
            .map(|t| t.current_tick())
            .unwrap_or(0)
    }
}

impl Default for GossipActor {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorInterface for GossipActor {
    type Incoming = GossipMessage;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: GossipMessage) {
        let self_addr = ctx.self_addr();
        match msg {
            GossipMessage::AddPeer(addr) => {
                if !self.peers.contains(&addr) {
                    let pname = self.peer_name(addr);
                    debug!(
                        "[{}] inbound AddPeer peer={}  (total_peers={})",
                        self.self_name(self_addr),
                        pname,
                        self.peers.len() + 1,
                    );
                    self.peers.push(addr);
                    self.record(
                        self_addr,
                        GossipEventKind::PeerAdded {
                            peer_name: pname,
                        },
                    );
                }
            }
            GossipMessage::RemovePeer(addr) => {
                let before = self.peers.len();
                self.peers.retain(|a| *a != addr);
                if self.peers.len() < before {
                    let pname = self.peer_name(addr);
                    debug!(
                        "[{}] inbound RemovePeer peer={}  (total_peers={})",
                        self.self_name(self_addr),
                        pname,
                        self.peers.len(),
                    );
                    self.record(
                        self_addr,
                        GossipEventKind::PeerRemoved {
                            peer_name: pname,
                        },
                    );
                }
            }
            GossipMessage::Set { key, value } => {
                debug!(
                    "[{}] inbound Set key={:?} value_len={}",
                    self.self_name(self_addr),
                    key,
                    value.len(),
                );
                self.state.set(key.clone(), value);
                self.record(self_addr, GossipEventKind::LocalSet { key });
            }
            GossipMessage::DoGossipRound => {
                let round = self.round();
                if let Some(peer) = self.pick_random_peer() {
                    let target = self.peer_name(peer);
                    debug!(
                        "[{}] round={} outbound Push -> {} (state_keys={})",
                        self.self_name(self_addr),
                        round,
                        target,
                        self.state.entries().len(),
                    );
                    self.record(
                        self_addr,
                        GossipEventKind::GossipRoundStarted {
                            target_name: target,
                        },
                    );
                    let _ = ctx.send(
                        peer,
                        GossipMessage::Push {
                            from: self_addr,
                            state: self.state.clone(),
                        },
                    );
                } else {
                    debug!(
                        "[{}] round={} no peers — skipping gossip",
                        self.self_name(self_addr),
                        round,
                    );
                    self.record(self_addr, GossipEventKind::GossipRoundNoPeers);
                }
            }
            GossipMessage::Push {
                from,
                state: remote,
            } => {
                let from_name = self.peer_name(from);
                let keys_updated = self.state.merge(&remote);
                debug!(
                    "[{}] round={} inbound Push <- {} keys_updated={} (state_keys={})",
                    self.self_name(self_addr),
                    self.round(),
                    from_name,
                    keys_updated,
                    self.state.entries().len(),
                );
                self.record(
                    self_addr,
                    GossipEventKind::PushReceived {
                        from_name,
                        keys_updated,
                    },
                );
            }
            GossipMessage::TakeSnapshot => {
                trace!(
                    "[{}] round={} snapshot (keys={})",
                    self.self_name(self_addr),
                    self.round(),
                    self.state.entries().len(),
                );
                self.record(
                    self_addr,
                    GossipEventKind::StateSnapshot {
                        snapshot: NodeSnapshot {
                            entries: self.state.entries().clone(),
                            peer_count: self.peers.len(),
                        },
                    },
                );
            }
            GossipMessage::Query { key, reply_to } => {
                let entry = self.state.get(&key);
                debug!(
                    "[{}] inbound Query key={:?} found={}",
                    self.self_name(self_addr),
                    key,
                    entry.is_some(),
                );
                self.record(
                    self_addr,
                    GossipEventKind::QueryReceived { key: key.clone() },
                );
                let resp = GossipQueryResponse {
                    key,
                    value: entry.map(|e| e.value.clone()),
                    version: entry.map(|e| e.version),
                };
                debug!(
                    "[{}] outbound QueryResponse -> {:?}",
                    self.self_name(self_addr),
                    &reply_to.0[..4],
                );
                let _ = ctx.send(reply_to, resp);
            }
        }
    }
}

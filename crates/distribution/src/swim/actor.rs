//! `SwimActor` — the actor-runtime shell around the SWIM protocol engine.
//!
//! This is the **interface-normative** layer of `SWIM_ACTOR_SPEC.md` (§2, §4):
//! the single `SwimIn` Incoming enum (§4.1), the timer-as-message clock seam
//! (`Tick{now}`, §4.2), send-failure-as-message (`SendFailed`, §4.3),
//! subscriptions, and the `NodeId`→`ActorAddress` Binding (`PeerDirectory`, §3.2).
//!
//! The **wire-normative** protocol semantics (§5–§12: the merge CRDT, probe
//! cycle, dissemination budget, refutation, Lifeguard) live unchanged in
//! [`SwimNode`] — the actor wraps it and translates the `Vec<NodeAction>` it
//! produces into `ctx.send(...)` (each peer addressed by resolving its `NodeId`
//! through the Binding) plus `MembershipChanged` notifications to subscribers
//! (§6.3). The actor reads no ambient clock: time enters only as the `now` field
//! of `Tick` (§4.2), so the actor is a pure function of its message stream.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::crypto::verify_directory_entry;
use crate::messages::{Ack, IndirectAck, JoinRequest, JoinResponse, Ping, PingReq};
use crate::types::{DirectoryEntry, MemberState, NodeId};

use super::node::{NodeAction, SwimNode, SwimObserver};
use super::probe::SwimConfig;

// ─── The Binding (§3.2) ───────────────────────────────────────────────────────

/// Resolves a protocol `NodeId` to its mailbox `ActorAddress` (§3.2).
///
/// `NodeId` and `ActorAddress` are deliberately distinct (§3.1): a node may
/// rebind to a fresh mailbox across restarts while its gossip identity stays
/// stable. The actor reasons only in `NodeId` and resolves to an `ActorAddress`
/// through this seam at the moment it is time to send. A resolution miss is not
/// a handler error — it surfaces as `SendFailed{to}` and feeds failure detection
/// (§4.3).
pub trait PeerDirectory: Send + Sync + 'static {
    fn resolve(&self, node: &NodeId) -> Option<ActorAddress>;
}

/// A cheaply-clonable, shared in-memory [`PeerDirectory`].
///
/// Bindings are the signed directory record (`DirectoryEntry`, §3.2): a later
/// `generation` supersedes an earlier one for the same `NodeId`. Clones share
/// the same backing map, so a harness can register bindings after the actors
/// are spawned.
#[derive(Clone, Default)]
pub struct SharedPeerDirectory {
    inner: Arc<RwLock<BTreeMap<NodeId, (u64, ActorAddress)>>>,
}

impl SharedPeerDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind `node_id → actor_addr` at `generation`. A binding at a strictly
    /// higher generation supersedes an earlier one (§3.2); lower/equal
    /// generations are ignored.
    pub fn bind(&self, node_id: NodeId, actor_addr: ActorAddress, generation: u64) {
        let mut g = self.inner.write().expect("peer directory lock poisoned");
        match g.get(&node_id) {
            Some((cur_gen, _)) if *cur_gen >= generation => {}
            _ => {
                g.insert(node_id, (generation, actor_addr));
            }
        }
    }

    /// Drop the binding for `node_id`, if any. After this, sends to that node
    /// hit a binding miss and surface as `SendFailed` (§4.3) — modelling a node
    /// that has become unreachable (a genuine silence for failure detection).
    pub fn unbind(&self, node_id: &NodeId) {
        self.inner
            .write()
            .expect("peer directory lock poisoned")
            .remove(node_id);
    }

    /// Register a signed [`DirectoryEntry`]. Returns `false` (and stores nothing)
    /// if the signature does not verify — the Binding only trusts records the
    /// owning node signed over `(actor_addr, node_id, generation)`.
    pub fn register(&self, entry: &DirectoryEntry) -> bool {
        if !verify_directory_entry(entry) {
            return false;
        }
        self.bind(entry.node_id, entry.actor_addr, entry.generation);
        true
    }
}

impl PeerDirectory for SharedPeerDirectory {
    fn resolve(&self, node: &NodeId) -> Option<ActorAddress> {
        self.inner
            .read()
            .expect("peer directory lock poisoned")
            .get(node)
            .map(|(_, addr)| *addr)
    }
}

// ─── The single `Incoming` enum (§4.1) ────────────────────────────────────────

/// Every message the `SwimActor` receives, as variants of one enum (§4.1).
///
/// The six network kinds are decoded from the wire by `type_tag` (§6.1) and
/// re-wrapped into the matching variant by the transport ingress; in-process,
/// actors send these variants to each other directly. The local control edges
/// (`Tick`, `SendFailed`, `Join`/`Leave`/`Subscribe`) are constructed by the
/// TimerDriver, the egress, and the application (§6.2).
#[derive(Clone)]
pub enum SwimIn {
    // network (§6.1)
    Ping(Ping),
    Ack(Ack),
    PingReq(PingReq),
    IndirectAck(IndirectAck),
    JoinRequest(JoinRequest),
    JoinResponse(JoinResponse),
    // local control (§6.2)
    /// The clock. The only source of `now`; every temporal guard keys off it (§4.2).
    Tick {
        now: Instant,
    },
    /// A fire-and-forget egress to `to` failed (binding miss or transport drop, §4.3).
    SendFailed {
        to: NodeId,
    },
    /// Application asks to join via `seeds`: send each a `JoinRequest{from=self}`.
    Join {
        seeds: Vec<NodeId>,
    },
    /// Application asks to leave: gossip self as `Dead` at the current incarnation.
    Leave,
    /// Register `observer` as a `MembershipChanged` sink (§6.3).
    Subscribe {
        observer: ActorAddress,
    },
}

// ─── The sole observable (§6.3) ────────────────────────────────────────────────

/// The actor's only externally-observable membership output (§6.3).
///
/// Delivered to every subscriber, unbounded and in-order, with no coalescing —
/// one notification per membership change. Replaces the old
/// `NodeAction::MembershipChanged` and the removed diagnostics as the membership
/// observable at the actor boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipChanged {
    pub node_id: NodeId,
    pub state: MemberState,
    pub incarnation: u64,
}

// ─── The actor ─────────────────────────────────────────────────────────────────

/// The SWIM actor: a `SwimNode` protocol engine plus the actor-new seams
/// (§5.5): `last_now`, `subscribers`, and the `peer_directory` Binding.
pub struct SwimActor {
    self_id: NodeId,
    node: SwimNode,
    /// Latest `Tick`'s `now` (§4.2). Non-`Tick` handlers reuse it via the engine.
    last_now: Instant,
    subscribers: Vec<ActorAddress>,
    peer_directory: Arc<dyn PeerDirectory>,
}

impl SwimActor {
    /// Build a SWIM actor. `now` anchors the engine's deadlines; the first
    /// `Tick` supplies the real clock thereafter (§4.2).
    pub fn new(
        self_id: NodeId,
        config: SwimConfig,
        now: Instant,
        peer_directory: Arc<dyn PeerDirectory>,
    ) -> Self {
        Self {
            self_id,
            node: SwimNode::new(self_id, config, now),
            last_now: now,
            subscribers: Vec::new(),
            peer_directory,
        }
    }

    /// Install a [`SwimObserver`] on the wrapped engine, so probe RTT and
    /// membership transitions (with their cause) surface to a telemetry sink.
    /// Non-breaking builder over [`new`](Self::new); callers that don't observe
    /// SWIM leave it off (production previously always did).
    pub fn with_observer(mut self, observer: Box<dyn SwimObserver>) -> Self {
        self.node.set_observer(observer);
        self
    }

    /// Resolve `to` through the Binding and send `msg`; on a binding miss or a
    /// transport-rejected send, deliver `SendFailed{to}` back to ourselves
    /// (§4.3). This never recurses synchronously — the failure is a mailbox
    /// message, so a reactive probe's own send-failure is absorbed by the probe
    /// phase guard (`is_currently_probing`) on the next turn rather than looping.
    fn send_to_node(&self, ctx: &Ctx, to: NodeId, msg: SwimIn) {
        match self.peer_directory.resolve(&to) {
            Some(addr) => {
                if ctx.send(addr, msg).is_err() {
                    let _ = ctx.send(ctx.self_addr(), SwimIn::SendFailed { to });
                }
            }
            None => {
                let _ = ctx.send(ctx.self_addr(), SwimIn::SendFailed { to });
            }
        }
    }

    /// Translate the engine's `NodeAction`s into actor effects: network sends
    /// resolved through the Binding, and `MembershipChanged` to subscribers.
    /// Effects are applied in the order produced (§10).
    fn dispatch(&self, ctx: &Ctx, actions: Vec<NodeAction>) {
        for action in actions {
            match action {
                NodeAction::SendPing {
                    to,
                    sequence,
                    piggyback,
                } => self.send_to_node(
                    ctx,
                    to,
                    SwimIn::Ping(Ping {
                        from: self.self_id,
                        sequence,
                        piggyback,
                    }),
                ),
                NodeAction::SendAck {
                    to,
                    sequence,
                    piggyback,
                } => self.send_to_node(
                    ctx,
                    to,
                    SwimIn::Ack(Ack {
                        from: self.self_id,
                        sequence,
                        piggyback,
                    }),
                ),
                NodeAction::SendPingReq {
                    relay,
                    target,
                    sequence,
                    piggyback,
                } => self.send_to_node(
                    ctx,
                    relay,
                    SwimIn::PingReq(PingReq {
                        from: self.self_id,
                        target,
                        sequence,
                        piggyback,
                    }),
                ),
                NodeAction::ForwardAck {
                    to,
                    target,
                    sequence,
                    piggyback,
                } => self.send_to_node(
                    ctx,
                    to,
                    // IndirectAck carries no `from`: the routing destination IS the
                    // original prober, and the message names only the probed target.
                    SwimIn::IndirectAck(IndirectAck {
                        target,
                        sequence,
                        piggyback,
                    }),
                ),
                NodeAction::SendJoinResponse { to, members } => {
                    self.send_to_node(ctx, to, SwimIn::JoinResponse(JoinResponse { members }))
                }
                NodeAction::MembershipChanged {
                    node_id,
                    state,
                    incarnation,
                } => {
                    let note = MembershipChanged {
                        node_id,
                        state,
                        incarnation,
                    };
                    for sub in &self.subscribers {
                        let _ = ctx.send(*sub, note.clone());
                    }
                }
            }
        }
    }
}

impl ActorInterface for SwimActor {
    type Incoming = SwimIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: SwimIn) {
        match msg {
            // §4.2: only Tick carries `now`; record it and step the probe with it.
            SwimIn::Tick { now } => {
                self.last_now = now;
                let acts = self.node.tick(now);
                self.dispatch(ctx, acts);
            }
            SwimIn::Ping(p) => {
                let acts = self.node.handle_ping(p.from, p.sequence, &p.piggyback);
                self.dispatch(ctx, acts);
            }
            SwimIn::Ack(a) => {
                let acts = self.node.handle_ack(a.from, a.sequence, &a.piggyback);
                self.dispatch(ctx, acts);
            }
            SwimIn::PingReq(pr) => {
                let acts =
                    self.node
                        .handle_ping_req(pr.from, pr.target, pr.sequence, &pr.piggyback);
                self.dispatch(ctx, acts);
            }
            SwimIn::IndirectAck(ia) => {
                let acts = self
                    .node
                    .handle_indirect_ack(ia.target, ia.sequence, &ia.piggyback);
                self.dispatch(ctx, acts);
            }
            SwimIn::JoinRequest(jr) => {
                let acts = self.node.handle_join_request(jr.from);
                self.dispatch(ctx, acts);
            }
            SwimIn::JoinResponse(jr) => {
                let acts = self.node.handle_join_response(jr.members);
                self.dispatch(ctx, acts);
            }
            // §4.3 / §9.6: a failed egress is a reactive probe trigger.
            SwimIn::SendFailed { to } => {
                let acts = self.node.report_send_failure(to);
                self.dispatch(ctx, acts);
            }
            // §10.10: bootstrap — ask each seed to admit us and return the roster.
            SwimIn::Join { seeds } => {
                let from = self.self_id;
                for seed in seeds {
                    self.send_to_node(ctx, seed, SwimIn::JoinRequest(JoinRequest { from }));
                }
            }
            // §10.10: gossip self as Dead; the record propagates via later piggybacks.
            SwimIn::Leave => {
                let acts = self.node.leave();
                self.dispatch(ctx, acts);
            }
            // §6.3: append a notification sink.
            SwimIn::Subscribe { observer } => {
                self.subscribers.push(observer);
            }
        }
    }
}

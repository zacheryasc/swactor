//! The network (SIM_SPEC §5 + RELAY_SPEC §4).
//!
//! A directed-graph link model with deterministic per-edge state, the
//! §5.4 send algorithm, the §5.5 mutation suite, and a §7-conformant
//! integer-only computation path (no floats touch any decision).
//!
//! RELAY_SPEC §4 adds the relay vertex: a first-class non-host node in
//! the topology with its own ingress and per-egress queues, head-of-
//! line serialization, queue-overflow drops, and cold-start penalty.
//! Relayed routes (RELAY_SPEC §4.1) are composed inside the network's
//! `send`; the engine sees one `SendOutcome` per query regardless.
//!
//! The network owns no schedule of its own; the engine pops events and
//! queries the network. Each query mutates per-link state but never
//! reads from any clock outside the `now_ns` the engine supplies.

use std::collections::{BTreeMap, BTreeSet};

use crate::rng::{SubstreamKey, SubstreamRng, jitter_sample};
use crate::scenario::{
    HostRoute, LinkPolicy, LinkRef, Mutation, MutationKind, Relay, Scenario,
};

// ──────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────

/// Monotonic identifier for an in-flight delivery. Returned by
/// successful sends, used by `apply_mutation` to point at deliveries
/// that need invalidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    Arrive {
        delivery_id: DeliveryId,
        at_ns: u64,
    },
    Drop {
        reason: DropReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// The (from, to) pair has no declared edge or route.
    NoRoute,
    /// The active partition set cuts this edge.
    Partitioned,
    /// The Bernoulli loss draw for the edge fired.
    Lossy,
    /// The relay's queue would overflow if this message were enqueued.
    /// RELAY_SPEC §4.4 step 2.
    RelayQueueFull,
    /// The route is through a relay that has been `RelayKill`-ed.
    /// RELAY_SPEC §4.5.
    RelayDown,
    /// Spec §"Sim cross-pollination" (N3 upgrade spec F3): the relay
    /// is still up and other peer pairs through it work fine, but a
    /// `RelayPeerConnDown` mutation has selectively cut this
    /// (from, to) pair's relay-mediated path. Models the
    /// 2026-05-25 "tunnel up, peer-via-tunnel down" asymmetry.
    RelayPeerConnDown,
}

/// Side-channel notification the engine consumes after each query.
/// `take_pending_notifications()` is the canonical way to drain them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkNotification {
    CacheStateChange {
        from: String,
        to: String,
        at_ns: u64,
        transition: CacheTransition,
    },
    DialStart {
        from: String,
        to: String,
        at_ns: u64,
    },
    DialOutcome {
        from: String,
        to: String,
        at_ns: u64,
        warm: bool,
    },
    /// RELAY_SPEC §4.4 step 7 / §9.1. The message reached the relay
    /// and was enqueued (or accounted for in the ingress) at `at_ns`.
    RelayEnqueue {
        relay: String,
        from: String,
        to: String,
        byte_len: u64,
        at_ns: u64,
    },
    /// RELAY_SPEC §4.4 step 7 / §9.1. The relay finished serving the
    /// message on its outbound egress at `at_ns`.
    RelayDequeue {
        relay: String,
        from: String,
        to: String,
        byte_len: u64,
        at_ns: u64,
    },
    /// RELAY_SPEC §4.4 step 2 / §4.5 / §9.1. The relay refused the
    /// message at `at_ns`.
    RelayDrop {
        relay: String,
        from: String,
        to: String,
        byte_len: u64,
        reason: RelayDropReason,
        at_ns: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayDropReason {
    QueueFull,
    Down,
    /// Spec F3 — peer-via-tunnel down. Distinguished from `Down` so
    /// the bundle reader can answer "did the relay die or did this
    /// specific peer's path through it die?" without inference.
    PeerConnDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTransition {
    /// The link warmed after `cache_warm_after_ns` of sustained traffic.
    Warmed,
    /// The link went cold by idle (`cache_invalidate_after_idle_ns`).
    IdleCooled,
    /// A mutation reset the link to Cold.
    Invalidated,
}

/// A delivery the engine should remove from its queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidatedDelivery {
    pub delivery_id: DeliveryId,
    pub from: String,
    pub to: String,
    pub scheduled_at_ns: u64,
}

// ──────────────────────────────────────────────────────────────────────
// Network
// ──────────────────────────────────────────────────────────────────────

pub struct Network {
    edges: BTreeMap<(String, String), EdgeState>,
    /// Symmetric partition set keyed by the lex-sorted pair. A pair in
    /// here means the directed edges in both directions are cut.
    partitioned: BTreeSet<(String, String)>,
    /// Active timed mutations the next `send` must consult. Time-windowed.
    active_latency_spike: Vec<TimedEffect<LatencySpike>>,
    active_loss_burst: Vec<TimedEffect<LossBurst>>,
    active_relay_buffer: Vec<TimedEffect<RelayBuffer>>,
    /// F3 — selectively-cut (relay, from, to) triples. While active,
    /// `send_relayed` drops with `RelayPeerConnDown` but the relay
    /// stays available for other pairs.
    active_relay_peer_down: Vec<TimedEffect<RelayPeerDown>>,
    /// Killed peers. Their inbound deliveries are invalidated when the
    /// kill mutation runs; later sends to them still return NoRoute is
    /// the engine's job (the kill is a peer-state thing the engine
    /// owns). The network exposes a list of in-flight deliveries to
    /// the killed peer for invalidation.
    killed_peers: BTreeSet<String>,
    /// RELAY_SPEC §4 relay vertices, indexed by id.
    relays: BTreeMap<String, RelayState>,
    /// RELAY_SPEC §4.1 — per ordered host pair, the resolved route
    /// (direct or relayed-through-a-named-relay). Pairs absent from
    /// here drop with `NoRoute`.
    routes: BTreeMap<(String, String), HostRoute>,
    seed: u64,
    next_delivery_id: u64,
    pending_notifications: Vec<NetworkNotification>,
}

#[derive(Debug)]
struct EdgeState {
    policy: LinkPolicy,
    last_send_ns: Option<u64>,
    last_arrive_ns: Option<u64>,
    cache: CacheState,
    rng: SubstreamRng,
    /// Sorted by `scheduled_at_ns`; mutations consult this list.
    in_flight: Vec<InFlight>,
}

#[derive(Debug)]
struct RelayState {
    policy: Relay,
    /// Per outbound link (keyed by destination node id), the virtual
    /// time at which the last scheduled message finishes serialization.
    egress_queue_tail_ns: BTreeMap<String, u64>,
    /// Across the single shared ingress, the virtual time at which the
    /// last scheduled message finishes serialization.
    ingress_queue_tail_ns: u64,
    /// In-flight messages currently between ingress-enqueue and
    /// egress-dequeue. Tracked so `RelayKill` returns the right set.
    in_flight: Vec<RelayInFlight>,
    boot_state: BootState,
    /// Has the relay been killed by a `RelayKill` mutation? After
    /// kill, no more messages forward until `RelayBoot`.
    killed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BootState {
    Booted,
    /// The relay has just been booted; the next forwarded message
    /// pays the cold-start penalty. `since_ns` is informational.
    Booting { since_ns: u64 },
}

#[derive(Debug, Clone)]
struct RelayInFlight {
    delivery_id: DeliveryId,
    from_host: String,
    to_host: String,
    byte_len: u64,
    /// Final arrival time at the destination host (post-egress + outbound leg).
    arrival_at_ns: u64,
    /// Virtual time the message left the relay's egress (used for
    /// `enqueued_bytes` bookkeeping in `decrement_after_egress`).
    egress_end_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CacheState {
    Cold,
    Warming { since_ns: u64 },
    Warm,
}

#[derive(Debug, Clone)]
struct InFlight {
    delivery_id: DeliveryId,
    scheduled_at_ns: u64,
    /// For relayed routes, the relay through which this delivery is
    /// being forwarded. `None` for direct edges.
    via_relay: Option<String>,
}

#[derive(Debug, Clone)]
struct TimedEffect<T> {
    start_ns: u64,
    end_ns: u64,
    payload: T,
}

#[derive(Debug, Clone)]
struct LatencySpike {
    links: BTreeSet<(String, String)>,
    factor_x100: u32,
}

#[derive(Debug, Clone)]
struct LossBurst {
    links: BTreeSet<(String, String)>,
    prob_ppm: u32,
}

#[derive(Debug, Clone)]
struct RelayBuffer {
    links: BTreeSet<(String, String)>,
    floor_ns: u64,
}

#[derive(Debug, Clone)]
struct RelayPeerDown {
    relay: String,
    from: String,
    to: String,
}

impl Network {
    pub fn new(scenario: &Scenario) -> Self {
        let mut edges = BTreeMap::new();
        for link in &scenario.links {
            let rng = SubstreamRng::derive(
                scenario.seed,
                &SubstreamKey::Link {
                    from: link.from.clone(),
                    to: link.to.clone(),
                },
            );
            edges.insert(
                (link.from.clone(), link.to.clone()),
                EdgeState {
                    policy: link.policy,
                    last_send_ns: None,
                    last_arrive_ns: None,
                    cache: CacheState::Cold,
                    rng,
                    in_flight: Vec::new(),
                },
            );
        }
        let mut relays = BTreeMap::new();
        for r in &scenario.relays {
            relays.insert(
                r.id.clone(),
                RelayState {
                    policy: r.clone(),
                    egress_queue_tail_ns: BTreeMap::new(),
                    ingress_queue_tail_ns: 0,
                    in_flight: Vec::new(),
                    boot_state: BootState::Booted,
                    killed: false,
                },
            );
        }
        let mut routes = BTreeMap::new();
        for r in &scenario.routes {
            routes.insert((r.from().to_string(), r.to().to_string()), r.clone());
        }
        // If the scenario has no relays and no `via` shorthand, the
        // `routes` field may be empty; in that case build a direct
        // route per declared host-to-host edge. This preserves
        // backward compatibility with scenarios written before the
        // relay extension.
        if routes.is_empty() {
            for (key, _edge) in edges.iter() {
                routes.insert(
                    key.clone(),
                    HostRoute::Direct {
                        from: key.0.clone(),
                        to: key.1.clone(),
                    },
                );
            }
        }
        Self {
            edges,
            partitioned: BTreeSet::new(),
            active_latency_spike: Vec::new(),
            active_loss_burst: Vec::new(),
            active_relay_buffer: Vec::new(),
            active_relay_peer_down: Vec::new(),
            killed_peers: BTreeSet::new(),
            relays,
            routes,
            seed: scenario.seed,
            next_delivery_id: 0,
            pending_notifications: Vec::new(),
        }
    }

    /// Drain side-channel notifications produced since the last call.
    pub fn take_pending_notifications(&mut self) -> Vec<NetworkNotification> {
        std::mem::take(&mut self.pending_notifications)
    }

    /// Read-only test hook: total number of in-flight deliveries
    /// across every edge.
    pub fn in_flight_count(&self) -> usize {
        self.edges.values().map(|e| e.in_flight.len()).sum()
    }

    /// Read-only test hook: number of messages currently in the named
    /// relay's queues (between ingress enqueue and egress dequeue).
    pub fn relay_in_flight_count(&self, relay: &str) -> usize {
        self.relays.get(relay).map(|r| r.in_flight.len()).unwrap_or(0)
    }

    /// Per §3.2 / §5.4 plus RELAY_SPEC §4.4 composition.
    pub fn send(&mut self, from: &str, to: &str, byte_len: u64, sent_at_ns: u64) -> SendOutcome {
        let route_key = (from.to_string(), to.to_string());
        let Some(route) = self.routes.get(&route_key).cloned() else {
            return SendOutcome::Drop {
                reason: DropReason::NoRoute,
            };
        };
        match route {
            HostRoute::Direct { .. } => self.send_direct(from, to, byte_len, sent_at_ns, None),
            HostRoute::Relayed { relay, .. } => {
                self.send_relayed(from, &relay, to, byte_len, sent_at_ns)
            }
        }
    }

    /// Direct (or single-leg) send along one declared edge. When
    /// `relay_context` is `Some`, the in-flight entry is tagged with
    /// the originating relay so `RelayKill` can invalidate the right
    /// deliveries. The composed `send_relayed` path uses this for the
    /// outbound leg.
    fn send_direct(
        &mut self,
        from: &str,
        to: &str,
        byte_len: u64,
        sent_at_ns: u64,
        relay_context: Option<&str>,
    ) -> SendOutcome {
        // 1. No declared edge → NoRoute. State unchanged.
        if !self.edges.contains_key(&(from.to_string(), to.to_string())) {
            return SendOutcome::Drop {
                reason: DropReason::NoRoute,
            };
        }
        // 2. Active partition cut → Partitioned. State unchanged.
        if self.is_partitioned(from, to) {
            return SendOutcome::Drop {
                reason: DropReason::Partitioned,
            };
        }
        // From here on we hold a mutable reference to the edge.
        let key = (from.to_string(), to.to_string());

        // 3. Bernoulli loss. LossBurst overrides for the duration.
        let loss_prob_ppm = self.effective_loss_ppm(&key, sent_at_ns);
        let draw = {
            let edge = self.edges.get_mut(&key).unwrap();
            edge.rng.next_u32() % 1_000_000
        };
        if draw < loss_prob_ppm {
            return SendOutcome::Drop {
                reason: DropReason::Lossy,
            };
        }

        // Pre-step: idle cooling.
        self.maybe_idle_cool(&key, sent_at_ns);

        // 4. serialization_start = max(sent_at, last_arrive_ns).
        let policy = self.edges[&key].policy;
        let last_arrive = self.edges[&key].last_arrive_ns.unwrap_or(0);
        let serialization_start = sent_at_ns.max(last_arrive);
        // serialization_end = serialization_start + (byte_len * 1e9 / bandwidth_bps).
        let nanos_for_bytes = ((byte_len as u128).saturating_mul(1_000_000_000u128)
            / (policy.bandwidth_bps as u128)) as u64;
        let serialization_end = serialization_start.saturating_add(nanos_for_bytes);

        // 5. arrival = serialization_end + latency + jitter.
        let jitter = {
            let edge = self.edges.get_mut(&key).unwrap();
            jitter_sample(&mut edge.rng, policy.jitter_stddev_ns)
        };
        let latency_plus_jitter = (policy.latency_ns as i64).saturating_add(jitter).max(0) as u64;

        // 6. LatencySpike: multiply additive latency contribution by factor_x100/100.
        let factor = self.effective_latency_factor(&key, sent_at_ns);
        let scaled_additive = if factor == 100 {
            latency_plus_jitter
        } else {
            ((latency_plus_jitter as u128).saturating_mul(factor as u128) / 100) as u64
        };
        let mut arrival = serialization_end.saturating_add(scaled_additive);

        // 7. RelayBuffer (legacy per-link floor) — applies to direct
        // edges; the new relay vertex has its own delay model.
        if let Some(floor_ns) = self.effective_relay_floor(&key, sent_at_ns) {
            arrival = arrival.max(sent_at_ns.saturating_add(floor_ns));
        }

        // 8. Cold-dial penalty.
        let cache_was_cold = matches!(self.edges[&key].cache, CacheState::Cold);
        if cache_was_cold {
            arrival = arrival.saturating_add(policy.cold_dial_penalty_ns);
            let dial_outcome_at = arrival;
            self.pending_notifications.push(NetworkNotification::DialStart {
                from: from.to_string(),
                to: to.to_string(),
                at_ns: sent_at_ns,
            });
            self.pending_notifications.push(NetworkNotification::DialOutcome {
                from: from.to_string(),
                to: to.to_string(),
                at_ns: dial_outcome_at,
                warm: true,
            });
            let edge = self.edges.get_mut(&key).unwrap();
            edge.cache = CacheState::Warming { since_ns: sent_at_ns };
        }

        // 9. Reorder draw.
        let reorder_draw = {
            let edge = self.edges.get_mut(&key).unwrap();
            edge.rng.next_u32() % 1_000_000
        };
        if reorder_draw < policy.reorder_prob_ppm {
            if let Some(latest) = self.edges[&key]
                .in_flight
                .iter()
                .map(|f| f.scheduled_at_ns)
                .max()
            {
                if latest >= arrival {
                    arrival = latest.saturating_add(1);
                }
            }
        }

        // 10. Update last_send_ns / last_arrive_ns.
        let edge = self.edges.get_mut(&key).unwrap();
        edge.last_send_ns = Some(sent_at_ns);
        edge.last_arrive_ns = Some(arrival);

        if let CacheState::Warming { since_ns } = edge.cache {
            if sent_at_ns.saturating_sub(since_ns) >= policy.cache_warm_after_ns {
                edge.cache = CacheState::Warm;
                let transition_at = since_ns.saturating_add(policy.cache_warm_after_ns);
                self.pending_notifications.push(NetworkNotification::CacheStateChange {
                    from: from.to_string(),
                    to: to.to_string(),
                    at_ns: transition_at,
                    transition: CacheTransition::Warmed,
                });
            }
        }

        let delivery_id = DeliveryId(self.next_delivery_id);
        self.next_delivery_id += 1;
        edge.in_flight.push(InFlight {
            delivery_id,
            scheduled_at_ns: arrival,
            via_relay: relay_context.map(|s| s.to_string()),
        });

        SendOutcome::Arrive { delivery_id, at_ns: arrival }
    }

    /// RELAY_SPEC §4.4 composition. Inbound leg via direct edge, then
    /// relay processing (ingress + egress with optional cold-start),
    /// then outbound leg via direct edge.
    fn send_relayed(
        &mut self,
        from: &str,
        relay: &str,
        to: &str,
        byte_len: u64,
        sent_at_ns: u64,
    ) -> SendOutcome {
        // RELAY_SPEC §4.5 — sends through a killed relay drop with
        // `RelayDown`. The inbound leg's state is not consulted: the
        // relay's deadness is an out-of-band fact about the route.
        if self.relays.get(relay).map(|r| r.killed).unwrap_or(false) {
            self.pending_notifications
                .push(NetworkNotification::RelayDrop {
                    relay: relay.to_string(),
                    from: from.to_string(),
                    to: to.to_string(),
                    byte_len,
                    reason: RelayDropReason::Down,
                    at_ns: sent_at_ns,
                });
            return SendOutcome::Drop {
                reason: DropReason::RelayDown,
            };
        }

        // F3 (sim spec §"cross-pollination"): selective drop of
        // (relay, from, to). Relay is otherwise healthy — other
        // pairs' traffic through it is unaffected. Returned reason
        // is a *distinct* variant from `RelayDown` so the bundle
        // reader can tell "tunnel down" from "peer-via-tunnel down."
        if self.is_relay_peer_down(relay, from, to, sent_at_ns) {
            self.pending_notifications
                .push(NetworkNotification::RelayDrop {
                    relay: relay.to_string(),
                    from: from.to_string(),
                    to: to.to_string(),
                    byte_len,
                    reason: RelayDropReason::PeerConnDown,
                    at_ns: sent_at_ns,
                });
            return SendOutcome::Drop {
                reason: DropReason::RelayPeerConnDown,
            };
        }

        // RELAY_SPEC §4.4 step 1 — inbound leg. Use the *internal*
        // send_direct so the inbound edge's state evolves the same way
        // a normal direct edge would, but the returned arrival time
        // becomes the relay-ingress arrival.
        let inbound = self.send_direct(from, relay, byte_len, sent_at_ns, None);
        let arrival_at_r = match inbound {
            SendOutcome::Arrive { delivery_id, at_ns } => {
                // We tracked an in-flight on the inbound edge as a
                // bookkeeping artefact; for relayed routes the
                // *outbound* leg's in-flight is the canonical one.
                // Drop the inbound bookkeeping so PeerKill / Partition
                // on the inbound leg do not see a phantom message.
                self.discard_inbound_inflight(from, relay, delivery_id);
                at_ns
            }
            SendOutcome::Drop { reason } => return SendOutcome::Drop { reason },
        };

        // RELAY_SPEC §4.4 step 2 — enqueue at the relay; check
        // queue-overflow exact (over-strict: only refuse when adding
        // would push beyond the bound).
        let Some(relay_state) = self.relays.get_mut(relay) else {
            return SendOutcome::Drop {
                reason: DropReason::NoRoute,
            };
        };
        let cleanup_at = arrival_at_r;
        relay_state.cleanup_finished(cleanup_at);
        let enqueued_bytes: u64 = relay_state.in_flight.iter().map(|m| m.byte_len).sum();
        if enqueued_bytes.saturating_add(byte_len) > relay_state.policy.queue_depth_bytes {
            self.pending_notifications
                .push(NetworkNotification::RelayDrop {
                    relay: relay.to_string(),
                    from: from.to_string(),
                    to: to.to_string(),
                    byte_len,
                    reason: RelayDropReason::QueueFull,
                    at_ns: arrival_at_r,
                });
            return SendOutcome::Drop {
                reason: DropReason::RelayQueueFull,
            };
        }
        // RELAY_SPEC §4.4 step 3 — ingress serialization.
        let ingress_capacity = relay_state.policy.ingress_capacity_bps;
        let ingress_serialization = ((byte_len as u128).saturating_mul(1_000_000_000u128)
            / (ingress_capacity as u128)) as u64;
        let ingress_end = arrival_at_r
            .max(relay_state.ingress_queue_tail_ns)
            .saturating_add(ingress_serialization);
        relay_state.ingress_queue_tail_ns = ingress_end;

        // RELAY_SPEC §4.4 step 4 — egress serialization. Cold start
        // penalty fires once per `Booting` state.
        let egress_capacity = relay_state.policy.egress_capacity_bps_per_link;
        let egress_serialization = ((byte_len as u128).saturating_mul(1_000_000_000u128)
            / (egress_capacity as u128)) as u64;
        let egress_tail = *relay_state
            .egress_queue_tail_ns
            .get(to)
            .unwrap_or(&0);
        let mut egress_start = ingress_end.max(egress_tail);
        if let BootState::Booting { .. } = relay_state.boot_state {
            egress_start = egress_start.saturating_add(relay_state.policy.cold_start_penalty_ns);
            relay_state.boot_state = BootState::Booted;
        }
        let egress_end = egress_start.saturating_add(egress_serialization);
        relay_state
            .egress_queue_tail_ns
            .insert(to.to_string(), egress_end);

        // RELAY_SPEC §4.4 step 7 — emit enqueue/dequeue notifications.
        self.pending_notifications
            .push(NetworkNotification::RelayEnqueue {
                relay: relay.to_string(),
                from: from.to_string(),
                to: to.to_string(),
                byte_len,
                at_ns: arrival_at_r,
            });
        self.pending_notifications
            .push(NetworkNotification::RelayDequeue {
                relay: relay.to_string(),
                from: from.to_string(),
                to: to.to_string(),
                byte_len,
                at_ns: egress_end,
            });

        // RELAY_SPEC §4.4 step 5 — outbound leg. The outbound edge's
        // own bandwidth model serializes on top of the relay's egress.
        let outbound = self.send_direct(relay, to, byte_len, egress_end, Some(relay));
        let final_arrival = match outbound {
            SendOutcome::Arrive { at_ns, .. } => at_ns,
            SendOutcome::Drop { reason } => {
                // The relay scheduling has already happened; we still
                // emit the dequeue notification (it represents the
                // relay's view) but the composed send drops.
                return SendOutcome::Drop { reason };
            }
        };

        // Record on the relay's in-flight so RelayKill can invalidate.
        let delivery_id = match self.last_outbound_delivery_id(relay, to) {
            Some(d) => d,
            None => DeliveryId(self.next_delivery_id.saturating_sub(1)),
        };
        let relay_state = self.relays.get_mut(relay).unwrap();
        relay_state.in_flight.push(RelayInFlight {
            delivery_id,
            from_host: from.to_string(),
            to_host: to.to_string(),
            byte_len,
            arrival_at_ns: final_arrival,
            egress_end_ns: egress_end,
        });

        SendOutcome::Arrive {
            delivery_id,
            at_ns: final_arrival,
        }
    }

    /// Forget the inbound-leg bookkeeping created by
    /// `send_direct(from, relay, …)`. The outbound leg owns the
    /// canonical in-flight entry for relayed routes.
    fn discard_inbound_inflight(&mut self, from: &str, relay: &str, delivery_id: DeliveryId) {
        if let Some(edge) = self
            .edges
            .get_mut(&(from.to_string(), relay.to_string()))
        {
            edge.in_flight.retain(|f| f.delivery_id != delivery_id);
        }
    }

    /// The delivery id the last-issued outbound `send_direct` returned
    /// (it pushes onto the outbound edge's `in_flight`; the relay
    /// then mirrors that id into its own in-flight).
    fn last_outbound_delivery_id(&self, relay: &str, to: &str) -> Option<DeliveryId> {
        self.edges
            .get(&(relay.to_string(), to.to_string()))?
            .in_flight
            .last()
            .map(|f| f.delivery_id)
    }

    /// Inform the network that the engine has delivered (or otherwise
    /// removed) a previously-scheduled delivery.
    pub fn notify_delivered(&mut self, from: &str, to: &str, delivery_id: DeliveryId) {
        // Direct edge bookkeeping.
        if let Some(edge) = self.edges.get_mut(&(from.to_string(), to.to_string())) {
            edge.in_flight.retain(|f| f.delivery_id != delivery_id);
        }
        // For relayed routes, the canonical edge is `relay → to`; the
        // engine still calls us with `(from = original sender, to)`.
        // Look up the route to find the relay.
        let via_relay = self
            .routes
            .get(&(from.to_string(), to.to_string()))
            .and_then(|r| match r {
                HostRoute::Relayed { relay, .. } => Some(relay.clone()),
                HostRoute::Direct { .. } => None,
            });
        if let Some(relay) = via_relay {
            if let Some(edge) = self
                .edges
                .get_mut(&(relay.clone(), to.to_string()))
            {
                edge.in_flight.retain(|f| f.delivery_id != delivery_id);
            }
            if let Some(rs) = self.relays.get_mut(&relay) {
                rs.in_flight.retain(|m| m.delivery_id != delivery_id);
            }
        }
    }

    /// Per §3.2 / §5.5 + RELAY_SPEC §4.5 / §6.1.
    pub fn apply_mutation(
        &mut self,
        mutation: &Mutation,
        at_ns: u64,
    ) -> Vec<InvalidatedDelivery> {
        match &mutation.kind {
            MutationKind::Partition { peers_a, peers_b } => {
                let mut invalidated = Vec::new();
                for a in peers_a {
                    for b in peers_b {
                        if a == b {
                            continue;
                        }
                        let pair = sorted_pair(a, b);
                        self.partitioned.insert(pair);
                        invalidated.extend(self.drain_in_flight_for(a, b));
                        invalidated.extend(self.drain_in_flight_for(b, a));
                    }
                }
                invalidated
            }
            MutationKind::Heal => {
                self.partitioned.clear();
                Vec::new()
            }
            MutationKind::LatencySpike {
                links,
                factor_x100,
                duration_ns,
            } => {
                let resolved = resolve_links(links);
                self.active_latency_spike.push(TimedEffect {
                    start_ns: at_ns,
                    end_ns: at_ns.saturating_add(*duration_ns),
                    payload: LatencySpike {
                        links: resolved,
                        factor_x100: *factor_x100,
                    },
                });
                Vec::new()
            }
            MutationKind::LossBurst {
                links,
                prob_ppm,
                duration_ns,
            } => {
                let resolved = resolve_links(links);
                self.active_loss_burst.push(TimedEffect {
                    start_ns: at_ns,
                    end_ns: at_ns.saturating_add(*duration_ns),
                    payload: LossBurst {
                        links: resolved,
                        prob_ppm: *prob_ppm,
                    },
                });
                Vec::new()
            }
            MutationKind::RelayBuffer {
                links,
                floor_ns,
                duration_ns,
            } => {
                let resolved = resolve_links(links);
                self.active_relay_buffer.push(TimedEffect {
                    start_ns: at_ns,
                    end_ns: at_ns.saturating_add(*duration_ns),
                    payload: RelayBuffer {
                        links: resolved,
                        floor_ns: *floor_ns,
                    },
                });
                Vec::new()
            }
            MutationKind::PeerKill { peer } => {
                self.killed_peers.insert(peer.clone());
                let mut invalidated = Vec::new();
                let edge_keys: Vec<(String, String)> = self
                    .edges
                    .keys()
                    .filter(|(_, to)| to == peer)
                    .cloned()
                    .collect();
                for (from, to) in edge_keys {
                    invalidated.extend(self.drain_in_flight_for(&from, &to));
                    let edge = self.edges.get_mut(&(from.clone(), to.clone())).unwrap();
                    if !matches!(edge.cache, CacheState::Cold) {
                        edge.cache = CacheState::Cold;
                        self.pending_notifications.push(
                            NetworkNotification::CacheStateChange {
                                from: from.clone(),
                                to: to.clone(),
                                at_ns,
                                transition: CacheTransition::Invalidated,
                            },
                        );
                    }
                }
                invalidated
            }
            MutationKind::PeerResurrect { peer, .. } => {
                self.killed_peers.remove(peer);
                Vec::new()
            }
            MutationKind::WorkerExit { .. } => {
                // RELAY_SPEC §6.1 — `WorkerExit` is engine-side; the
                // network has nothing to invalidate. The engine
                // dispatches it as a recv envelope to the target host.
                Vec::new()
            }
            MutationKind::RelayKill { relay } => self.apply_relay_kill(relay, at_ns),
            MutationKind::RelayPeerConnDown {
                relay,
                from,
                to,
                duration_ns,
            } => {
                // duration_ns == 0 ⇒ permanent for the rest of the
                // run (until u64::MAX). Matches the spec's expected
                // "set and forget" use case for incident-replay
                // scenarios.
                let end_ns = if *duration_ns == 0 {
                    u64::MAX
                } else {
                    at_ns.saturating_add(*duration_ns)
                };
                self.active_relay_peer_down.push(TimedEffect {
                    start_ns: at_ns,
                    end_ns,
                    payload: RelayPeerDown {
                        relay: relay.clone(),
                        from: from.clone(),
                        to: to.clone(),
                    },
                });
                // Invalidate any in-flight delivery on the outbound
                // leg from this relay to `to` — same shape as
                // `PeerKill` cleans up in-flight deliveries.
                self.drain_in_flight_for(relay, to)
            }
            MutationKind::RelayBoot { relay } => {
                self.apply_relay_boot(relay, at_ns);
                Vec::new()
            }
            MutationKind::RelayCapacityChange {
                relay,
                ingress_capacity_bps,
                egress_capacity_bps_per_link,
                queue_depth_bytes,
            } => {
                self.apply_relay_capacity_change(
                    relay,
                    *ingress_capacity_bps,
                    *egress_capacity_bps_per_link,
                    *queue_depth_bytes,
                );
                Vec::new()
            }
        }
        // `_seed` is captured at construction; we keep it on the type
        // so future randomness in mutations (none currently do) can
        // derive a substream from `("mutation", index)`.
    }

    // ── Relay mutation helpers ───────────────────────────────────────

    fn apply_relay_kill(&mut self, relay: &str, _at_ns: u64) -> Vec<InvalidatedDelivery> {
        let Some(rs) = self.relays.get_mut(relay) else {
            return Vec::new();
        };
        rs.killed = true;
        let drained = std::mem::take(&mut rs.in_flight);
        // Each drained entry has a canonical in-flight on the outbound
        // edge (relay → to_host). Drop it there too, so the engine
        // doesn't think the delivery is still pending.
        let invalidated: Vec<_> = drained
            .into_iter()
            .map(|m| InvalidatedDelivery {
                delivery_id: m.delivery_id,
                from: m.from_host,
                to: m.to_host,
                scheduled_at_ns: m.arrival_at_ns,
            })
            .collect();
        for inv in &invalidated {
            if let Some(edge) = self.edges.get_mut(&(relay.to_string(), inv.to.clone())) {
                edge.in_flight.retain(|f| f.delivery_id != inv.delivery_id);
            }
        }
        invalidated
    }

    fn apply_relay_boot(&mut self, relay: &str, at_ns: u64) {
        if let Some(rs) = self.relays.get_mut(relay) {
            rs.killed = false;
            rs.in_flight.clear();
            rs.egress_queue_tail_ns.clear();
            rs.ingress_queue_tail_ns = 0;
            rs.boot_state = BootState::Booting { since_ns: at_ns };
        }
    }

    fn apply_relay_capacity_change(
        &mut self,
        relay: &str,
        ingress: Option<u64>,
        egress: Option<u64>,
        depth: Option<u64>,
    ) {
        if let Some(rs) = self.relays.get_mut(relay) {
            if let Some(v) = ingress {
                rs.policy.ingress_capacity_bps = v;
            }
            if let Some(v) = egress {
                rs.policy.egress_capacity_bps_per_link = v;
            }
            if let Some(v) = depth {
                rs.policy.queue_depth_bytes = v;
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn is_partitioned(&self, from: &str, to: &str) -> bool {
        let pair = sorted_pair(from, to);
        self.partitioned.contains(&pair)
    }

    /// F3: is the (relay, from, to) triple currently cut by an
    /// active `RelayPeerConnDown` mutation? Directional — a cut from
    /// A→B does not imply B→A is cut.
    fn is_relay_peer_down(&self, relay: &str, from: &str, to: &str, now_ns: u64) -> bool {
        self.active_relay_peer_down.iter().any(|effect| {
            now_ns >= effect.start_ns
                && now_ns < effect.end_ns
                && effect.payload.relay == relay
                && effect.payload.from == from
                && effect.payload.to == to
        })
    }

    fn effective_loss_ppm(&self, key: &(String, String), now_ns: u64) -> u32 {
        let base = self.edges[key].policy.loss_prob_ppm;
        let mut best = base;
        for effect in &self.active_loss_burst {
            if now_ns >= effect.start_ns
                && now_ns < effect.end_ns
                && effect.payload.links.contains(key)
            {
                best = effect.payload.prob_ppm;
            }
        }
        best
    }

    fn effective_latency_factor(&self, key: &(String, String), now_ns: u64) -> u32 {
        let mut factor: u32 = 100;
        for effect in &self.active_latency_spike {
            if now_ns >= effect.start_ns
                && now_ns < effect.end_ns
                && effect.payload.links.contains(key)
            {
                factor = effect.payload.factor_x100;
            }
        }
        factor
    }

    fn effective_relay_floor(&self, key: &(String, String), now_ns: u64) -> Option<u64> {
        let mut best: Option<u64> = None;
        for effect in &self.active_relay_buffer {
            if now_ns >= effect.start_ns
                && now_ns < effect.end_ns
                && effect.payload.links.contains(key)
            {
                best = Some(best.map_or(effect.payload.floor_ns, |b| b.max(effect.payload.floor_ns)));
            }
        }
        best
    }

    fn maybe_idle_cool(&mut self, key: &(String, String), now_ns: u64) {
        let edge = self.edges.get_mut(key).unwrap();
        let invalidate_after = edge.policy.cache_invalidate_after_idle_ns;
        let go_cold = matches!(edge.cache, CacheState::Warming { .. } | CacheState::Warm)
            && match edge.last_send_ns {
                Some(last) => now_ns.saturating_sub(last) >= invalidate_after,
                None => false,
            };
        if go_cold {
            edge.cache = CacheState::Cold;
            self.pending_notifications.push(NetworkNotification::CacheStateChange {
                from: key.0.clone(),
                to: key.1.clone(),
                at_ns: now_ns,
                transition: CacheTransition::IdleCooled,
            });
        }
    }

    fn drain_in_flight_for(&mut self, from: &str, to: &str) -> Vec<InvalidatedDelivery> {
        let Some(edge) = self.edges.get_mut(&(from.to_string(), to.to_string())) else {
            return Vec::new();
        };
        let drained = std::mem::take(&mut edge.in_flight);
        // For each drained entry, also remove the mirror from the
        // relay (if any) so RelayKill semantics stay consistent with
        // PeerKill semantics for relayed routes.
        for f in &drained {
            if let Some(relay) = f.via_relay.clone() {
                if let Some(rs) = self.relays.get_mut(&relay) {
                    rs.in_flight.retain(|m| m.delivery_id != f.delivery_id);
                }
            }
        }
        drained
            .into_iter()
            .map(|f| InvalidatedDelivery {
                delivery_id: f.delivery_id,
                from: from.to_string(),
                to: to.to_string(),
                scheduled_at_ns: f.scheduled_at_ns,
            })
            .collect()
    }

    /// Allow tests to inspect the seed without depending on the
    /// internal representation. Kept private to the crate.
    #[allow(dead_code)]
    pub(crate) fn seed(&self) -> u64 {
        self.seed
    }
}

impl RelayState {
    /// Drop in-flight entries whose `arrival_at_ns` is at or before
    /// `now_ns`. RELAY_SPEC §4.4 step 6 — the relay decrements
    /// `enqueued_bytes` lazily once the message clears egress.
    fn cleanup_finished(&mut self, now_ns: u64) {
        self.in_flight.retain(|m| m.egress_end_ns > now_ns);
    }
}

fn sorted_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

fn resolve_links(refs: &[LinkRef]) -> BTreeSet<(String, String)> {
    refs.iter()
        .map(|l| (l.from.clone(), l.to.clone()))
        .collect()
}

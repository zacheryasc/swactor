//! The network (SIM_SPEC §5).
//!
//! A directed-graph link model with deterministic per-edge state, the
//! §5.4 send algorithm, the §5.5 mutation suite, and a §7-conformant
//! integer-only computation path (no floats touch any decision).
//!
//! The network owns no schedule of its own; the engine pops events and
//! queries the network. Each query mutates per-link state but never
//! reads from any clock outside the `now_ns` the engine supplies.

use std::collections::{BTreeMap, BTreeSet};

use crate::rng::{SubstreamKey, SubstreamRng, jitter_sample};
use crate::scenario::{LinkPolicy, LinkRef, Mutation, MutationKind, Scenario};

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
    /// The (from, to) pair has no declared edge.
    NoRoute,
    /// The active partition set cuts this edge.
    Partitioned,
    /// The Bernoulli loss draw for the edge fired.
    Lossy,
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
    /// Killed peers. Their inbound deliveries are invalidated when the
    /// kill mutation runs; later sends to them still return NoRoute is
    /// the engine's job (the kill is a peer-state thing the engine
    /// owns). The network exposes a list of in-flight deliveries to
    /// the killed peer for invalidation.
    killed_peers: BTreeSet<String>,
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
        Self {
            edges,
            partitioned: BTreeSet::new(),
            active_latency_spike: Vec::new(),
            active_loss_burst: Vec::new(),
            active_relay_buffer: Vec::new(),
            killed_peers: BTreeSet::new(),
            seed: scenario.seed,
            next_delivery_id: 0,
            pending_notifications: Vec::new(),
        }
    }

    /// Drain side-channel notifications produced since the last call.
    pub fn take_pending_notifications(&mut self) -> Vec<NetworkNotification> {
        std::mem::take(&mut self.pending_notifications)
    }

    /// Read-only test hook: total number of in-flight deliveries.
    pub fn in_flight_count(&self) -> usize {
        self.edges.values().map(|e| e.in_flight.len()).sum()
    }

    /// Per §3.2 / §5.4.
    pub fn send(&mut self, from: &str, to: &str, byte_len: u64, sent_at_ns: u64) -> SendOutcome {
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

        // Pre-step: idle cooling. If last_send_ns - now > cache_invalidate_after_idle_ns,
        // transition to Cold and emit a CacheStateChange.
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

        // 7. RelayBuffer floor: arrival = max(arrival, sent_at + floor_ns).
        if let Some(floor_ns) = self.effective_relay_floor(&key, sent_at_ns) {
            arrival = arrival.max(sent_at_ns.saturating_add(floor_ns));
        }

        // 8. Cold-dial penalty. If the cache is Cold, add the penalty;
        // emit DialStart and DialOutcome notifications.
        let cache_was_cold = matches!(self.edges[&key].cache, CacheState::Cold);
        if cache_was_cold {
            arrival = arrival.saturating_add(policy.cold_dial_penalty_ns);
            // §5.4 step 8 says "DialOutcome at the arrival time" —
            // the only `arrival` in scope at that point is the
            // post-penalty value, so capture *after* the add.
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
            // §5.4 step 8 — transition Cold → Warming(sent_at_ns).
            // No CacheStateChange notification here; per §5.4 step 10
            // the Warmed event fires only when Warming→Warm crosses
            // the cache_warm_after_ns threshold.
            let edge = self.edges.get_mut(&key).unwrap();
            edge.cache = CacheState::Warming { since_ns: sent_at_ns };
        }

        // 9. Reorder draw. If it fires, push arrival past the next
        // scheduled delivery on this edge. We pick the latest
        // in-flight arrival on the edge plus a small delta.
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

        // §5.4 step 10 — if Warming and the cumulative warm-after
        // threshold has been crossed, transition Warming → Warm and
        // emit the single CacheStateChange{Warmed} notification.
        // The notification carries the actual transition time
        // (`since_ns + cache_warm_after_ns`) rather than the
        // observing send's `sent_at_ns`; the bundle reader sees the
        // moment the link became warm, not the moment the engine
        // happened to detect it.
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

        // Record in-flight.
        let delivery_id = DeliveryId(self.next_delivery_id);
        self.next_delivery_id += 1;
        edge.in_flight.push(InFlight {
            delivery_id,
            scheduled_at_ns: arrival,
        });

        SendOutcome::Arrive { delivery_id, at_ns: arrival }
    }

    /// Inform the network that the engine has delivered (or otherwise
    /// removed) a previously-scheduled delivery. The network drops
    /// the corresponding in-flight entry; this is how `in_flight`
    /// stays accurate for mutation invalidation.
    pub fn notify_delivered(&mut self, from: &str, to: &str, delivery_id: DeliveryId) {
        if let Some(edge) = self.edges.get_mut(&(from.to_string(), to.to_string())) {
            edge.in_flight.retain(|f| f.delivery_id != delivery_id);
        }
    }

    /// Per §3.2 / §5.5.
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
                // Invalidate every delivery destined to the peer.
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
                    // Invalidate cache for that edge per §5.5 (kill
                    // resets the link). Emit a notification.
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
        }
        // `_seed` is captured at construction; we keep it on the type
        // so future randomness in mutations (none currently) can derive
        // a substream from `("mutation", index)`.
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn is_partitioned(&self, from: &str, to: &str) -> bool {
        let pair = sorted_pair(from, to);
        self.partitioned.contains(&pair)
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

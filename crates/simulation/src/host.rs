//! Host trait and engine ⇄ host contract types (SIM_SPEC §3.2, §6.1).
//!
//! A host kind is `(Host impl, codec)`. The engine treats every host
//! through the same trait; the codec lives next to the host kind and
//! produces the opaque bytes the engine carries through the network.
//!
//! The engine does not know the host's message type. Every payload
//! crossing the engine ⇄ host boundary is a `HostMessage` envelope; the
//! envelope's `App` arm carries a pre-encoded byte slice the host
//! decodes on receipt.

use crate::network::DropReason;

/// Stable identifier for a host. Engine routes by `HostId`; the
/// scenario loader produces them from `peer.id` strings.
pub type HostId = String;

/// Opaque host-kind tag, e.g. `"swim"`. Used for bundle tagging and
/// for routing decisions where the engine has to distinguish kinds.
pub type KindTag = &'static str;

/// Caller-defined token a host attaches to a timer it scheduled. The
/// engine echoes it back through `recv` when the timer fires; no
/// engine code interprets it.
pub type TimerToken = u64;

/// Snapshot bytes per §6.1. Opaque to the engine.
pub type SnapshotBytes = Vec<u8>;

/// Event payload bytes a host produces via `RecordEvent`. Opaque to
/// the engine.
pub type EventBytes = Vec<u8>;

/// Envelope the engine passes to `Host::recv`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostMessage {
    /// An application-level message the host kind encoded on the
    /// sender side. The receiver decodes via its own codec.
    App(Vec<u8>),
    /// A scheduled timer fired; `token` echoes the value the host
    /// passed to `ScheduleTimer`.
    TimerFired { token: TimerToken },
    /// A `Send` the network refused. The sender — not the destination
    /// — receives this so the host can react (re-queue, log, etc.).
    SendFailed { to: HostId, reason: DropReason },
    /// RELAY_SPEC §3.1 / §5.3 — an internal worker-exit signal. The
    /// stage host kind consumes this; other kinds refuse it and the
    /// engine aborts the run with a structured error if it is
    /// delivered to a kind that does not accept it.
    WorkerExit {
        reason: String,
        status_code: Option<i32>,
        signal: Option<i32>,
    },
}

/// One thing a host's `tick` or `recv` can ask the engine to do.
///
/// The engine processes the returned vector left-to-right (§4.10 action
/// ordering). An action outside this closed set is a §4.10 closed-action-
/// set violation; the engine aborts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Send `encoded` to `to`. The engine asks the network with
    /// `byte_len = encoded.len()`, and on `Arrive` queues a `Deliver`
    /// carrying the same bytes.
    Send { to: HostId, encoded: Vec<u8> },
    /// Forward an event payload to the bundle writer with the
    /// current virtual time.
    RecordEvent { kind_tag: String, event: EventBytes },
    /// Ask the engine to fire `TimerFired(token)` at `at_ns`.
    ScheduleTimer { at_ns: u64, token: TimerToken },
    /// Stop dispatching ticks to this host (§4.3). Deliveries still
    /// flow.
    Halt,
}

/// The host kind contract. Implementors are dynamically dispatched by
/// the engine (`Box<dyn Host>`), so no host-kind-specific generics
/// leak into engine code.
pub trait Host: Send {
    fn id(&self) -> &str;
    fn kind_tag(&self) -> KindTag;

    fn tick(&mut self, now_ns: u64) -> Vec<Action>;
    fn recv(&mut self, message: HostMessage, now_ns: u64) -> Vec<Action>;
    fn snapshot(&self) -> SnapshotBytes;
}

/// The engine's host-instantiation factory (SIM_SPEC §6.3 step 4). One
/// `HostFactory` per kind; the engine asks it for fresh host instances
/// when a scenario declares peers of that kind or when a
/// `PeerResurrect { preserve_state: false }` mutation replaces a host.
pub trait HostFactory: Send + Sync {
    fn kind_tag(&self) -> KindTag;
    fn build(
        &self,
        host_id: &str,
        kind_config: &toml::value::Table,
        peers: &[String],
        tick_period_ns: u64,
    ) -> Box<dyn Host>;
}

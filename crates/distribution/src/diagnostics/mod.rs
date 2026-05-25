//! Diagnostics: structured observability for the iroh / SWIM layer.
//!
//! The story is in `examples/pipeline-parallel-inference/DIAGNOSTICS_PLAN.md`.
//! Briefly: every node in a run emits typed events and periodic
//! snapshots into a per-process [`Aggregator`], which forwards them
//! through a [`Sink`] to a collector. The collector assembles a
//! tarball at end-of-run, which the post-processor renders into a
//! one-pager that answers "what failed and why."
//!
//! This module is the API surface — types and traits. IO (HTTP sink,
//! on-disk spool, the collector binary) lives in later stages.
//!
//! Existing call sites in `crates/distribution` thread an
//! `Arc<dyn Sink>`, defaulting to [`NoopSink`], so adding diagnostics
//! never changes behavior of code that does not opt in.

pub mod aggregator;
pub mod dep_versions;
pub mod event;
pub mod host_introspect;
pub mod identity;
pub mod probes;
pub mod process_stats;
pub mod reachability;
pub mod registry_introspect;
pub mod relay_observability;
pub mod sink;
pub mod subprocess_introspect;
pub mod snapshot;
pub mod spool;
pub mod swim_introspect;
pub mod vastai_context;

#[cfg(feature = "collector")]
pub mod collector;
#[cfg(feature = "iroh")]
pub mod iroh_introspect;
#[cfg(feature = "collector")]
pub mod postproc;
#[cfg(feature = "collector")]
pub mod signal;

pub use aggregator::Aggregator;
pub use dep_versions::{GIT_SHA, IROH_VERSION};
pub use event::{ConnType, DialOutcome, Event, EventRecord, PeerState};
pub use identity::{HostContext, Identity, Role};
pub use reachability::{PeerReachability, StateTransition};
pub use sink::{
    DynEmitter, DynSink, EventEmitter, InMemorySink, NoopEmitter, NoopSink, Sink, noop_emitter,
};
#[cfg(feature = "collector")]
pub use sink::{HttpSink, SinkConfig, SinkHandle};
#[cfg(feature = "collector")]
pub use signal::SnapshotSignal;
pub use host_introspect::HostIntrospect;
pub use probes::ProbeScheduler;
pub use process_stats::ProcessStats;
pub use relay_observability::RelayObservability;
pub use snapshot::{
    HostIntrospector, IrohIntrospector, MetricSample, MetricValueWire, ProbeIntrospector,
    ProcessIntrospector, RegistryIntrospector, RelayServerIntrospector, Snapshot, SnapshotBody,
    SnapshotTrigger, SubprocessIntrospector, SwimIntrospector, Tier2ConnectionCache,
    Tier2IrohState, Tier2Peer, Tier2Registry, Tier2RegistryEntry, Tier2RelaySession,
    Tier2SwimConfig, Tier2SwimMessage, Tier2SwimPeer, Tier2SwimState, Tier3DnsResolution,
    Tier3HostNetwork, Tier3HostState, Tier3Interface, Tier3InterfaceCounters, Tier3Probe,
    Tier3ProbeState, Tier3ProcessStats, Tier3RelayServer, Tier3Route, Tier3Subprocess,
    Tier3SubprocessState, Tier3TokioStats, Tier3UdpKernelStats, Tier3UdpSocket,
    Tier3VastaiContext, TransportAddrWire, VastaiIntrospector,
};
pub use subprocess_introspect::SubprocessIntrospect;
pub use registry_introspect::RegistryIntrospect;
pub use swim_introspect::SwimIntrospect;
pub use vastai_context::VastaiContext;
#[cfg(feature = "iroh")]
pub use iroh_introspect::{ConnectionCacheTracker, IrohIntrospect};

/// Current wall clock in milliseconds since the UNIX epoch. One
/// canonical helper so feature-gated and non-gated code paths agree.
pub fn wall_ms_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

//! The vastai monitoring layer — an independent telemetry layer for rented
//! vast.ai GPU nodes.
//!
//! This layer tracks only the state of rented vast.ai instances: their external
//! vast.ai API view (cost, lifecycle, live util) and their in-VM OS/GPU/log view.
//! It is deliberately decoupled from the swactor (actor / SWIM / iroh)
//! diagnostics:
//!
//!   * its records ([`record`]) are their own typed schema, not swactor `Event`s;
//!   * its transport ([`shipper`]) is self-contained, not the swactor `HttpSink`;
//!   * it reuses only the collector's content-agnostic storage + SSE, under the
//!     `vastai_*` [`RecordKind`](crate::diagnostics::collector::RecordKind)s.
//!
//! As a result, vastai monitoring runs with swactor diagnostics off and vice
//! versa. (Note: the unrelated [`vastai_context`](crate::diagnostics::vastai_context)
//! module is a swactor *snapshot field* capturing boot-time env vars — not part of
//! this layer.)

pub mod record;
pub mod sampler;

#[cfg(feature = "collector")]
pub mod logs;
#[cfg(feature = "collector")]
pub mod shipper;

pub use record::{
    CgroupSample, CpuSample, DiskSample, GpuSample, HostSample, InstanceObservation, LifecycleEvent,
    LogBatch, LogLine, LogStream, MemSample, NetSample, Source, VastaiBody, VastaiNodeRef,
    VastaiRecord, VASTAI_SCHEMA_VERSION,
};

pub use sampler::{GpuSource, NoGpu, Sampler};

#[cfg(feature = "collector")]
pub use sampler::{spawn_sampler, SamplerHandle};
#[cfg(feature = "collector")]
pub use logs::{LogForwarder, LogForwarderConfig, LogFlusherHandle};
#[cfg(feature = "collector")]
pub use shipper::{VastaiShipper, VastaiShipperConfig, VastaiShipperHandle};

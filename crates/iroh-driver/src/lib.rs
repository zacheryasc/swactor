//! `iroh-driver` — iroh-backed transport driver for the actorized distribution stack.
//!
//! This crate owns the concrete iroh endpoint/QUIC/relay machinery. The
//! distribution crate owns cluster dynamics, protocol actors, routing claims, and
//! wire message definitions.

// Engine boundary enforcement: disallowed scheduling/time/core-driving methods
// are hard errors in this crate (ENGINE_SPEC.md §2). All engine-hosted
// work goes through `EngineHandle`.
#![deny(clippy::disallowed_methods)]

pub mod driver_pumps;
pub mod edge_transport;
pub mod endpoint_advertisement;
pub mod iroh_driver;
pub mod telemetry_transport;

pub use endpoint_advertisement::{
    EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint,
};
pub use iroh_driver::{
    ConnType, IrohDriver, IrohDriverConfig, JoinPhase, JoinStatus, TelemetryPublishHandle,
    conn_type_of, discover_lan_ips,
};

pub use edge_transport::{EDGE_ALPN, EdgeSendHandle, EdgeTransportEvent, EdgeTransportFault};

pub use telemetry_transport::{
    TELEMETRY_ALPN, TelemetryQuicHeader, TelemetryQuicRead, TelemetryQuicWriteStats,
    read_events_from_stream, read_next_event, read_next_uni_from_connection, read_pull_request,
    read_stream_header, read_stream_into_fanout, spawn_connection_reader, spawn_pull_collector,
    spawn_pull_server, spawn_subscription_writer, write_available_subscription, write_event,
    write_pull_request, write_subscription_until_closed,
};

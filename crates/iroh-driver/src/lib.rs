//! `iroh-driver` — iroh-backed transport driver for the actorized distribution stack.
//!
//! This crate owns the concrete iroh endpoint/QUIC/relay machinery. The
//! distribution crate owns cluster dynamics, protocol actors, routing claims, and
//! wire message definitions.

pub mod datastream_transport;
pub mod iroh_driver;

pub use iroh_driver::{
    ConnType, IrohDriver, IrohDriverConfig, JoinPhase, JoinStatus, conn_type_of, discover_lan_ips,
};

pub use datastream_transport::{
    DATASTREAM_ALPN, DatastreamQuicHeader, DatastreamQuicRead, DatastreamQuicWriteStats,
    read_deliveries_from_stream, read_next_uni_from_connection, read_stream_into_fanout,
    spawn_connection_reader, spawn_subscription_writer, write_available_subscription,
    write_delivery, write_subscription_until_closed,
};

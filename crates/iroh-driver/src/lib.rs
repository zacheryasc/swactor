//! `iroh-driver` — iroh-backed transport driver for the actorized distribution stack.
//!
//! This crate owns the concrete iroh endpoint/QUIC/relay machinery. The
//! distribution crate owns cluster dynamics, protocol actors, routing claims, and
//! wire message definitions.

pub mod datastream_transport;
pub mod driver_pumps;
pub mod edge_transport;
pub mod endpoint_advertisement;
pub mod iroh_driver;

pub use iroh_driver::{
    ConnType, DatastreamPublishHandle, IrohDriver, IrohDriverConfig, JoinPhase, JoinStatus,
    conn_type_of, discover_lan_ips,
};
pub use endpoint_advertisement::{
    MVP_IROH_ENDPOINT_ADDR_MASK_ENV, EndpointAddrMask, advertised_endpoint,
};

pub use edge_transport::{EDGE_ALPN, EdgeSendHandle, EdgeTransportEvent, EdgeTransportFault};

pub use datastream_transport::{
    DATASTREAM_ALPN, DatastreamQuicHeader, DatastreamQuicRead, DatastreamQuicWriteStats,
    read_events_from_stream, read_next_event, read_next_uni_from_connection, read_stream_header,
    read_stream_into_fanout, spawn_connection_reader, spawn_subscription_writer,
    write_available_subscription, write_event, write_subscription_until_closed,
};

//! The per-node telemetry **datastream** (see `DATASTREAM_SPEC.md`).
//!
//! A deliberately dumb pipe: producers dump bytes tagged by channel, a
//! single per-node mux interleaves them into one ordered stream, a
//! best-effort transport carries that stream to the one consumer, ingest
//! reconstructs each node's stream by position, and views are read-time
//! projections over the stored stream. Nothing between a producer and a
//! view interprets the payload.
//!
//! ```text
//!    producers (caller-owned records + text)
//!             │  bytes tagged by channel        → [`record::Record`]
//!             ▼
//!         per-node MUX                          → [`mux::Mux`]
//!             │  one ordered stream of [`Frame`]s
//!             ▼
//!    best-effort transport                      → [`transport`]
//!             │  delivery: frames, maybe dropped/reordered/delayed
//!             ▼
//!       consumer INGEST                         → [`ingest::Consumer`]
//!             │  complete stream, stored whole
//!             ▼
//!      stored STREAM (truth)                    → [`store`]
//!             │  read-time only
//!             ▼
//!         VIEWS                                 → [`views`]
//! ```
//!
//! The data model ([`frame`]), extension contract ([`record`]), and wire
//! envelope ([`wire`]) are the seams a test observes. Channel meanings live in
//! producer/consumer crates, not in a datastream-wide catalog.

pub mod emit;
pub mod endpoint;
pub mod frame;
pub mod health;
pub mod ingest;
pub mod mux;
pub mod record;
pub mod sink_actor;
pub mod store;
pub mod timing;
pub mod transport;
pub mod views;
pub mod wire;

pub use endpoint::{
    DatastreamEndpoint, DatastreamProducer, DatastreamSubscription, DeliveryFanout, EndpointTick,
    SubscriberSnapshot, SubscriptionId,
};
pub use frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
pub use ingest::Consumer;
pub use mux::Mux;
pub use record::{ChannelKind, ChannelRegistry, Record};
pub use sink_actor::{DATASTREAM_SINK_NAME, DatastreamSink};
pub use store::{GapSpan, Store, StoredStream};
pub use timing::{FRAME_TIME_CHANNEL, FrameTimeSample};
pub use transport::{Delivery, Reorder, ScriptedTransport, StreamScript};
pub use views::{Body, LogEntry, MergedFrame};

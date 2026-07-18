//! The per-node telemetry **datastream** (see `DATASTREAM_SPEC.md`).
//!
//! A deliberately dumb pipe: producers dump bytes tagged by stream-local
//! channel id, a single per-node mux accepts those bytes and assigns canonical
//! positions during drain, the endpoint broadcasts catalog-aware events to
//! subscribers, ingest reconstructs streams by position, and views are
//! read-time projections over stored frames. Nothing between a producer and a
//! view interprets the payload.
//!
//! ```text
//!    producers (caller-owned records + text)
//!             │  bytes tagged by registered ChannelId
//!             ▼
//!    endpoint / catalog                         → [`endpoint`]
//!             │  channel metadata + producer handles
//!             ▼
//!    per-node MUX                               → [`mux::Mux`]
//!             │  positioned [`frame::Frame`]s
//!             ▼
//!    endpoint fanout                            → [`endpoint::DeliveryFanout`]
//!             │  catalog-aware events, maybe dropped per subscriber
//!             ▼
//!    ingest / store                             → [`ingest`], [`store`]
//!             │  position-keyed frame truth
//!             ▼
//!    views                                      → [`views`]
//! ```
//!
//! The data model ([`frame`]), extension contract ([`record`]), endpoint/fanout
//! seam ([`endpoint`]), and compatibility wire helpers ([`wire`]) are the seams
//! tests observe. Channel meanings live in producer/consumer crates, not in a
//! datastream-wide global registry.

pub mod emit;
pub mod endpoint;
pub mod frame;
pub mod hardware;
pub mod health;
pub mod ingest;
pub mod mux;
pub mod publisher_actor;
pub mod record;
pub mod sink_actor;
pub mod store;
pub mod transport;
pub mod views;
pub mod wire;

pub use endpoint::{
    CatalogSnapshot, ChannelRegistrationError, DatastreamEndpoint, DatastreamProducer,
    DatastreamSnapshot, DatastreamSubscription, DeliveryFanout, EndpointTick, SubscriberSnapshot,
    SubscriptionId, frame_event_to_delivery,
};
pub use frame::{
    ChannelContent, ChannelContentKind, ChannelDescriptor, ChannelFilter, ChannelId, ChannelRef,
    DatastreamEvent, Frame, FrameDelivery, Lifetime, NodeId, Position, SourceFilter,
    StreamDescriptor, StreamId, StreamOrigin, SubscriptionRequest,
};
pub use ingest::Consumer;
pub use mux::Mux;
pub use publisher_actor::{
    DATASTREAM_PUBLISHER_NAME, DatastreamPublisherActor, DatastreamPublisherMsg,
    DatastreamSubscribe, register_datastream_publisher_codec,
};
pub use record::{ChannelKind, ChannelRegistry, Record};
pub use sink_actor::{DATASTREAM_SINK_NAME, DatastreamSink};
pub use store::{GapSpan, Store, StoredStream};
pub use transport::{Delivery, Reorder, ScriptedTransport, StreamScript};
pub use views::{Body, LogEntry, MergedFrame};

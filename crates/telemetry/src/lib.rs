//! The per-node telemetry **telemetry** (see `TELEMETRY_SPEC.md`).
//!
//! A deliberately dumb pipe: producers dump bytes tagged with a stream-local
//! channel id, a single per-node mux accepts those bytes and assigns canonical
//! positions during drain, the endpoint broadcasts catalog-aware events to
//! subscribers, ingest reconstructs streams by position, and views are
//! read-time projections over stored frames. Nothing between a producer and a
//! view interprets the payload.
//!
//! ## Producer vs observer surface
//!
//! This crate has two surfaces:
//!
//! - **Producer** — re-exported at the crate root ([`TelemetryEndpoint`],
//!   [`TelemetryProducer`], [`Record`], [`ChannelId`], [`StreamId`], …).
//!   Everything control-plane and actor code needs to *emit* telemetry.
//!
//! - **Observer** — in submodules ([`frame::Frame`], [`frame::TelemetryEvent`],
//!   [`store::Store`], [`views`], [`ingest::Consumer`]).  Everything a sink
//!   (dashboard, archive, transport) needs to *read* telemetry.
//!
//! The crate root deliberately does **not** re-export [`frame::Frame`] or
//! [`frame::TelemetryEvent`].  `use telemetry::Frame` is a compile error; the
//! full path `telemetry::frame::Frame` compiles but is banned in control-plane
//! modules by `cargo xtask check-telemetry-isolation`.
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
//! telemetry-wide global registry.

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

// ── Producer surface (re-exported at root; safe for control-plane code) ──

pub use endpoint::{
    CatalogSnapshot, ChannelRegistrationError, TelemetryEndpoint, TelemetryProducer,
    TelemetrySnapshot, TelemetrySubscription, DeliveryFanout, EndpointTick, SubscriberSnapshot,
    SubscriptionId,
};
pub use frame::{
    ChannelContent, ChannelContentKind, ChannelDescriptor, ChannelFilter, ChannelId, ChannelRef,
    Lifetime, NodeId, Position, SourceFilter, StreamDescriptor, StreamId, StreamOrigin,
    SubscriptionRequest,
};
pub use mux::Mux;
pub use publisher_actor::{
    TELEMETRY_PUBLISHER_NAME, TelemetryPublisherActor, TelemetryPublisherMsg,
    TelemetrySubscribe, register_telemetry_publisher_codec,
};
pub use record::{ChannelKind, ChannelRegistry, Record};
pub use sink_actor::{TELEMETRY_SINK_NAME, TelemetrySink};

// ── Observer surface (in submodules; NOT re-exported at root) ──
//
// frame::Frame, frame::TelemetryEvent, frame::FrameDelivery,
// store::Store, ingest::Consumer, views::*, transport::Delivery
//
// Access these via their module paths (e.g. `telemetry::frame::Frame`).
// Control-plane modules must not import them — enforced by CI.

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
//!    producers (typed + text)
//!             │  bytes tagged by channel        → [`catalog`]
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
//! The data model ([`frame`]) and the wire envelope ([`wire`]) are the
//! seams a test observes; the catalog ([`catalog`]) is the schema contract
//! between producers and views. See `DATASTREAM_TESTING_SPEC.md` for how
//! the pipe is verified.

pub mod catalog;
pub mod emit;
pub mod frame;
pub mod ingest;
pub mod mux;
pub mod sink_actor;
pub mod source;
pub mod store;
pub mod transport;
pub mod views;
pub mod wire;

pub use frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
pub use ingest::Consumer;
pub use sink_actor::{DatastreamSink, DATASTREAM_SINK_NAME};
pub use mux::Mux;
pub use store::{GapSpan, Store, StoredStream};
pub use transport::{Delivery, Reorder, ScriptedTransport, StreamScript};
pub use views::{Body, LogEntry, MergedFrame};

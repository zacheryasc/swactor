mod actor;
mod builder;
mod engine;
mod error;

pub use actor::WasmActor;
pub use builder::WasmActorBuilder;
pub use engine::SharedEngine;
pub use error::WasmActorError;

/// A message carrying raw bytes, suitable for passing to/from Wasm guests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteMessage(pub Vec<u8>);

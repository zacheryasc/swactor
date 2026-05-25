//! Simulator MVP. The contract is
//! `examples/pipeline-parallel-inference/SIM_SPEC.md`. Each module here
//! corresponds to one of the six components named in SIM_SPEC §3.1;
//! crossing-boundary types are public, internals are private.

pub mod bundle;
pub mod bundle_file;
pub mod engine;
pub mod evaluator;
pub mod host;
pub mod network;
pub mod parity_host;
pub mod property;
pub mod rng;
pub mod scenario;
pub mod stage_host;
pub mod swim_codec;
pub mod swim_host;

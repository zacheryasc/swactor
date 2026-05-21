//! Sim-side facade backend and engine I/O.
//!
//! This directory is allowlisted by `lint-deterministic` (per
//! TESTING_SPEC §4.1's facade-impl allowlist) so it can call the
//! raw `std::fs` / `std::env` / `std::time` surfaces the rest of
//! the simulation crate must not touch directly. Everything below
//! the facade boundary lives here.

pub mod bundle;
pub mod poison;

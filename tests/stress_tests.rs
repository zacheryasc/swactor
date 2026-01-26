//! Stress test suite for swactor runtime.
//!
//! Run with: cargo test --features stress stress_ -- --nocapture
//!
//! These tests are hidden behind the `stress` feature flag because they:
//! - Take longer to run
//! - Intentionally push the system to failure
//! - May produce different results on different machines

#[cfg(feature = "stress")]
mod stress;

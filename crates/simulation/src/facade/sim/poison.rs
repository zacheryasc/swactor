//! Test-only RNG poisoning switch (TESTING_SPEC §2.5).
//!
//! Lives inside the sim-facade impl directory because the spec
//! requires the poison switch to be the *only* test-only switch
//! permitted, and to live in the sim facade crate. The flag is a
//! thread-local cell so concurrent tests (each running on its own
//! thread) cannot leak poison between runs.
//!
//! When the cell is set, the engine xor-mutates a single byte of
//! the loopback host's synthesised `Custom` event payload (see
//! `engine.rs`), giving the divergence detector a deterministic
//! single-record disagreement to surface.

use std::cell::Cell;

thread_local! {
    static POISON_RNG: Cell<bool> = const { Cell::new(false) };
}

/// Read the current poison flag for this thread.
pub fn is_poisoned() -> bool {
    POISON_RNG.with(|c| c.get())
}

/// Enable / disable poisoning for the current thread. Returns the
/// previous value so callers can scope-restore in a guard.
pub fn set_poison(value: bool) -> bool {
    POISON_RNG.with(|c| c.replace(value))
}

/// RAII guard that flips `POISON_RNG` on construction and restores
/// the previous value on drop. Used by the divergence-check helper
/// so a panic inside the poisoned run cannot leave poison set.
pub struct PoisonGuard {
    previous: bool,
}

impl PoisonGuard {
    pub fn engage() -> Self {
        let previous = set_poison(true);
        Self { previous }
    }
}

impl Drop for PoisonGuard {
    fn drop(&mut self) {
        set_poison(self.previous);
    }
}

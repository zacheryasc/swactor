//! Runtime guarantee verification modules.
//!
//! Consolidates all formal verification (Kani bounded model checking)
//! and exhaustive correspondence testing into a single module tree.
//!
//! - `g4_lifecycle`: Kani proofs for lifecycle ordering (G4)
//! - `g5_fault_isolation`: Tests for fault isolation (G5)
//! - `g6_g7_death_orphan`: Tests for death notifications (G6) and orphan cleanup (G7)
//! - `g10_supervisor`: Kani proofs for supervisor restart decisions (G10)
//! - `correspondence`: Exhaustive deterministic tests verifying production decision functions match runtime behavior

#[cfg(kani)]
mod g4_lifecycle;

#[cfg(kani)]
mod g10_supervisor;

#[cfg(test)]
mod g5_fault_isolation;

#[cfg(test)]
mod g6_g7_death_orphan;

#[cfg(test)]
mod correspondence;

#[cfg(test)]
mod model_checker;

#[cfg(test)]
mod stateright_lifecycle;

#[cfg(test)]
mod stateright_death_orphan;

#[cfg(test)]
mod stateright_supervisor;

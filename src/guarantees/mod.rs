//! Runtime guarantee verification modules.
//!
//! Formal and exhaustive checks now cover core runtime behavior only. Alpha std
//! features that are not used by production crates were pruned from the beta
//! surface, so their feature-specific guarantee modules are no longer compiled.

#[cfg(kani)]
mod g4_lifecycle;

#[cfg(test)]
mod g5_fault_isolation;

#[cfg(test)]
mod correspondence;

#[cfg(test)]
mod model_checker;

#[cfg(test)]
mod stateright_lifecycle;

#[cfg(test)]
mod stateright_death_orphan;

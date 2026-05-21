//! The runtime trait surface is fingerprinted; the lock value lives
//! in `surface.lock`. Live ≠ locked is a hard fail. Updating the
//! surface and the lock together is a single commit.

#[test]
fn live_fingerprint_matches_locked() {
    let live = simulation::runtime::surface_fingerprint();
    let locked = include_str!("../surface.lock").trim();
    assert_eq!(
        live, locked,
        "runtime surface fingerprint drifted.\n\
         live   = {live}\n\
         locked = {locked}\n\
         If the trait surface change is intentional, update \
         crates/simulation/surface.lock in the same commit."
    );
}

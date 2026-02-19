//! Human-readable name generation for swactor nodes.
//!
//! Produces deterministic `adjective-animal` names from a keypair's
//! public key bytes, so the same identity always gets the same default name.

use distribution::crypto::Keypair;

const ADJECTIVES: &[&str] = &[
    "bold", "brave", "bright", "calm", "clever",
    "cool", "crisp", "deft", "eager", "fair",
    "fast", "fierce", "fleet", "fond", "frank",
    "free", "fresh", "glad", "grand", "green",
    "happy", "hardy", "keen", "kind", "light",
    "live", "lucky", "merry", "mild", "neat",
    "noble", "pale", "plain", "prime", "proud",
    "pure", "quick", "quiet", "rapid", "ready",
    "rich", "sharp", "sleek", "smart", "solid",
    "sound", "stout", "sure", "swift", "wise",
];

const ANIMALS: &[&str] = &[
    "badger", "bear", "bison", "bobcat", "crane",
    "crow", "deer", "dove", "eagle", "elk",
    "falcon", "finch", "fox", "frog", "goose",
    "hare", "hawk", "heron", "horse", "ibis",
    "jackal", "jay", "kite", "lark", "lion",
    "lynx", "marten", "mink", "moose", "newt",
    "otter", "owl", "panda", "pike", "puma",
    "quail", "raven", "robin", "salmon", "seal",
    "shrike", "snake", "sparrow", "stork", "swan",
    "tiger", "toad", "trout", "viper", "wolf",
];

/// Generate a deterministic human-readable name from a keypair.
///
/// Uses the first two bytes of the public key (node ID) to index into
/// the adjective and animal word lists.
pub fn generate_name(keypair: &Keypair) -> String {
    let id = keypair.node_id();
    let adj_idx = id.0[0] as usize % ADJECTIVES.len();
    let animal_idx = id.0[1] as usize % ANIMALS.len();
    format!("{}-{}", ADJECTIVES[adj_idx], ANIMALS[animal_idx])
}

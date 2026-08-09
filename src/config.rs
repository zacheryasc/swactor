/// The tunable settings for the runtime.
pub struct RuntimeConfig {
    pub max_actors: usize,
    pub channel_buffer_size: usize,
    /// Maximum messages processed per actor per tick.
    /// Prevents a single actor with a large mailbox from starving others.
    /// `0` means unlimited (drain entire mailbox).
    pub actor_message_budget: usize,
}

/// 8kB for the `Box<..>` before counting the rest of the memory
const DEFAULT_MAX_ACTORS: usize = 1_000;

/// Pre-allocated ring buffer capacity for each channel (transfer, spawn, inbox).
/// When the ring is full, messages overflow into an unbounded backup queue.
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1_000;

/// Default per-actor message budget per tick.
/// Inspired by BEAM's reduction budget (4000) and tokio's cooperative budget (128).
/// 64 is a good default: high enough for throughput, low enough for fairness.
const DEFAULT_ACTOR_MESSAGE_BUDGET: usize = 64;

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_actors: DEFAULT_MAX_ACTORS,
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            actor_message_budget: DEFAULT_ACTOR_MESSAGE_BUDGET,
        }
    }
}

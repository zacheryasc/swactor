/// What to do when a bounded mailbox is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxOverflow {
    /// Drop the incoming message (newest). The message is silently discarded.
    DropNewest,
    /// Drop the oldest message in the queue to make room for the new one.
    DropOldest,
}

/// The tunable settings for the runtime.
pub struct RuntimeConfig {
    pub max_actors: usize,
    pub channel_buffer_size: usize,
    pub num_threads: usize,
    /// Maximum messages processed per actor per tick.
    /// Prevents a single actor with a large mailbox from starving others.
    /// `0` means unlimited (drain entire mailbox).
    pub actor_message_budget: usize,
    /// Default per-actor mailbox capacity. `0` means unbounded (no limit).
    /// When non-zero, `mailbox_overflow` controls what happens when the mailbox is full.
    pub default_mailbox_capacity: usize,
    /// Overflow policy for bounded mailboxes. Ignored when `default_mailbox_capacity` is 0.
    pub mailbox_overflow: MailboxOverflow,
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
            num_threads: 1,
            actor_message_budget: DEFAULT_ACTOR_MESSAGE_BUDGET,
            default_mailbox_capacity: 0,
            mailbox_overflow: MailboxOverflow::DropNewest,
        }
    }
}

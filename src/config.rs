/// Backoff policy for worker threads when idle.
///
/// Workers spin → yield → sleep with increasing delay when no work is available.
pub struct BackoffPolicy {
    /// Number of idle ticks before switching from spin to yield.
    pub spin_threshold: u32,
    /// Number of idle ticks before switching from yield to sleep.
    pub yield_threshold: u32,
    /// Microseconds added per tick beyond the yield threshold.
    pub sleep_increment_us: u64,
    /// Maximum sleep duration in microseconds.
    pub sleep_max_us: u64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            spin_threshold: 64,
            yield_threshold: 256,
            sleep_increment_us: 50,
            sleep_max_us: 1000,
        }
    }
}

/// The tunable settings for the runtime.
pub struct RuntimeConfig {
    pub max_actors: usize,
    pub actor_max_messages: usize,
    pub num_threads: usize,
    pub backoff_policy: BackoffPolicy,
}

/// 8kB for the `Box<..>` before counting the rest of the memory
const DEFAULT_MAX_ACTORS: usize = 1_000;

/// 16kB PER ACTOR to alloc space for storing messages.
/// With default setting of [DEFAULT_MAX_ACTORS] this is:
/// 1_000 * 16kB = 16MB
const DEFAULT_ACTOR_MAX_MESSAGES: usize = 1_000;

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_actors: DEFAULT_MAX_ACTORS,
            actor_max_messages: DEFAULT_ACTOR_MAX_MESSAGES,
            num_threads: 1,
            backoff_policy: BackoffPolicy::default(),
        }
    }
}

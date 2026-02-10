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
    pub channel_buffer_size: usize,
    pub num_threads: usize,
    pub backoff_policy: BackoffPolicy,
}

/// 8kB for the `Box<..>` before counting the rest of the memory
const DEFAULT_MAX_ACTORS: usize = 1_000;

/// Pre-allocated ring buffer capacity for each channel (transfer, spawn, inbox).
/// When the ring is full, messages overflow into an unbounded backup queue.
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1_000;

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_actors: DEFAULT_MAX_ACTORS,
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            num_threads: 1,
            backoff_policy: BackoffPolicy::default(),
        }
    }
}

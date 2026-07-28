use crate::staging::shard_fetch::{
    FetchError, ShardCache, ShardFetchCoordinator, ShardFetchStatus, ShardFetcher, ShardLocation,
};
use crate::staging::weight_shards::{ShardAssignment, ShardValidationError, ValidatedShard};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardLifecycleState {
    Idle,
    Assigned,
    Located,
    Fetching,
    Fetched,
    Validating,
    Binding,
    Ready,
    Faulted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardLifecycleEvent {
    Assigned { assignment: ShardAssignment },
    Located { location: ShardLocation },
    Fetching { location: ShardLocation },
    CacheHit { cache_key: String },
    Fetched { uri: String, local_path: String },
    Validated { local_path: String },
    Binding { local_path: String },
    Ready { local_path: String },
    Faulted { reason: ShardLifecycleFault },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardLifecycleFault {
    Fetch(FetchError),
    Validation(ShardValidationError),
    Bind(BindError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindError {
    WorkerRejected,
    DeviceAllocationFailed,
}

pub trait WorkerShardBinder {
    fn bind(&mut self, shard: &ValidatedShard) -> Result<(), BindError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardWeightLifecycle {
    state: ShardLifecycleState,
    events: Vec<ShardLifecycleEvent>,
}

impl ShardWeightLifecycle {
    pub fn new() -> Self {
        Self {
            state: ShardLifecycleState::Idle,
            events: Vec::new(),
        }
    }

    pub fn state(&self) -> ShardLifecycleState {
        self.state
    }

    pub fn events(&self) -> &[ShardLifecycleEvent] {
        &self.events
    }

    pub fn load<C, F, B>(
        &mut self,
        assignment: ShardAssignment,
        cache: &mut C,
        fetcher: &mut F,
        binder: &mut B,
    ) where
        C: ShardCache,
        F: ShardFetcher,
        B: WorkerShardBinder,
    {
        if matches!(
            self.state,
            ShardLifecycleState::Ready | ShardLifecycleState::Faulted
        ) {
            return;
        }

        self.state = ShardLifecycleState::Assigned;
        self.events.push(ShardLifecycleEvent::Assigned {
            assignment: assignment.clone(),
        });

        let location = crate::staging::shard_fetch::ShardLocator::locate(&assignment);
        self.state = ShardLifecycleState::Located;
        self.events.push(ShardLifecycleEvent::Located {
            location: location.clone(),
        });

        self.state = ShardLifecycleState::Fetching;
        self.events.push(ShardLifecycleEvent::Fetching {
            location: location.clone(),
        });

        let outcome = match ShardFetchCoordinator::get_or_fetch(&assignment, cache, fetcher) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.fault(ShardLifecycleFault::Fetch(error));
                return;
            }
        };

        match outcome.status {
            ShardFetchStatus::CacheHit => self.events.push(ShardLifecycleEvent::CacheHit {
                cache_key: outcome.location.cache_key,
            }),
            ShardFetchStatus::Downloaded => self.events.push(ShardLifecycleEvent::Fetched {
                uri: outcome.location.uri,
                local_path: outcome.shard.local_path.clone(),
            }),
        }

        self.state = ShardLifecycleState::Fetched;
        self.state = ShardLifecycleState::Validating;
        let validated = match ValidatedShard::new(
            assignment,
            outcome.shard.manifest,
            outcome.shard.local_path.clone(),
        ) {
            Ok(validated) => validated,
            Err(error) => {
                self.fault(ShardLifecycleFault::Validation(error));
                return;
            }
        };
        self.events.push(ShardLifecycleEvent::Validated {
            local_path: validated.local_path.clone(),
        });

        self.state = ShardLifecycleState::Binding;
        self.events.push(ShardLifecycleEvent::Binding {
            local_path: validated.local_path.clone(),
        });
        if let Err(error) = binder.bind(&validated) {
            self.fault(ShardLifecycleFault::Bind(error));
            return;
        }

        self.state = ShardLifecycleState::Ready;
        self.events.push(ShardLifecycleEvent::Ready {
            local_path: validated.local_path,
        });
    }

    fn fault(&mut self, reason: ShardLifecycleFault) {
        self.state = ShardLifecycleState::Faulted;
        self.events.push(ShardLifecycleEvent::Faulted { reason });
    }
}

impl Default for ShardWeightLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

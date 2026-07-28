use std::collections::BTreeMap;

use mvp_system::staging::shard_fetch as fetch;
use mvp_system::staging::shard_weight_lifecycle as lifecycle;
use mvp_system::staging::weight_shards as shards;

fn assignment() -> shards::ShardAssignment {
    let model_ref =
        shards::ModelArtifactRef::parse("hf://org/repo@abcdef123456/model.gguf").unwrap();
    let split_scheme = shards::SplitScheme::GgufLayerContiguousV1;
    let split_id = shards::SplitId::derive(&model_ref, split_scheme);
    shards::ShardAssignment::new(
        model_ref,
        split_id,
        split_scheme,
        3,
        8,
        shards::LayerRange::new(12, 16).unwrap(),
    )
    .unwrap()
}

fn fetched_for(assignment: &shards::ShardAssignment) -> fetch::FetchedShard {
    fetch::FetchedShard::new(
        "/cache/stage-00003.gguf",
        shards::ShardManifest::for_assignment(
            assignment,
            shards::ContentHash::literal("sha256:stage-3").unwrap(),
        ),
    )
}

fn event_kinds(events: &[lifecycle::ShardLifecycleEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match event {
            lifecycle::ShardLifecycleEvent::Assigned { .. } => "assigned",
            lifecycle::ShardLifecycleEvent::Located { .. } => "located",
            lifecycle::ShardLifecycleEvent::Fetching { .. } => "fetching",
            lifecycle::ShardLifecycleEvent::CacheHit { .. } => "cache-hit",
            lifecycle::ShardLifecycleEvent::Fetched { .. } => "fetched",
            lifecycle::ShardLifecycleEvent::Validated { .. } => "validated",
            lifecycle::ShardLifecycleEvent::Binding { .. } => "binding",
            lifecycle::ShardLifecycleEvent::Ready { .. } => "ready",
            lifecycle::ShardLifecycleEvent::Faulted { .. } => "faulted",
        })
        .collect()
}

#[derive(Default)]
struct MemoryCache {
    entries: BTreeMap<String, fetch::FetchedShard>,
    inserts: usize,
}

impl fetch::ShardCache for MemoryCache {
    fn get(&self, cache_key: &str) -> Option<fetch::FetchedShard> {
        self.entries.get(cache_key).cloned()
    }

    fn insert(&mut self, cache_key: String, shard: fetch::FetchedShard) {
        self.inserts += 1;
        self.entries.insert(cache_key, shard);
    }
}

struct RecordingFetcher {
    calls: Vec<fetch::FetchShard>,
    result: Result<fetch::FetchedShard, fetch::FetchError>,
}

impl fetch::ShardFetcher for RecordingFetcher {
    fn fetch(
        &mut self,
        request: &fetch::FetchShard,
    ) -> Result<fetch::FetchedShard, fetch::FetchError> {
        self.calls.push(request.clone());
        self.result.clone()
    }
}

struct RecordingBinder {
    calls: Vec<shards::ValidatedShard>,
    result: Result<(), lifecycle::BindError>,
}

impl lifecycle::WorkerShardBinder for RecordingBinder {
    fn bind(&mut self, shard: &shards::ValidatedShard) -> Result<(), lifecycle::BindError> {
        self.calls.push(shard.clone());
        self.result.clone()
    }
}

#[test]
fn lifecycle_happy_path_reaches_ready() {
    let assignment = assignment();
    let fetched = fetched_for(&assignment);
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Ok(fetched.clone()),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Ok(()),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment.clone(), &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Ready);
    assert_eq!(
        event_kinds(lifecycle.events()),
        vec![
            "assigned",
            "located",
            "fetching",
            "fetched",
            "validated",
            "binding",
            "ready"
        ]
    );
    assert_eq!(fetcher.calls.len(), 1);
    assert_eq!(cache.inserts, 1);
    assert_eq!(binder.calls.len(), 1);
    assert_eq!(binder.calls[0].assignment, assignment);
    assert_eq!(binder.calls[0].local_path, fetched.local_path);
}

#[test]
fn lifecycle_cache_hit_skips_fetch_but_still_validates_and_binds() {
    let assignment = assignment();
    let fetched = fetched_for(&assignment);
    let location = fetch::ShardLocator::locate(&assignment);
    let mut cache = MemoryCache::default();
    cache.entries.insert(location.cache_key.clone(), fetched);
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Err(fetch::FetchError::Unavailable),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Ok(()),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment, &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Ready);
    assert_eq!(
        event_kinds(lifecycle.events()),
        vec![
            "assigned",
            "located",
            "fetching",
            "cache-hit",
            "validated",
            "binding",
            "ready"
        ]
    );
    assert!(fetcher.calls.is_empty());
    assert_eq!(cache.inserts, 0);
    assert_eq!(binder.calls.len(), 1);
}

#[test]
fn lifecycle_fetch_failure_faults_without_validation_or_bind() {
    let assignment = assignment();
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Err(fetch::FetchError::NotFound),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Ok(()),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment, &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Faulted);
    assert_eq!(fetcher.calls.len(), 1);
    assert!(binder.calls.is_empty());
    assert!(matches!(
        lifecycle.events().last(),
        Some(lifecycle::ShardLifecycleEvent::Faulted {
            reason: lifecycle::ShardLifecycleFault::Fetch(fetch::FetchError::NotFound)
        })
    ));
}

#[test]
fn lifecycle_validation_failure_faults_without_bind() {
    let assignment = assignment();
    let mut bad_manifest = shards::ShardManifest::for_assignment(
        &assignment,
        shards::ContentHash::literal("sha256:stage-3").unwrap(),
    );
    bad_manifest.stage_index = 4;
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Ok(fetch::FetchedShard::new(
            "/cache/bad-stage.gguf",
            bad_manifest,
        )),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Ok(()),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment, &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Faulted);
    assert!(binder.calls.is_empty());
    assert!(matches!(
        lifecycle.events().last(),
        Some(lifecycle::ShardLifecycleEvent::Faulted {
            reason: lifecycle::ShardLifecycleFault::Validation(
                shards::ShardValidationError::StageIndexMismatch
            )
        })
    ));
}

#[test]
fn lifecycle_bind_failure_faults_after_validation() {
    let assignment = assignment();
    let fetched = fetched_for(&assignment);
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Ok(fetched),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Err(lifecycle::BindError::WorkerRejected),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment, &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Faulted);
    assert_eq!(binder.calls.len(), 1);
    assert!(event_kinds(lifecycle.events()).contains(&"validated"));
    assert!(matches!(
        lifecycle.events().last(),
        Some(lifecycle::ShardLifecycleEvent::Faulted {
            reason: lifecycle::ShardLifecycleFault::Bind(lifecycle::BindError::WorkerRejected)
        })
    ));
}

#[test]
fn ready_terminal_state_is_idempotent() {
    let assignment = assignment();
    let fetched = fetched_for(&assignment);
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Ok(fetched.clone()),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Ok(()),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment.clone(), &mut cache, &mut fetcher, &mut binder);
    let event_count = lifecycle.events().len();
    lifecycle.load(assignment, &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Ready);
    assert_eq!(lifecycle.events().len(), event_count);
    assert_eq!(fetcher.calls.len(), 1);
    assert_eq!(binder.calls.len(), 1);
}

#[test]
fn fault_terminal_state_is_idempotent() {
    let assignment = assignment();
    let fetched = fetched_for(&assignment);
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Err(fetch::FetchError::Unavailable),
    };
    let mut binder = RecordingBinder {
        calls: Vec::new(),
        result: Ok(()),
    };
    let mut lifecycle = lifecycle::ShardWeightLifecycle::new();

    lifecycle.load(assignment.clone(), &mut cache, &mut fetcher, &mut binder);
    let event_count = lifecycle.events().len();
    fetcher.result = Ok(fetched);
    lifecycle.load(assignment, &mut cache, &mut fetcher, &mut binder);

    assert_eq!(lifecycle.state(), lifecycle::ShardLifecycleState::Faulted);
    assert_eq!(lifecycle.events().len(), event_count);
    assert_eq!(fetcher.calls.len(), 1);
    assert!(binder.calls.is_empty());
}

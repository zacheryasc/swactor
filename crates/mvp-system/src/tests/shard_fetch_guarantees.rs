use std::collections::BTreeMap;

use mvp_system::staging::shard_fetch as fetch;
use mvp_system::staging::weight_shards as shards;

fn assignment(stage_index: u32) -> shards::ShardAssignment {
    let model_ref =
        shards::ModelArtifactRef::parse("hf://org/repo@abcdef123456/model.gguf").unwrap();
    let split_scheme = shards::SplitScheme::GgufLayerContiguousV1;
    let split_id = shards::SplitId::derive(&model_ref, split_scheme);
    shards::ShardAssignment::new(
        model_ref,
        split_id,
        split_scheme,
        stage_index,
        8,
        shards::LayerRange::new(stage_index * 4, stage_index * 4 + 4).unwrap(),
    )
    .unwrap()
}

fn fetched_for(assignment: &shards::ShardAssignment) -> fetch::FetchedShard {
    fetch::FetchedShard::new(
        format!("/cache/stage-{:05}.gguf", assignment.stage_index),
        shards::ShardManifest::for_assignment(
            assignment,
            shards::ContentHash::literal(format!("sha256:stage-{}", assignment.stage_index))
                .unwrap(),
        ),
    )
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

#[test]
fn shard_locator_derives_expected_uri() {
    let assignment = assignment(3);
    let location = fetch::ShardLocator::locate(&assignment);

    assert_eq!(
        location.uri,
        format!(
            "hf://org/repo@abcdef123456/shards/{}/stage-00003.gguf",
            assignment.split_id.as_str()
        )
    );
    assert!(location.cache_key.contains(assignment.split_id.as_str()));
    assert!(location.cache_key.ends_with(":00003"));
}

#[test]
fn cache_key_depends_on_model_split_and_stage() {
    let stage_three = assignment(3);
    let stage_four = assignment(4);

    let different_model_ref =
        shards::ModelArtifactRef::parse("hf://other/repo@abcdef123456/model.gguf").unwrap();
    let split_scheme = shards::SplitScheme::GgufLayerContiguousV1;
    let different_model = shards::ShardAssignment::new(
        different_model_ref.clone(),
        shards::SplitId::derive(&different_model_ref, split_scheme),
        split_scheme,
        3,
        8,
        shards::LayerRange::new(12, 16).unwrap(),
    )
    .unwrap();

    let different_split = shards::ShardAssignment::new(
        stage_three.model_ref.clone(),
        shards::SplitId::literal("split-other").unwrap(),
        stage_three.split_scheme,
        3,
        8,
        stage_three.layer_range,
    )
    .unwrap();

    let keys = [
        fetch::ShardLocator::locate(&stage_three).cache_key,
        fetch::ShardLocator::locate(&stage_four).cache_key,
        fetch::ShardLocator::locate(&different_model).cache_key,
        fetch::ShardLocator::locate(&different_split).cache_key,
    ];

    assert_eq!(
        keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
        keys.len()
    );
}

#[test]
fn coordinator_downloads_and_caches_missing_shard() {
    let assignment = assignment(2);
    let expected = fetched_for(&assignment);
    let mut cache = MemoryCache::default();
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Ok(expected.clone()),
    };

    let outcome =
        fetch::ShardFetchCoordinator::get_or_fetch(&assignment, &mut cache, &mut fetcher).unwrap();

    assert_eq!(outcome.status, fetch::ShardFetchStatus::Downloaded);
    assert_eq!(outcome.shard, expected);
    assert_eq!(fetcher.calls.len(), 1);
    assert_eq!(fetcher.calls[0].location, outcome.location);
    assert_eq!(cache.inserts, 1);
    assert_eq!(
        cache.entries.get(&outcome.location.cache_key),
        Some(&expected)
    );
}

#[test]
fn coordinator_uses_cache_hit_without_fetching() {
    let assignment = assignment(5);
    let location = fetch::ShardLocator::locate(&assignment);
    let cached = fetched_for(&assignment);
    let mut cache = MemoryCache::default();
    cache
        .entries
        .insert(location.cache_key.clone(), cached.clone());
    let mut fetcher = RecordingFetcher {
        calls: Vec::new(),
        result: Err(fetch::FetchError::Unavailable),
    };

    let outcome =
        fetch::ShardFetchCoordinator::get_or_fetch(&assignment, &mut cache, &mut fetcher).unwrap();

    assert_eq!(outcome.status, fetch::ShardFetchStatus::CacheHit);
    assert_eq!(outcome.location, location);
    assert_eq!(outcome.shard, cached);
    assert!(fetcher.calls.is_empty());
    assert_eq!(cache.inserts, 0);
}

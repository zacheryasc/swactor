use crate::staging::weight_shards::{ShardAssignment, ShardManifest};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardLocation {
    pub uri: String,
    pub cache_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchShard {
    pub location: ShardLocation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedShard {
    pub local_path: String,
    pub manifest: ShardManifest,
}

impl FetchedShard {
    pub fn new(local_path: impl Into<String>, manifest: ShardManifest) -> Self {
        Self {
            local_path: local_path.into(),
            manifest,
        }
    }
}

pub struct ShardLocator;

impl ShardLocator {
    pub fn locate(assignment: &ShardAssignment) -> ShardLocation {
        let uri = assignment
            .model_ref
            .shard_uri(&assignment.split_id, assignment.stage_index);
        let cache_key = format!(
            "{}:{}:{:05}",
            assignment.expected_model_digest().as_str(),
            assignment.split_id.as_str(),
            assignment.stage_index,
        );
        ShardLocation { uri, cache_key }
    }
}

pub trait ShardCache {
    fn get(&self, cache_key: &str) -> Option<FetchedShard>;
    fn insert(&mut self, cache_key: String, shard: FetchedShard);
}

pub trait ShardFetcher {
    fn fetch(&mut self, request: &FetchShard) -> Result<FetchedShard, FetchError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchError {
    Unauthorized,
    NotFound,
    Unavailable,
    IntegrityMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardFetchStatus {
    CacheHit,
    Downloaded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardFetchOutcome {
    pub shard: FetchedShard,
    pub location: ShardLocation,
    pub status: ShardFetchStatus,
}

pub struct ShardFetchCoordinator;

impl ShardFetchCoordinator {
    pub fn get_or_fetch<C, F>(
        assignment: &ShardAssignment,
        cache: &mut C,
        fetcher: &mut F,
    ) -> Result<ShardFetchOutcome, FetchError>
    where
        C: ShardCache,
        F: ShardFetcher,
    {
        let location = ShardLocator::locate(assignment);
        if let Some(shard) = cache.get(&location.cache_key) {
            return Ok(ShardFetchOutcome {
                shard,
                location,
                status: ShardFetchStatus::CacheHit,
            });
        }

        let request = FetchShard {
            location: location.clone(),
        };
        let shard = fetcher.fetch(&request)?;
        cache.insert(location.cache_key.clone(), shard.clone());
        Ok(ShardFetchOutcome {
            shard,
            location,
            status: ShardFetchStatus::Downloaded,
        })
    }
}

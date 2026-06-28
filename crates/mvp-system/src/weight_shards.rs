#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelArtifactRef {
    canonical: String,
    repo: String,
    revision: String,
    path: String,
}

impl ModelArtifactRef {
    pub fn parse(value: impl Into<String>) -> Result<Self, ModelArtifactRefError> {
        let value = value.into();
        let rest = value
            .strip_prefix("hf://")
            .ok_or(ModelArtifactRefError::InvalidScheme)?;
        let (repo, revision_and_path) = rest
            .split_once('@')
            .ok_or(ModelArtifactRefError::MissingRevision)?;
        let (revision, path) = revision_and_path
            .split_once('/')
            .ok_or(ModelArtifactRefError::MissingPath)?;
        Self::hugging_face(repo, revision, path)
    }

    pub fn hugging_face(
        repo: impl Into<String>,
        revision: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self, ModelArtifactRefError> {
        let repo = repo.into().trim_matches('/').to_owned();
        let revision = revision.into();
        let path = path.into().trim_start_matches('/').to_owned();
        if repo.is_empty() {
            return Err(ModelArtifactRefError::MissingRepo);
        }
        if revision.is_empty() {
            return Err(ModelArtifactRefError::MissingRevision);
        }
        if revision.contains('/') {
            return Err(ModelArtifactRefError::RevisionMustBePathSegment);
        }
        if path.is_empty() {
            return Err(ModelArtifactRefError::MissingPath);
        }
        let canonical = format!("hf://{repo}@{revision}/{path}");
        Ok(Self {
            canonical,
            repo,
            revision,
            path,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub fn repo(&self) -> &str {
        &self.repo
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn model_digest(&self) -> ModelDigest {
        ModelDigest(stable_digest_hex(&["model", self.as_str()]))
    }

    pub fn shard_uri(&self, split_id: &SplitId, stage_index: u32) -> String {
        format!(
            "hf://{}@{}/shards/{}/stage-{stage_index:05}.gguf",
            self.repo,
            self.revision,
            split_id.as_str(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelArtifactRefError {
    InvalidScheme,
    MissingRepo,
    MissingRevision,
    RevisionMustBePathSegment,
    MissingPath,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitScheme {
    GgufLayerContiguousV1,
}

impl SplitScheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GgufLayerContiguousV1 => "gguf-layer-contiguous-v1",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SplitId(String);

impl SplitId {
    pub fn derive(model_ref: &ModelArtifactRef, scheme: SplitScheme) -> Self {
        Self(format!(
            "split-{}",
            stable_digest_hex(&["split", model_ref.as_str(), scheme.as_str()])
        ))
    }

    pub fn literal(value: impl Into<String>) -> Result<Self, SplitIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SplitIdError::Empty);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(SplitIdError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitIdError {
    Empty,
    InvalidCharacter,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelDigest(String);

impl ModelDigest {
    pub fn literal(value: impl Into<String>) -> Result<Self, ModelDigestError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ModelDigestError::Empty);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelDigestError {
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash(String);

impl ContentHash {
    pub fn literal(value: impl Into<String>) -> Result<Self, ContentHashError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ContentHashError::Empty);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentHashError {
    Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LayerRange {
    pub start: u32,
    pub end_exclusive: u32,
}

impl LayerRange {
    pub fn new(start: u32, end_exclusive: u32) -> Result<Self, LayerRangeError> {
        if start >= end_exclusive {
            return Err(LayerRangeError::EmptyOrInverted);
        }
        Ok(Self {
            start,
            end_exclusive,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayerRangeError {
    EmptyOrInverted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardAssignment {
    pub model_ref: ModelArtifactRef,
    pub split_id: SplitId,
    pub split_scheme: SplitScheme,
    pub stage_index: u32,
    pub stage_count: u32,
    pub layer_range: LayerRange,
}

impl ShardAssignment {
    pub fn new(
        model_ref: ModelArtifactRef,
        split_id: SplitId,
        split_scheme: SplitScheme,
        stage_index: u32,
        stage_count: u32,
        layer_range: LayerRange,
    ) -> Result<Self, ShardAssignmentError> {
        if stage_count == 0 {
            return Err(ShardAssignmentError::EmptyStageCount);
        }
        if stage_index >= stage_count {
            return Err(ShardAssignmentError::StageIndexOutOfRange);
        }
        Ok(Self {
            model_ref,
            split_id,
            split_scheme,
            stage_index,
            stage_count,
            layer_range,
        })
    }

    pub fn expected_model_digest(&self) -> ModelDigest {
        self.model_ref.model_digest()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardAssignmentError {
    EmptyStageCount,
    StageIndexOutOfRange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardManifest {
    pub model_digest: ModelDigest,
    pub split_id: SplitId,
    pub stage_index: u32,
    pub stage_count: u32,
    pub layer_range: LayerRange,
    pub content_hash: ContentHash,
}

impl ShardManifest {
    pub fn for_assignment(assignment: &ShardAssignment, content_hash: ContentHash) -> Self {
        Self {
            model_digest: assignment.expected_model_digest(),
            split_id: assignment.split_id.clone(),
            stage_index: assignment.stage_index,
            stage_count: assignment.stage_count,
            layer_range: assignment.layer_range,
            content_hash,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedShard {
    pub assignment: ShardAssignment,
    pub manifest: ShardManifest,
    pub local_path: String,
}

impl ValidatedShard {
    pub fn new(
        assignment: ShardAssignment,
        manifest: ShardManifest,
        local_path: impl Into<String>,
    ) -> Result<Self, ShardValidationError> {
        ShardValidator::validate(&assignment, &manifest)?;
        Ok(Self {
            assignment,
            manifest,
            local_path: local_path.into(),
        })
    }
}

pub struct ShardValidator;

impl ShardValidator {
    pub fn validate(
        assignment: &ShardAssignment,
        manifest: &ShardManifest,
    ) -> Result<(), ShardValidationError> {
        if manifest.model_digest != assignment.expected_model_digest() {
            return Err(ShardValidationError::ModelDigestMismatch);
        }
        if manifest.split_id != assignment.split_id {
            return Err(ShardValidationError::SplitIdMismatch);
        }
        if manifest.stage_index != assignment.stage_index {
            return Err(ShardValidationError::StageIndexMismatch);
        }
        if manifest.stage_count != assignment.stage_count {
            return Err(ShardValidationError::StageCountMismatch);
        }
        if manifest.layer_range != assignment.layer_range {
            return Err(ShardValidationError::LayerRangeMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardValidationError {
    ModelDigestMismatch,
    SplitIdMismatch,
    StageIndexMismatch,
    StageCountMismatch,
    LayerRangeMismatch,
}

fn stable_digest_hex(parts: &[&str]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

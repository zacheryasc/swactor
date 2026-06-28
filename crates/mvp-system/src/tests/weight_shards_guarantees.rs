use mvp_system::weight_shards as shards;

fn model_ref() -> shards::ModelArtifactRef {
    shards::ModelArtifactRef::parse("hf://org/repo@abcdef123456/model.gguf").unwrap()
}

fn assignment() -> shards::ShardAssignment {
    let model_ref = model_ref();
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

fn content_hash() -> shards::ContentHash {
    shards::ContentHash::literal("sha256:test-content").unwrap()
}

#[test]
fn model_ref_canonicalization_is_stable() {
    let parsed = shards::ModelArtifactRef::parse("hf://org/repo@abcdef123456/model.gguf").unwrap();
    let from_parts =
        shards::ModelArtifactRef::hugging_face("/org/repo/", "abcdef123456", "/model.gguf")
            .unwrap();

    assert_eq!(parsed, from_parts);
    assert_eq!(parsed.as_str(), "hf://org/repo@abcdef123456/model.gguf");
    assert_eq!(parsed.repo(), "org/repo");
    assert_eq!(parsed.revision(), "abcdef123456");
    assert_eq!(parsed.path(), "model.gguf");
}

#[test]
fn split_id_is_deterministic_and_model_sensitive() {
    let first = model_ref();
    let same = shards::ModelArtifactRef::parse("hf://org/repo@abcdef123456/model.gguf").unwrap();
    let different =
        shards::ModelArtifactRef::parse("hf://org/repo@fedcba654321/model.gguf").unwrap();
    let scheme = shards::SplitScheme::GgufLayerContiguousV1;

    assert_eq!(
        shards::SplitId::derive(&first, scheme),
        shards::SplitId::derive(&same, scheme)
    );
    assert_ne!(
        shards::SplitId::derive(&first, scheme),
        shards::SplitId::derive(&different, scheme)
    );
}

#[test]
fn assignment_rejects_invalid_stage_shape_and_ranges() {
    let model_ref = model_ref();
    let scheme = shards::SplitScheme::GgufLayerContiguousV1;
    let split_id = shards::SplitId::derive(&model_ref, scheme);
    let range = shards::LayerRange::new(1, 2).unwrap();

    assert_eq!(
        shards::ShardAssignment::new(model_ref.clone(), split_id.clone(), scheme, 0, 0, range),
        Err(shards::ShardAssignmentError::EmptyStageCount)
    );
    assert_eq!(
        shards::ShardAssignment::new(model_ref, split_id, scheme, 2, 2, range),
        Err(shards::ShardAssignmentError::StageIndexOutOfRange)
    );
    assert_eq!(
        shards::LayerRange::new(4, 4),
        Err(shards::LayerRangeError::EmptyOrInverted)
    );
    assert_eq!(
        shards::LayerRange::new(5, 4),
        Err(shards::LayerRangeError::EmptyOrInverted)
    );
}

#[test]
fn validator_accepts_matching_manifest() {
    let assignment = assignment();
    let manifest = shards::ShardManifest::for_assignment(&assignment, content_hash());

    assert_eq!(
        shards::ShardValidator::validate(&assignment, &manifest),
        Ok(())
    );
    assert!(shards::ValidatedShard::new(assignment, manifest, "/cache/stage-00003.gguf").is_ok());
}

#[test]
fn validator_rejects_mismatched_manifest() {
    let assignment = assignment();
    let matching = shards::ShardManifest::for_assignment(&assignment, content_hash());

    let mut wrong_model = matching.clone();
    wrong_model.model_digest = shards::ModelDigest::literal("wrong-model").unwrap();
    assert_eq!(
        shards::ShardValidator::validate(&assignment, &wrong_model),
        Err(shards::ShardValidationError::ModelDigestMismatch)
    );

    let mut wrong_split = matching.clone();
    wrong_split.split_id = shards::SplitId::literal("split-wrong").unwrap();
    assert_eq!(
        shards::ShardValidator::validate(&assignment, &wrong_split),
        Err(shards::ShardValidationError::SplitIdMismatch)
    );

    let mut wrong_stage = matching.clone();
    wrong_stage.stage_index = 4;
    assert_eq!(
        shards::ShardValidator::validate(&assignment, &wrong_stage),
        Err(shards::ShardValidationError::StageIndexMismatch)
    );

    let mut wrong_count = matching.clone();
    wrong_count.stage_count = 9;
    assert_eq!(
        shards::ShardValidator::validate(&assignment, &wrong_count),
        Err(shards::ShardValidationError::StageCountMismatch)
    );

    let mut wrong_range = matching;
    wrong_range.layer_range = shards::LayerRange::new(16, 20).unwrap();
    assert_eq!(
        shards::ShardValidator::validate(&assignment, &wrong_range),
        Err(shards::ShardValidationError::LayerRangeMismatch)
    );
}

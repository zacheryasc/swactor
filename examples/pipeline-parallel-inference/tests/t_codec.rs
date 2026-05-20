//! T-codec: pipeline-parallel message serialization tests (TEST_SPEC §1).
//!
//! Names match TEST_SPEC verbatim. Roundtrips use the direct codec path and
//! the type-erased `CodecRegistry` path. Negative variants assert `Err` —
//! no `#[should_panic]`, per plan rules.

use pipeline_parallel_inference::messages::{
    inference_codec_registry, InferenceCodec, InferenceRequest, InferenceResponse, NextToken,
    StageActivation,
};
use swactor::actor::ActorAddress;
use swactor::transport::Codec;

fn request_codec() -> &'static dyn Codec<InferenceRequest> {
    &InferenceCodec
}
fn response_codec() -> &'static dyn Codec<InferenceResponse> {
    &InferenceCodec
}
fn activation_codec() -> &'static dyn Codec<StageActivation> {
    &InferenceCodec
}
fn next_token_codec() -> &'static dyn Codec<NextToken> {
    &InferenceCodec
}

/// A non-trivial bf16-shaped payload. 8 positions × 16 lanes × 2 bytes = 256
/// bytes of varied content. Not all-zeros, not monotonic, includes high
/// bytes — survives JSON-array roundtrip and gives byte-flip / truncation
/// probes something real to corrupt.
fn sample_hidden(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            // Mix a few bits so flipped/truncated copies differ from the original
            // in ways the decoder must catch.
            ((i as u32).wrapping_mul(2654435761) >> 8) as u8
        })
        .collect()
}

fn sample_activation() -> StageActivation {
    StageActivation {
        request_id: 0xDEAD_BEEF_CAFE_F00D,
        position: 7,
        hidden: sample_hidden(256),
        seq_len: 8,
        is_prefill: false,
    }
}

// ─── Roundtrip Tests (TEST_SPEC §1) ───────────────────────────────────────

/// Prefill (`seq_len` == prompt length) and decode (`seq_len == 1`) both
/// flow through the same codec. This sweep proves the codec is shape-
/// agnostic — varying `seq_len` does not change roundtrip identity, so a
/// chain at any `N` can carry prefill or decode hops on the same wire.
#[test]
fn stage_activation_roundtrips_with_varied_seq_len() {
    let codec = activation_codec();
    let registry = inference_codec_registry();
    let hidden_dim_bytes = 32; // arbitrary; the codec is shape-agnostic.

    for &seq_len in &[1u32, 2, 32, 256] {
        let len = (seq_len as usize) * hidden_dim_bytes;
        let original = StageActivation {
            request_id: 0xABCD_0000 ^ u64::from(seq_len),
            position: seq_len.saturating_mul(7),
            hidden: sample_hidden(len),
            seq_len,
            is_prefill: seq_len > 1,
        };
        assert_eq!(original.hidden.len(), len, "sanity: payload size matches seq_len");

        let bytes = codec.encode(&original).expect("encode should succeed");
        let decoded = codec.decode(&bytes).expect("decode should succeed");
        assert_eq!(decoded, original, "direct roundtrip differs at seq_len={seq_len}");
        assert_eq!(
            decoded.hidden, original.hidden,
            "binary payload must survive at seq_len={seq_len}",
        );

        let (tag, payload) = registry
            .encode(
                std::any::TypeId::of::<StageActivation>(),
                Box::new(original.clone()),
            )
            .expect("registry encode should succeed");
        assert_eq!(tag, "pp::StageActivation");
        let any_msg = registry
            .decode(&tag, &payload)
            .expect("registry decode should succeed");
        let decoded = any_msg
            .downcast::<StageActivation>()
            .expect("downcast should succeed");
        assert_eq!(*decoded, original, "registry roundtrip differs at seq_len={seq_len}");
    }
}

#[test]
fn stage_activation_roundtrips_through_codec_and_registry() {
    let original = sample_activation();
    assert!(
        original.hidden.iter().any(|&b| b != 0),
        "test payload must be non-trivial"
    );

    // Direct codec path
    let codec = activation_codec();
    let bytes = codec.encode(&original).expect("encode should succeed");
    let decoded = codec.decode(&bytes).expect("decode should succeed");
    assert_eq!(decoded, original);
    assert_eq!(decoded.hidden, original.hidden, "binary payload must survive");

    // Registry (type-erased) path
    let registry = inference_codec_registry();
    let (tag, payload) = registry
        .encode(
            std::any::TypeId::of::<StageActivation>(),
            Box::new(original.clone()),
        )
        .expect("registry encode should succeed");
    assert_eq!(tag, "pp::StageActivation");

    let any_msg = registry
        .decode(&tag, &payload)
        .expect("registry decode should succeed");
    let decoded = any_msg
        .downcast::<StageActivation>()
        .expect("downcast should succeed");
    assert_eq!(*decoded, original);
}

#[test]
fn next_token_roundtrips_through_codec_and_registry() {
    // Run both `done` polarities so the flag itself is exercised.
    for done in [false, true] {
        let original = NextToken {
            request_id: 42,
            token_id: 128_001,
            position: 23,
            done,
        };

        let codec = next_token_codec();
        let bytes = codec.encode(&original).expect("encode should succeed");
        let decoded = codec.decode(&bytes).expect("decode should succeed");
        assert_eq!(decoded, original);
        assert_eq!(decoded.done, done);

        let registry = inference_codec_registry();
        let (tag, payload) = registry
            .encode(
                std::any::TypeId::of::<NextToken>(),
                Box::new(original.clone()),
            )
            .expect("registry encode should succeed");
        assert_eq!(tag, "pp::NextToken");

        let any_msg = registry
            .decode(&tag, &payload)
            .expect("registry decode should succeed");
        let decoded = any_msg
            .downcast::<NextToken>()
            .expect("downcast should succeed");
        assert_eq!(*decoded, original);
    }
}

#[test]
fn inference_request_roundtrips_with_max_tokens_field() {
    let original = InferenceRequest {
        reply_to: ActorAddress::new_random(),
        prompt: "Say hello".into(),
        max_tokens: 64,
    };

    let codec = request_codec();
    let bytes = codec.encode(&original).expect("encode should succeed");
    let decoded = codec.decode(&bytes).expect("decode should succeed");
    assert_eq!(decoded, original);
    assert_eq!(decoded.max_tokens, 64, "max_tokens must survive roundtrip");

    // Registry path — confirms the type tag is what the rest of the system
    // sees on the wire.
    let registry = inference_codec_registry();
    let (tag, payload) = registry
        .encode(
            std::any::TypeId::of::<InferenceRequest>(),
            Box::new(original.clone()),
        )
        .expect("registry encode should succeed");
    assert_eq!(tag, "pp::InferenceRequest");

    let any_msg = registry
        .decode(&tag, &payload)
        .expect("registry decode should succeed");
    let decoded = any_msg
        .downcast::<InferenceRequest>()
        .expect("downcast should succeed");
    assert_eq!(*decoded, original);
}

#[test]
fn inference_response_roundtrips_through_codec_and_registry() {
    let original = InferenceResponse {
        text: "Hello! I'd be happy to help.".into(),
    };

    let codec = response_codec();
    let bytes = codec.encode(&original).expect("encode should succeed");
    let decoded = codec.decode(&bytes).expect("decode should succeed");
    assert_eq!(decoded, original);

    let registry = inference_codec_registry();
    let (tag, payload) = registry
        .encode(
            std::any::TypeId::of::<InferenceResponse>(),
            Box::new(original.clone()),
        )
        .expect("registry encode should succeed");
    assert_eq!(tag, "pp::InferenceResponse");

    let any_msg = registry
        .decode(&tag, &payload)
        .expect("registry decode should succeed");
    let decoded = any_msg
        .downcast::<InferenceResponse>()
        .expect("downcast should succeed");
    assert_eq!(*decoded, original);
}

// ─── Corruption / Negative Tests (TEST_SPEC §1) ───────────────────────────

/// A handful of byte flips at varied offsets in an encoded `StageActivation`
/// must all decode to `Err` — never panic, never silently succeed.
#[test]
fn corrupted_bytes_produce_error_for_stage_activation() {
    let codec = activation_codec();
    let original = sample_activation();
    let bytes = codec.encode(&original).unwrap();
    assert!(bytes.len() > 32, "test setup: encoded payload too small");

    // Deterministic offsets spread across the payload — the start (structural
    // JSON tokens), the middle (mostly inside the numeric `hidden` array),
    // and near the end (closing brace / final fields).
    let offsets = [
        0,
        1,
        3,
        bytes.len() / 4,
        bytes.len() / 2,
        (bytes.len() * 3) / 4,
        bytes.len() - 2,
        bytes.len() - 1,
    ];

    let mut err_count = 0;
    for &off in &offsets {
        let mut corrupted = bytes.clone();
        // 0xFF is never a valid JSON byte at any structural position and is
        // never valid UTF-8 as a lone byte inside a string — guarantees a
        // parser error whatever offset we land on.
        corrupted[off] = 0xFF;
        let result = codec.decode(&corrupted);
        assert!(
            result.is_err(),
            "byte flip at offset {off} should produce Err, got {result:?}"
        );
        err_count += 1;
    }
    assert_eq!(err_count, offsets.len());
}

/// Cutting the encoded payload in half lands inside the `hidden` array — the
/// JSON is no longer well-formed and decode must reject it.
#[test]
fn truncated_stage_activation_produces_error() {
    let codec = activation_codec();
    let original = sample_activation();
    let bytes = codec.encode(&original).unwrap();

    // Sweep truncation points so a future encoding change can't accidentally
    // land on a happy boundary. Every truncation that is not the full
    // payload must be rejected.
    let cut_points = [
        0,
        1,
        bytes.len() / 8,
        bytes.len() / 4,
        bytes.len() / 2,
        bytes.len() - 1,
    ];
    for &cut in &cut_points {
        let truncated = &bytes[..cut];
        let result = codec.decode(truncated);
        assert!(
            result.is_err(),
            "truncation to {cut} of {} bytes should produce Err",
            bytes.len()
        );
    }
}

/// Decoding bytes of one type with the codec of another must error — neither
/// the direct path nor the registry path should silently produce nonsense.
#[test]
fn wrong_message_type_produces_error() {
    let request = InferenceRequest {
        reply_to: ActorAddress::new_random(),
        prompt: "hi".into(),
        max_tokens: 8,
    };
    let response = InferenceResponse { text: "ok".into() };
    let next = NextToken {
        request_id: 1,
        token_id: 5,
        position: 0,
        done: false,
    };
    let activation = sample_activation();

    let req_bytes = request_codec().encode(&request).unwrap();
    let res_bytes = response_codec().encode(&response).unwrap();
    let next_bytes = next_token_codec().encode(&next).unwrap();
    let act_bytes = activation_codec().encode(&activation).unwrap();

    // Each pair (encoded as A, decoded as B) must fail.
    assert!(activation_codec().decode(&req_bytes).is_err());
    assert!(activation_codec().decode(&res_bytes).is_err());
    assert!(activation_codec().decode(&next_bytes).is_err());
    assert!(next_token_codec().decode(&act_bytes).is_err());
    assert!(next_token_codec().decode(&req_bytes).is_err());
    assert!(request_codec().decode(&res_bytes).is_err());
    assert!(request_codec().decode(&next_bytes).is_err());
    assert!(request_codec().decode(&act_bytes).is_err());

    // Registry-level guard: a payload encoded under one tag, decoded under
    // another, must error rather than producing a wrong-type value.
    let registry = inference_codec_registry();
    let (_tag, response_payload) = registry
        .encode(
            std::any::TypeId::of::<InferenceResponse>(),
            Box::new(response.clone()),
        )
        .unwrap();
    let result = registry.decode("pp::InferenceRequest", &response_payload);
    assert!(
        result.is_err(),
        "registry must reject cross-decoded payload, got Ok"
    );
}

/// Zero bytes is not a valid encoding for any pipeline message type.
#[test]
fn empty_bytes_produce_error() {
    assert!(request_codec().decode(&[]).is_err());
    assert!(response_codec().decode(&[]).is_err());
    assert!(activation_codec().decode(&[]).is_err());
    assert!(next_token_codec().decode(&[]).is_err());

    // Registry path too — empty payload under any registered tag.
    let registry = inference_codec_registry();
    for tag in [
        "pp::InferenceRequest",
        "pp::InferenceResponse",
        "pp::StageActivation",
        "pp::NextToken",
    ] {
        assert!(
            registry.decode(tag, &[]).is_err(),
            "registry must reject empty payload under tag {tag}"
        );
    }
}

/// Every `NetworkMessage` the pipeline defines must be reachable from the
/// type-erased registry: encode under its `TypeId`, decode under its
/// `type_tag`, downcast back to the original concrete type. Catches the
/// "forgot to register one of the four" regression.
#[test]
fn codec_registry_dispatches_all_four_types() {
    let registry = inference_codec_registry();

    let request = InferenceRequest {
        reply_to: ActorAddress::new_random(),
        prompt: "dispatch me".into(),
        max_tokens: 16,
    };
    let response = InferenceResponse { text: "dispatched".into() };
    let activation = sample_activation();
    let next_token = NextToken {
        request_id: 11,
        token_id: 22,
        position: 33,
        done: true,
    };

    // (tag, encode under TypeId<T>, then decode + downcast back to T).
    let (req_tag, req_bytes) = registry
        .encode(
            std::any::TypeId::of::<InferenceRequest>(),
            Box::new(request.clone()),
        )
        .expect("InferenceRequest must be registered");
    assert_eq!(req_tag, "pp::InferenceRequest");
    let req_any = registry.decode(&req_tag, &req_bytes).unwrap();
    let req_decoded = *req_any.downcast::<InferenceRequest>().unwrap();
    assert_eq!(req_decoded, request);

    let (resp_tag, resp_bytes) = registry
        .encode(
            std::any::TypeId::of::<InferenceResponse>(),
            Box::new(response.clone()),
        )
        .expect("InferenceResponse must be registered");
    assert_eq!(resp_tag, "pp::InferenceResponse");
    let resp_any = registry.decode(&resp_tag, &resp_bytes).unwrap();
    let resp_decoded = *resp_any.downcast::<InferenceResponse>().unwrap();
    assert_eq!(resp_decoded, response);

    let (act_tag, act_bytes) = registry
        .encode(
            std::any::TypeId::of::<StageActivation>(),
            Box::new(activation.clone()),
        )
        .expect("StageActivation must be registered");
    assert_eq!(act_tag, "pp::StageActivation");
    let act_any = registry.decode(&act_tag, &act_bytes).unwrap();
    let act_decoded = *act_any.downcast::<StageActivation>().unwrap();
    assert_eq!(act_decoded, activation);

    let (nt_tag, nt_bytes) = registry
        .encode(
            std::any::TypeId::of::<NextToken>(),
            Box::new(next_token.clone()),
        )
        .expect("NextToken must be registered");
    assert_eq!(nt_tag, "pp::NextToken");
    let nt_any = registry.decode(&nt_tag, &nt_bytes).unwrap();
    let nt_decoded = *nt_any.downcast::<NextToken>().unwrap();
    assert_eq!(nt_decoded, next_token);
}

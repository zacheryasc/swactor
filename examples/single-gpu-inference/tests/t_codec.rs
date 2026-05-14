//! T-codec: InferenceRequest / InferenceResponse serialization tests.
//!
//! Validates the message contract that all other smoke test groups depend on:
//! - Roundtrip fidelity for both message types
//! - Graceful error handling on corrupted input

use single_gpu_inference::messages::{
    inference_codec_registry, InferenceCodec, InferenceRequest, InferenceResponse,
};
use swactor::actor::ActorAddress;
use swactor::transport::Codec;

fn request_codec() -> &'static dyn Codec<InferenceRequest> {
    &InferenceCodec
}

fn response_codec() -> &'static dyn Codec<InferenceResponse> {
    &InferenceCodec
}

// ─── Roundtrip Tests ───────────────────────────────────────────────────────

/// A request with representative field values survives encode → decode
/// through both the direct codec and the type-erased registry path.
#[test]
fn inference_request_roundtrips_through_codec_and_registry() {
    let original = InferenceRequest {
        prompt: "Tell me about distributed systems".into(),
        max_tokens: 128,
        temperature: 0.7,
        reply_to: ActorAddress::new_random(),
    };

    // Direct codec path
    let codec = request_codec();
    let bytes = codec.encode(&original).expect("encode should succeed");
    let decoded = codec.decode(&bytes).expect("decode should succeed");
    assert_eq!(decoded, original);

    // Registry (type-erased) path — encode via TypeId, decode via type_tag
    let registry = inference_codec_registry();
    let (tag, payload) = registry
        .encode(
            std::any::TypeId::of::<InferenceRequest>(),
            Box::new(original.clone()),
        )
        .expect("registry encode should succeed");
    assert_eq!(tag, "smoke::InferenceRequest");

    let any_msg = registry
        .decode(&tag, &payload)
        .expect("registry decode should succeed");
    let decoded = any_msg
        .downcast::<InferenceRequest>()
        .expect("downcast should succeed");
    assert_eq!(*decoded, original);
}

/// A response roundtrips through both the direct codec and the registry.
#[test]
fn inference_response_roundtrips_through_codec_and_registry() {
    let original = InferenceResponse {
        text: "Hello! I'd be happy to discuss distributed systems.".into(),
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
    assert_eq!(tag, "smoke::InferenceResponse");

    let any_msg = registry
        .decode(&tag, &payload)
        .expect("registry decode should succeed");
    let decoded = any_msg
        .downcast::<InferenceResponse>()
        .expect("downcast should succeed");
    assert_eq!(*decoded, original);
}

// ─── Corruption Tests ──────────────────────────────────────────────────────

/// Completely random bytes are not valid JSON — the decoder must return
/// an error rather than panicking or producing a garbage message.
#[test]
fn corrupted_bytes_produce_error_for_request() {
    let codec = request_codec();
    let garbage: Vec<u8> = vec![0xFF, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];
    let result = codec.decode(&garbage);
    assert!(result.is_err(), "garbage bytes must produce an error");
}

#[test]
fn corrupted_bytes_produce_error_for_response() {
    let codec = response_codec();
    let garbage: Vec<u8> = vec![0xFF, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];
    let result = codec.decode(&garbage);
    assert!(result.is_err(), "garbage bytes must produce an error");
}

/// Truncated payload — valid JSON prefix cut short mid-value.
#[test]
fn truncated_request_bytes_produce_error() {
    let codec = request_codec();
    let original = InferenceRequest {
        prompt: "hello".into(),
        max_tokens: 64,
        temperature: 0.5,
        reply_to: ActorAddress::new_random(),
    };
    let bytes = codec.encode(&original).unwrap();
    // Chop off the last half
    let truncated = &bytes[..bytes.len() / 2];
    let result = codec.decode(truncated);
    assert!(result.is_err(), "truncated bytes must produce an error");
}

/// Empty input — zero bytes is not valid JSON.
#[test]
fn empty_bytes_produce_error() {
    let req_codec = request_codec();
    let res_codec = response_codec();
    assert!(req_codec.decode(&[]).is_err());
    assert!(res_codec.decode(&[]).is_err());
}

/// Valid JSON but wrong schema — a response payload fed to the request
/// decoder. The missing required fields must cause an error.
#[test]
fn wrong_message_type_produces_error() {
    let res_codec = response_codec();
    let req_codec = request_codec();

    let response = InferenceResponse {
        text: "oops".into(),
    };
    let response_bytes = res_codec.encode(&response).unwrap();

    // Decoding response bytes as a request should fail (missing prompt, max_tokens, etc.)
    let result = req_codec.decode(&response_bytes);
    assert!(
        result.is_err(),
        "decoding response bytes as request must fail"
    );
}

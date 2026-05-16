# Test Spec: Pipeline-Parallel Inference, Two Nodes

This spec enumerates every component surface in `SPEC.md` and names the behavioral test that proves it correct. No implementations here — just the name and a one-line statement of what passing the test proves.

## Test Philosophy

- **Fail fast, cheap first.** Pure-Rust unit tests run on every save. Local multi-process integration runs on every commit. Anything touching real vast.ai is gated and rare.
- **Behavior over structure.** Tests describe observable outcomes (a response arrives, a token id is valid, a cluster converges). They do not assert internal call sequences or echo back the implementation.
- **One killer correctness test.** The "non-empty response" bar is too low for sliced inference — silently wrong outputs would pass. Equivalence against single-node inference (§9) is the load-bearing test.

## Test Tiers

| Tier | What runs | When | Approx wall-clock |
|---|---|---|---|
| 1. Pure unit (Rust) | codec, layer-range math, message types | `cargo test` | < 1 s |
| 2. Python worker contract | stdin/stdout protocol, stub mode | `pytest` | a few seconds |
| 3. Stage actor (Rust, in-process) | one stage actor + its worker subprocess | `cargo test` | seconds |
| 4. Local cluster integration | 3 swactor nodes in one process, real iroh, stub worker | `cargo test` | seconds |
| 5. Local cluster + real tinygrad | same as 4, but real GGUF on CPU | `cargo test -- --ignored` | minutes |
| 6. Binary E2E (localhost) | full `pp-smoke-run --seed` driving real `pp-gpu-node` processes | `cargo test -- --ignored` | minutes |
| 7. vast.ai REST (mocked) | 2-instance create/destroy with mocked HTTP | `cargo test` | < 1 s |
| 8. vast.ai full run | actual smoke run on rented hardware | manual only | ~10 min |

---

## 1. Message Codec — `tests/t_codec.rs`

Surface: `InferenceRequest`, `InferenceResponse`, `StageActivation`, `NextToken` defined in §3 of SPEC.

| Test name | Proves |
|---|---|
| `stage_activation_roundtrips_through_codec_and_registry` | `StageActivation` with non-trivial binary `hidden` payload encodes and decodes to identity. |
| `next_token_roundtrips_through_codec_and_registry` | `NextToken` encodes and decodes to identity including `done` flag. |
| `inference_request_roundtrips_with_max_tokens_field` | New `max_tokens` field on `InferenceRequest` roundtrips (forked from single-GPU). |
| `inference_response_roundtrips_through_codec_and_registry` | Same as single-GPU, kept for parity. |
| `corrupted_bytes_produce_error_for_stage_activation` | Random byte flips in encoded `StageActivation` decode to `Err`, never panic. |
| `truncated_stage_activation_produces_error` | Truncating the `hidden` blob is rejected with `Err`. |
| `wrong_message_type_produces_error` | Decoding bytes of one type with the codec of another returns `Err`. |
| `empty_bytes_produce_error` | Empty input decodes to `Err` for every message type. |

Reused-as-is codec tests from `single-gpu-inference/tests/t_codec.rs` are not duplicated.

---

## 2. Layer-Range and Sampling Math — `tests/test_worker.py::TestLayerMath`

Surface: the layer-slice computation and argmax sampling described in §4 of SPEC.

| Test name | Proves |
|---|---|
| `test_stage_0_layer_range_is_lower_half` | `STAGE=0 NUM_STAGES=2` yields `(0, total // 2)`. |
| `test_stage_1_layer_range_covers_remainder` | `STAGE=1 NUM_STAGES=2` yields `(total // 2, total)`, including any odd block. |
| `test_layer_range_partition_is_total_coverage` | The union of all stage ranges equals `[0, total)` with no gaps or overlap, for `NUM_STAGES ∈ {2, 3, 4}`. (Generalizes early, costs nothing.) |
| `test_argmax_sampling_is_deterministic` | Same logits tensor → same `token_id`, repeated calls. |

---

## 3. Python Worker Contract — `tests/test_worker.py`

Surface: `pp_tinygrad_worker.py` stdin/stdout protocol described in §4 of SPEC. Run against a `--stub` mode that skips real GGUF load.

### TestWorkerStartup

| Test name | Proves |
|---|---|
| `test_worker_emits_ready_with_pid_and_stage` | Startup writes `{"status": "ready", "pid": <int>, "stage": <int>}` to stdout. |
| `test_worker_rejects_invalid_stage_env` | `STAGE` outside `[0, NUM_STAGES)` causes immediate exit with non-zero code. |

### TestStage0Operations (stub mode)

| Test name | Proves |
|---|---|
| `test_embed_and_forward_returns_hidden_for_prompt` | `op=embed_and_forward` with a tokenized prompt returns a base64 `hidden` payload whose decoded byte length matches `seq_len * hidden_dim * 2` (bf16). |
| `test_decode_step_returns_hidden_for_single_token` | `op=decode_step` with one token returns hidden bytes of length `1 * hidden_dim * 2`. |
| `test_kv_cache_grows_across_successive_decode_steps` | Successive `decode_step` calls at increasing `position` succeed; the worker does not error or reset state between them. |
| `test_stage_0_rejects_stage_1_ops` | Sending `op=forward_and_sample` to a stage-0 worker returns `{"error": ...}`, worker stays alive. |

### TestStage1Operations (stub mode)

| Test name | Proves |
|---|---|
| `test_forward_and_sample_returns_valid_token_id` | `op=forward_and_sample` returns a `token_id` in `[0, vocab_size)`. |
| `test_forward_and_sample_is_deterministic_for_same_input` | Same `hidden` + `position` → same `token_id`. |
| `test_stage_1_rejects_stage_0_ops` | Sending `op=embed_and_forward` to a stage-1 worker returns `{"error": ...}`, worker stays alive. |

### TestWorkerMalformedInput

| Test name | Proves |
|---|---|
| `test_malformed_json_returns_error_and_continues` | One garbage line yields an `{"error": ...}` reply and the worker accepts a valid request immediately after. |
| `test_missing_op_field_returns_error` | Valid JSON without `op` returns `{"error": ...}`. |
| `test_oversized_hidden_payload_returns_error` | `hidden` whose decoded length disagrees with declared `seq_len` is rejected. |

### TestWorkerEOFShutdown

| Test name | Proves |
|---|---|
| `test_eof_causes_clean_exit` | Closing stdin makes the worker exit 0 within a few seconds. |

---

## 4. Stage Actor — `tests/t_actor.rs`

Surface: the two new actor types (`Stage0Actor`, `Stage1Actor`) and their `ProcessBridge` to the Python worker. Uses the stub worker.

### Stage0Actor

| Test name | Proves |
|---|---|
| `stage_0_actor_spawns_worker_and_reports_ready` | After spawn, the actor emits `WorkerReady { pid }` once the stub worker prints its ready line. |
| `inference_request_produces_stage_activation_to_next_address` | Sending `InferenceRequest` causes the actor to emit a `StageActivation` to its configured next-stage address, with `is_prefill=true` and `seq_len == prompt_token_count`. |
| `next_token_produces_stage_activation_for_decode_step` | Sending `NextToken { done: false }` causes the actor to emit `StageActivation` with `is_prefill=false`, `seq_len=1`. |
| `next_token_with_done_produces_no_further_activations` | After `NextToken { done: true }`, the actor emits nothing more for that request. |
| `stage_0_worker_crash_is_handled_without_poisoning_runtime` | If the worker process dies, the actor reports the failure and the runtime continues serving other actors. |
| `stopping_stage_0_actor_kills_child_worker` | Dropping/stopping the actor cleans up the worker pid. |

### Stage1Actor

| Test name | Proves |
|---|---|
| `stage_1_actor_spawns_worker_and_reports_ready` | Mirror of stage 0. |
| `stage_activation_produces_next_token_to_prev_address` | Receiving `StageActivation` causes the actor to emit `NextToken` to its configured prev-stage address. |
| `stage_1_accumulates_tokens_and_emits_inference_response_on_eos` | After enough `StageActivation`s to produce the configured EOS token (stub-controlled), the actor emits `InferenceResponse { text }` with detokenized output. |
| `stage_1_emits_inference_response_when_max_tokens_reached` | After `max_tokens` activations, the actor emits `InferenceResponse` even without EOS. |
| `stage_1_worker_crash_is_handled_without_poisoning_runtime` | Mirror of stage 0. |
| `stopping_stage_1_actor_kills_child_worker` | Mirror of stage 0. |

---

## 5. Name Resolution and Topology — `tests/t_topology.rs`

Surface: SWIM name registration described in §3 of SPEC (`pp-entry`, `pp-exit`, `pp-stage-{i}`) and per-stage computation of next/prev neighbor names.

| Test name | Proves |
|---|---|
| `stage_0_registers_pp_entry_and_pp_stage_0` | Stage 0 boot registers both names; both resolve to the same `ActorAddress`. |
| `stage_1_registers_pp_exit_and_pp_stage_1` | Stage 1 boot registers both names; both resolve to the same `ActorAddress`. |
| `stage_resolves_next_neighbor_after_cluster_join` | Stage 0 resolves `pp-stage-1` to a non-empty address after the 3-node cluster converges. |
| `stage_resolves_prev_neighbor_after_cluster_join` | Stage 1 resolves `pp-stage-0` to a non-empty address after the 3-node cluster converges. |
| `next_stage_name_computed_from_env_alone` | A stage given `STAGE=0`, `NUM_STAGES=2` computes its outbound target name without any external config. |
| `last_stage_has_no_next_neighbor` | Stage `NUM_STAGES - 1` does not attempt to resolve a next neighbor. |

---

## 6. Cluster Transport — `tests/t_cluster.rs`

Surface: 3-node swactor cluster (orchestrator + 2 stages) over real iroh, single-process, separate `DistributedNode`s.

| Test name | Proves |
|---|---|
| `three_node_cluster_converges_via_iroh_seed_join` | All three nodes report each other alive within the SWIM convergence window. |
| `stage_activation_roundtrips_stage_0_to_stage_1` | A `StageActivation` sent stage-0 → stage-1 arrives intact (bytes + scalars). |
| `next_token_roundtrips_stage_1_to_stage_0` | A `NextToken` sent stage-1 → stage-0 arrives intact. |
| `inference_response_roundtrips_stage_1_to_orchestrator` | An `InferenceResponse` sent stage-1 → orchestrator arrives intact. |
| `node_death_detected_via_swim_after_stage_shutdown` | If a stage actor's host node shuts down, the other two mark it dead within the SWIM timeout. |

---

## 7. vast.ai Client Extensions — `tests/t_vastai.rs`

Surface: changes needed in the vast.ai client to rent and destroy two instances. Mocks HTTP, does not hit the real API.

| Test name | Proves |
|---|---|
| `create_two_instances_sends_distinct_stage_env_vars` | Two consecutive `create_instance` calls send `STAGE=0` and `STAGE=1` in the env payload respectively. |
| `create_two_instances_both_receive_same_seed_addr` | Both creation calls carry the same `SEED_ADDR`. |
| `failure_to_create_second_instance_triggers_destroy_of_first` | If the second `create_instance` errors, the orchestrator destroys the first instance before exiting. |
| `destroy_two_instances_sends_two_delete_requests` | Cleanup issues exactly two DELETEs, one per instance id. |
| `destroy_continues_when_one_delete_fails` | A failure to destroy one instance does not prevent the attempt to destroy the other. |

Reused-as-is single-instance vast.ai tests in `single-gpu-inference/tests/t_vastai.rs` are not duplicated.

---

## 8. Local Integration (in-process, stub worker) — `tests/t_integration.rs`

Surface: orchestrator + stage 0 + stage 1, each on a separate `DistributedNode` inside one test process, real iroh, stub worker. Drives the autoregressive loop end-to-end without real model compute.

| Test name | Proves |
|---|---|
| `distributed_pipeline_through_stub_workers_returns_response` | `pp-smoke-run`'s in-process equivalent sends `InferenceRequest { prompt: "Say hello", max_tokens: 4 }`, receives non-empty `InferenceResponse` from stage 1. |
| `decode_loop_terminates_on_stub_eos` | Stub worker configured to emit EOS at token N causes the response to arrive with exactly N tokens of accumulated text. |
| `decode_loop_terminates_on_max_tokens` | Stub worker that never emits EOS, with `max_tokens=4`, produces a response of length 4. |
| `stage_failure_mid_decode_surfaces_as_error_to_orchestrator` | Killing one stage's worker mid-loop causes the orchestrator's request future to resolve to an error within the SWIM detection window, not hang. |

---

## 9. Sliced-vs-Full Equivalence — `tests/t_integration.rs` (gated)

**The load-bearing correctness test.** Surface: that the 2-stage sliced forward pass produces the same outputs as a single-process full forward pass on the same prompt and model. Without this, every other test could pass with silently wrong inference.

| Test name | Proves |
|---|---|
| `sliced_two_stage_inference_matches_single_node_full_inference_for_say_hello` | Given the same `llama3.2:1b` GGUF, same `argmax` sampling, and the prompt `"Say hello"`, the token sequence produced by the 2-stage pipeline equals the token sequence produced by tinygrad's single-node `Transformer.generate()` up to `max_tokens=8`. |
| `sliced_two_stage_inference_matches_single_node_for_three_diverse_prompts` | Same equivalence holds for three short prompts of different shape (one word, one sentence, one with punctuation). Catches off-by-one errors in position handling, KV cache misalignment, and layer-boundary tensor shape mismatches. |

Marked `#[ignore]`. Runs on CPU with real GGUF; takes minutes.

---

## 10. Binary E2E (localhost) — `tests/t_binary.rs`

Surface: the actual `pp-smoke-run --seed` and `pp-gpu-node` binaries, spawned as child processes from the test, no vast.ai.

| Test name | Proves |
|---|---|
| `binary_e2e_two_stub_workers_returns_hello_response` | Running `pp-smoke-run --seed` against two `pp-gpu-node` child processes (`STAGE=0`/`STAGE=1`, stub worker) prints a non-empty response and exits 0. |
| `binary_e2e_two_stub_workers_cleans_up_on_failure` | If one `pp-gpu-node` is killed mid-run, `pp-smoke-run` exits non-zero and leaves no orphaned child processes. |
| `binary_e2e_two_tinygrad_workers_returns_response` | Same as the first but with the real tinygrad worker on CPU. `#[ignore]`. |

---

## 11. Manual Smoke Run — not a test, documented for completeness

Surface: actual rental on vast.ai. Not run by CI. Documented as a runbook command and an expected-output assertion.

| Procedure | Pass criterion |
|---|---|
| `VAST_API_KEY=... pp-smoke-run --vastai --gpu RTX_4090` | Exits 0 within 15 minutes; prints a non-empty response; both rented instances appear destroyed in the vast.ai dashboard. |

---

## Out of Scope (Tests We Are Not Writing)

- Performance / latency / throughput tests. Spec §2 declares no perf targets.
- Fault-tolerance recovery: no retry, no rescheduling, so no test for them.
- N > 2 stage configurations beyond the layer-range-math sanity check in §2. The actor and topology tests are 2-stage only.
- Streaming token tests. No streaming API in the MVP.
- Sampling correctness beyond argmax determinism. No temperature, top-p, etc.
- Concurrent in-flight requests. Single-request only.
- Tests of reused-as-is components from `single-gpu-inference` (iroh transport, swactor process bridge mechanics, single-instance vast.ai operations). They are tested in that example.

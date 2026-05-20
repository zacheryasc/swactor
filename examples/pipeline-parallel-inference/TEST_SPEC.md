# Test Spec: Pipeline-Parallel Inference, N Stages

> Companion to [`./SPEC.md`](./SPEC.md). This document enumerates
> every component surface in the N-stage spec and names the
> behavioural test that proves it correct. No implementations here —
> just the test name and a one-line statement of what passing the
> test proves.

## Test Philosophy

* **Fail fast, cheap first.** Pure-Rust unit tests run on every save.
  Local multi-process integration runs on every commit. Anything
  touching real vast.ai is gated and rare.
* **Behaviour over structure.** Tests describe observable outcomes
  (a response arrives, a token id is valid, a cluster converges).
  They do not assert internal call sequences or echo the
  implementation.
* **One killer correctness test.** "Non-empty response" is too low a
  bar for sliced inference — silently wrong outputs would pass.
  Equivalence against single-node inference (§12) is the load-bearing
  test.
* **Tier 9 is the pre-deploy gate.** The localised binary E2E at
  `N = 5` must pass before any vast.ai deploy. It is the only test
  that exercises the actual binaries against each other in their
  intended N-stage configuration; everything below it is either
  unit-shaped or replaces real bits with stubs.
* **Coverage scope.** Every component that runs as part of an N-stage
  deployment has at least one tier-1–4 test that runs without a
  cluster, plus a tier-5+ test that runs it in an integrated setting.
  We do not ship a component whose only proving ground is the
  deployed system.

## Test Tiers

| Tier | What runs | When | Wall-clock |
|---|---|---|---|
| 1. Pure unit (Rust) | codec, topology helpers, role computation, layer-range math | `cargo test` | < 1 s |
| 2. Python worker contract | stdin/stdout protocol, stub mode, all three op sets | `pytest` | seconds |
| 3. Stage actor (Rust, in-process) | one `StageActor` per role + its worker subprocess | `cargo test` | seconds |
| 4. Bridges (Rust, in-process) | `RequestBridge`, `NextTokenBridge`, `ActivationBridge` adapter behaviour | `cargo test` | < 1 s |
| 5. Cluster transport (Rust, in-process, real iroh) | N-node swactor cluster carrying every message type | `cargo test` | seconds |
| 6. Orchestrator setup (Rust, in-process) | spawn-chain address propagation, find-offer loop, rollback | `cargo test` | seconds |
| 7. vast.ai client (Rust, mocked HTTP) | N-instance create/destroy with mocked vast.ai | `cargo test` | < 1 s |
| 8. In-process integration (Rust, stub worker, real iroh) | full N-stage pipeline in one process for N ∈ {2,3,4,5} | `cargo test` | seconds |
| 9. **Localised binary E2E (`pp-smoke-run --seed`)** | actual binaries against each other at N ∈ {2,3,5} with stub workers | `cargo test -- --ignored` (gate) | minutes |
| 10. Sliced-vs-full equivalence (real tinygrad on CPU) | per-stage output matches single-node `Transformer.generate()` | `cargo test -- --ignored` | minutes |
| 11. Real-tinygrad binary E2E (localhost) | binaries with real GGUF worker on CPU | `cargo test -- --ignored` | minutes |
| 12. vast.ai full run | actual smoke run on rented hardware at N=5 | manual only | ~10 min |

---

## 1. Message Codec — `tests/t_codec.rs`

Surface: `InferenceRequest`, `InferenceResponse`, `StageActivation`,
`NextToken` defined in `messages.rs`. No semantic change from the
2-stage codec — the same codec carries activations across every
intermediate hop. Tests are listed in full rather than incrementally
because some 2-stage tests now imply more (every roundtrip is
exercised across `(N - 1)` hops at runtime).

| Test name | Proves |
|---|---|
| `stage_activation_roundtrips_through_codec_and_registry` | `StageActivation` with non-trivial binary `hidden` payload encodes and decodes to identity. |
| `stage_activation_roundtrips_with_varied_seq_len` | `StageActivation` with `seq_len ∈ {1, 2, 32, 256}` roundtrips intact — prefill and decode steps must both survive the wire. |
| `next_token_roundtrips_through_codec_and_registry` | `NextToken` encodes and decodes to identity including `done` flag. |
| `inference_request_roundtrips_with_max_tokens_field` | `InferenceRequest` including `reply_to` and `max_tokens` roundtrips. |
| `inference_response_roundtrips_through_codec_and_registry` | `InferenceResponse` with arbitrary UTF-8 text roundtrips. |
| `corrupted_bytes_produce_error_for_stage_activation` | Random byte flips in encoded `StageActivation` decode to `Err`, never panic. |
| `truncated_stage_activation_produces_error` | Truncating the `hidden` blob is rejected with `Err`. |
| `wrong_message_type_produces_error` | Decoding bytes of one type with the codec of another returns `Err`. |
| `empty_bytes_produce_error` | Empty input decodes to `Err` for every message type. |
| `codec_registry_dispatches_all_four_types` | All four `NetworkMessage` types are registered and decode correctly when dispatched by tag. |

---

## 2. Layer-Range and Sampling Math — `tests/test_worker.py::TestLayerMath`

Surface: the layer-slice computation and argmax sampling described in
SPEC §3.4 and §4. Generalised for arbitrary N.

| Test name | Proves |
|---|---|
| `test_stage_0_layer_range_starts_at_zero` | `STAGE=0` always yields `(0, …)` for any `NUM_STAGES`. |
| `test_last_stage_layer_range_reaches_total` | `STAGE=N-1` always yields `(…, total)` for any `NUM_STAGES`, including when `total` is not divisible by `N`. |
| `test_layer_range_partition_is_total_coverage` | The union of all stage ranges equals `[0, total)` with no gaps or overlap, for `NUM_STAGES ∈ {2, 3, 4, 5, 6, 7, 8}`. Property test. |
| `test_layer_range_each_stage_owns_at_least_one_block` | For any `NUM_STAGES ≤ total`, no stage's range is empty. |
| `test_layer_range_partition_for_indivisible_totals` | `total = 17, N = 4` produces 4 contiguous ranges that cover `[0, 17)` and differ by at most one block in length. |
| `test_layer_range_rejects_num_stages_one` | `NUM_STAGES = 1` is rejected by the worker at boot (the example does not serve `N = 1`). |
| `test_layer_range_rejects_num_stages_zero_or_oversize` | `NUM_STAGES = 0` and `STAGE >= NUM_STAGES` exit non-zero. |
| `test_argmax_sampling_is_deterministic` | Same logits tensor → same `token_id`, repeated calls. |

---

## 3. Role Computation — `tests/t_role.rs` (new)

Surface: the pure function `StageRole::for_stage(stage: u32, num_stages: u32) -> StageRole`
defined alongside `StageRole` in `stage_actor.rs`. Pure function, no
runtime needed.

| Test name | Proves |
|---|---|
| `stage_zero_is_first_for_any_num_stages` | `(0, N) → First` for `N ∈ {2..8}`. |
| `last_index_is_last_for_any_num_stages` | `(N-1, N) → Last` for `N ∈ {2..8}`. |
| `middle_indices_are_middle` | `(i, N) → Middle` for every `0 < i < N - 1`, `N ∈ {3..8}`. |
| `n_stages_two_has_no_middle` | At `N = 2`, only `First` and `Last` are produced; no input yields `Middle`. |
| `role_partition_property` | For every `N ∈ {2..16}`, exactly one stage is `First`, exactly one is `Last`, and the rest are `Middle`. |

---

## 4. Topology Helpers — `tests/t_topology.rs`

Surface: `topology.rs`. Helpers already parameterise on N; tests are
extended to assert that generalisation holds at N > 2 and that name
registration matches the role.

| Test name | Proves |
|---|---|
| `stage_name_uses_pp_prefix` | `stage_name(i) == "pp-stage-{i}"` for `i ∈ {0..16}`. |
| `first_stage_has_no_prev` | `prev_stage_name(0) == None`. |
| `last_stage_has_no_next` | `next_stage_name(N-1, N) == None` for `N ∈ {2..8}`. |
| `middle_stage_has_both_neighbours` | For `0 < i < N - 1`, `prev_stage_name(i)` and `next_stage_name(i, N)` are both `Some(_)` and differ. |
| `first_stage_registers_entry_and_index` | `register_stage_names(_, 0, N, addr)` registers `pp-entry` and `pp-stage-0`, both resolving to `addr`. |
| `last_stage_registers_exit_and_index` | `register_stage_names(_, N-1, N, addr)` registers `pp-exit` and `pp-stage-{N-1}`. |
| `middle_stage_registers_index_only` | `register_stage_names(_, i, N, addr)` with `0 < i < N - 1` registers only `pp-stage-{i}`. |
| `register_stage_names_count_matches_role` | Returned `Vec<String>` length is 2 for First and Last, 1 for Middle. |
| `register_stage_names_uniqueness` | Across all stages in `{0..N}`, the per-index names are pairwise distinct; `pp-entry` and `pp-exit` are each registered exactly once. |
| `next_stage_name_computed_from_env_alone` | A stage given `STAGE=i`, `NUM_STAGES=N` computes its outbound target name without any external config or network access. |

---

## 5. Python Worker Contract — `tests/test_worker.py`

Surface: `pp_tinygrad_worker.py` stdin/stdout protocol described in
SPEC §3.4 and the original 2-stage SPEC §4. Run against `--stub` mode
that skips real GGUF load.

### 5.1 TestWorkerStartup

| Test name | Proves |
|---|---|
| `test_worker_emits_ready_with_pid_and_stage` | Startup writes `{"status": "ready", "pid": <int>, "stage": <int>}` to stdout. |
| `test_worker_rejects_invalid_stage_env` | `STAGE` outside `[0, NUM_STAGES)` causes immediate exit with non-zero code. |
| `test_worker_rejects_num_stages_one` | `NUM_STAGES=1` exits non-zero (single-node configurations are not served by this example). |

### 5.2 TestFirstStageOperations (stub mode, `STAGE=0`, `NUM_STAGES=4`)

| Test name | Proves |
|---|---|
| `test_tokenize_returns_token_ids_for_prompt` | `op=tokenize` returns `{"request_id": rid, "tokens": [...]}` with at least one id for a non-empty prompt. |
| `test_embed_and_forward_returns_hidden_for_prompt` | `op=embed_and_forward` with a tokenized prompt returns a base64 `hidden` whose decoded byte length matches `seq_len * hidden_dim * 2` (bf16). |
| `test_decode_step_returns_hidden_for_single_token` | `op=decode_step` returns hidden bytes of length `1 * hidden_dim * 2`. |
| `test_kv_cache_grows_across_successive_decode_steps` | Successive `decode_step` calls at increasing `position` succeed; the worker does not error or reset state between them. |
| `test_first_stage_rejects_forward_range` | `op=forward_range` on `STAGE=0` returns `{"error": ...}`, worker stays alive. |
| `test_first_stage_rejects_forward_and_sample` | `op=forward_and_sample` on `STAGE=0` returns `{"error": ...}`, worker stays alive. |

### 5.3 TestMiddleStageOperations (stub mode, `STAGE=1`, `NUM_STAGES=4`) — NEW

| Test name | Proves |
|---|---|
| `test_forward_range_returns_hidden_with_input_seq_len` | `op=forward_range` with `hidden_b64`, `position=p`, `seq_len=s` returns hidden bytes of length `s * hidden_dim * 2`, `request_id` echoed. |
| `test_forward_range_is_deterministic_for_same_input` | Same `hidden_b64` + `position` + `seq_len` → same output hidden bytes. |
| `test_forward_range_kv_cache_advances_with_position` | Repeated `forward_range` at positions `0, prompt_len, prompt_len+1, …` succeed without resetting state. |
| `test_middle_stage_rejects_embed_and_forward` | `op=embed_and_forward` on a middle stage returns `{"error": ...}`. |
| `test_middle_stage_rejects_decode_step` | `op=decode_step` on a middle stage returns `{"error": ...}`. |
| `test_middle_stage_rejects_forward_and_sample` | `op=forward_and_sample` on a middle stage returns `{"error": ...}`. |
| `test_middle_stage_rejects_tokenize` | `op=tokenize` on a middle stage returns `{"error": ...}`. |
| `test_middle_stage_rejects_detokenize` | `op=detokenize` on a middle stage returns `{"error": ...}`. |
| `test_forward_range_rejects_seq_len_mismatch` | `hidden_b64` whose decoded length disagrees with declared `seq_len` is rejected with `{"error": ...}`. |

### 5.4 TestLastStageOperations (stub mode, `STAGE=N-1`)

| Test name | Proves |
|---|---|
| `test_forward_and_sample_returns_valid_token_id` | `op=forward_and_sample` returns a `token_id` in `[0, vocab_size)`. |
| `test_forward_and_sample_is_deterministic_for_same_input` | Same `hidden` + `position` → same `token_id`. |
| `test_detokenize_returns_text_for_token_ids` | `op=detokenize` returns `{"request_id": rid, "text": "..."}` for a valid id sequence. |
| `test_last_stage_rejects_embed_and_forward` | `op=embed_and_forward` on the last stage returns `{"error": ...}`. |
| `test_last_stage_rejects_forward_range` | `op=forward_range` on the last stage returns `{"error": ...}`. |

### 5.5 TestWorkerMalformedInput

| Test name | Proves |
|---|---|
| `test_malformed_json_returns_error_and_continues` | One garbage line yields an `{"error": ...}` reply and the worker accepts a valid request immediately after. |
| `test_missing_op_field_returns_error` | Valid JSON without `op` returns `{"error": ...}`. |
| `test_unknown_op_returns_error` | An `op` not in the worker's dispatch table returns `{"error": ...}`. |
| `test_missing_request_id_returns_error` | Valid op without `request_id` returns `{"error": ...}`. |

### 5.6 TestWorkerEOFShutdown

| Test name | Proves |
|---|---|
| `test_eof_causes_clean_exit` | Closing stdin makes the worker exit 0 within a few seconds. |
| `test_sigterm_causes_clean_exit_after_in_flight_op` | A `SIGTERM` while an op is in flight is honoured after the op completes; worker exits 0. |

---

## 6. Stage Actor — `tests/t_actor.rs`

Surface: the unified `StageActor` and its three roles. Uses the stub
worker. Tests are organised per role.

### 6.1 Common (all roles)

| Test name | Proves |
|---|---|
| `stage_actor_spawns_worker_and_reports_ready` | After spawn, the actor emits `WorkerReady { pid }` once the stub worker prints its ready line, for every role. |
| `stage_actor_worker_crash_emits_process_exited` | If the worker process dies, the actor reports `ProcessExited { status }` and the runtime is not poisoned, for every role. |
| `stopping_stage_actor_kills_child_worker` | Dropping/stopping the actor cleans up the worker pid, for every role. |
| `set_neighbors_updates_routing_addresses` | After `SetNeighbors`, subsequent outbound messages use the new addresses; the previous placeholder addresses are not used. |
| `reset_clears_per_request_state` | After `Reset`, the actor accepts a fresh request and does not double-handle anything from the prior request. |

### 6.2 StageRole::First

| Test name | Proves |
|---|---|
| `first_role_inference_request_produces_stage_activation_to_next` | Sending `InferenceRequest` causes the actor to emit a `StageActivation` to its configured `next` address, with `is_prefill=true` and `seq_len == prompt_token_count`. |
| `first_role_next_token_produces_stage_activation_for_decode_step` | Sending `NextToken { done: false }` causes the actor to emit `StageActivation` with `is_prefill=false`, `seq_len=1`, `position = nt.position`. |
| `first_role_next_token_with_done_produces_no_further_activations` | After `NextToken { done: true }`, the actor emits nothing more for that request. |
| `first_role_drops_activation_messages_defensively` | Sending `StageMsg::Activation` to a `First` actor produces no outbound `StageActivation` and does not crash. |
| `first_role_request_before_ready_is_dropped_without_panic` | An `InferenceRequest` arriving before `WorkerReady` is dropped silently; subsequent requests post-ready still succeed. |

### 6.3 StageRole::Middle — NEW

| Test name | Proves |
|---|---|
| `middle_role_activation_produces_activation_to_next` | Receiving `StageActivation` causes the actor to emit a new `StageActivation` to `next`, with the same `request_id`, `position`, `seq_len`, `is_prefill`. |
| `middle_role_preserves_control_fields_property` | Property test over random `(request_id, position, seq_len, is_prefill)`: a middle stage is a pass-through for those fields; only `hidden` may change. |
| `middle_role_drops_inference_requests_defensively` | Sending `InferenceRequest` to a `Middle` actor produces no outbound messages. |
| `middle_role_drops_next_tokens_defensively` | Sending `NextToken` to a `Middle` actor produces no outbound messages. |
| `middle_role_does_not_emit_inference_response` | A middle actor never emits `InferenceResponse`, regardless of input. |

### 6.4 StageRole::Last

| Test name | Proves |
|---|---|
| `last_role_activation_produces_next_token_to_first` | Receiving `StageActivation` causes the actor to emit `NextToken` to its configured `prev` address (which is stage 0 — the orchestrator wires it that way). |
| `last_role_accumulates_tokens_and_emits_response_on_eos` | After enough `StageActivation`s to produce the configured EOS token (stub-controlled), the actor emits `InferenceResponse { text }` with detokenised output to `reply_to`. |
| `last_role_emits_response_when_max_tokens_reached` | After `max_tokens` activations, the actor emits `InferenceResponse` even without EOS. |
| `last_role_drops_inference_requests_defensively` | Sending `InferenceRequest` to a `Last` actor produces no outbound messages. |
| `last_role_drops_next_tokens_defensively` | Sending `NextToken` to a `Last` actor produces no outbound messages. |
| `last_role_terminate_clears_pending_state` | After terminate, sending another `StageActivation` for the same `request_id` does not duplicate the `InferenceResponse`. |

---

## 7. Bridges — `tests/t_actor.rs::bridges` (or `tests/t_bridges.rs`)

Surface: `RequestBridge`, `NextTokenBridge`, `ActivationBridge` — the
thin actors that adapt a single network type into `StageMsg::*`.
Small, but the wiring contract is load-bearing.

| Test name | Proves |
|---|---|
| `request_bridge_forwards_inference_request_to_target` | A `RequestBridge` configured with `target = T` rewraps an incoming `InferenceRequest` as `StageMsg::Inference(req)` and sends it to `T`. |
| `next_token_bridge_forwards_next_token_to_target` | Same shape for `NextToken` → `StageMsg::NextToken(nt)`. |
| `activation_bridge_forwards_stage_activation_to_target` | Same shape for `StageActivation` → `StageMsg::Activation(act)`. |
| `bridges_drop_messages_when_target_address_invalid` | Sending to a bridge whose target address has been deregistered does not panic; the message is dropped silently. |

---

## 8. Cluster Transport — `tests/t_cluster.rs`

Surface: an N-node swactor cluster (1 orchestrator + N stages) inside
one test process, separate `DistributedNode`s connected via real
iroh in `RelayMode::Disabled`. Tests run for N ∈ {2, 3, 4}.

| Test name | Proves |
|---|---|
| `n_node_cluster_converges_via_iroh_seed_join[N]` | For `N ∈ {2, 3, 4}`, all nodes report each other alive within the SWIM convergence window. |
| `stage_activation_roundtrips_between_each_adjacent_pair[N]` | For each hop `(i → i+1)` in an N-stage cluster, a `StageActivation` arrives intact (bytes + scalars). |
| `next_token_roundtrips_last_to_first[N]` | A `NextToken` sent from stage `N-1` to stage 0 arrives intact (skipping any middle stages on the wire). |
| `inference_response_roundtrips_last_to_orchestrator[N]` | An `InferenceResponse` sent stage-`N-1` → orchestrator arrives intact. |
| `node_death_detected_via_swim_after_middle_stage_shutdown` | If a middle stage's node shuts down, the remaining nodes mark it dead within the SWIM timeout. |
| `node_death_detected_via_swim_after_first_or_last_stage_shutdown` | First and last stage deaths are detected the same way (no role bias in SWIM). |

The `[N]` suffix indicates a parameterised test; concrete cases are
generated by macro or `rstest`.

---

## 9. Orchestrator Setup Dance — `tests/t_orchestrator.rs` (new)

Surface: the spawn-chain address propagation, the find-offer loop,
and the rollback discipline described in SPEC §4. These are pulled
out of the binary into testable helpers
(`pp_smoke_run::seed::spawn_chain` and `pp_smoke_run::vastai::lease_chain`)
so they can be exercised without invoking the binary.

### 9.1 Seed spawn chain

| Test name | Proves |
|---|---|
| `spawn_chain_propagates_each_stage_peer_direct_to_successor` | For N child specs, child `i` is spawned with `PEER_DIRECT` set to child `i-1`'s announced direct addresses; child 0 has no `PEER_DIRECT`. |
| `spawn_chain_reads_pp_gpu_node_addr_in_order` | The chain waits for each child's `PP_GPU_NODE_ADDR` stdout line before spawning the next; children produced out of order are an error. |
| `spawn_chain_kills_already_spawned_on_failure` | If spawn of child `k` fails, children `0..k` are killed before the helper returns. |
| `spawn_chain_kills_already_spawned_on_addr_timeout` | If child `k` fails to announce its addr within the timeout, children `0..k` are killed. |
| `spawn_chain_supports_num_stages_two_through_eight` | The chain runs successfully for `N ∈ {2..8}` against a fake "child" that prints a fixed address line. |

### 9.2 vast.ai lease chain (mocked HTTP — see §10 for the client itself)

| Test name | Proves |
|---|---|
| `lease_chain_finds_n_distinct_offers` | `find_offer` is called N times; each call excludes all previously selected offer ids. |
| `lease_chain_creates_n_instances_with_distinct_stage_env` | `create_instance` is called N times; the `STAGE` env var on call `i` is `i`; `NUM_STAGES` is N on every call. |
| `lease_chain_rolls_back_on_partial_creation` | If `create_instance` call `k` fails, all `0..k` previously created contracts are destroyed before returning the error. |
| `lease_chain_waits_for_running_per_contract` | Each contract reaches `running` state (or times out) before the chain returns success. |

### 9.3 Convergence wait

| Test name | Proves |
|---|---|
| `await_convergence_returns_when_n_minus_one_peers_alive` | The orchestrator's convergence wait returns once `(N-1)` alive peers are seen (the orchestrator itself plus N stages = N+1 nodes total). |
| `await_convergence_returns_error_after_timeout` | If only `(N-2)` peers go alive before the timeout, the wait returns an error and does not consume more time. |

---

## 10. vast.ai Client Extensions — `tests/t_vastai.rs`

Surface: the vast.ai client functions that the lease chain (§9.2)
calls. Mocks HTTP via `wiremock`. Does not hit the real API.

| Test name | Proves |
|---|---|
| `create_n_instances_sends_distinct_stage_env_vars[N]` | For `N ∈ {2, 3, 5}`, N consecutive `create_instance` calls send `STAGE=0..N-1` in the env payload respectively. |
| `create_n_instances_all_receive_same_seed_addr[N]` | For `N ∈ {2, 3, 5}`, every creation call carries the same `SEED_ADDR`. |
| `create_n_instances_all_receive_same_num_stages_env[N]` | Every creation call carries `NUM_STAGES=N`. |
| `failure_to_create_kth_instance_triggers_destroy_of_prior` | If creation of instance `k` errors, the orchestrator destroys instances `0..k` before exiting. |
| `find_offer_excludes_all_prior_offer_ids` | The Nth `find_offer` call excludes the offer ids returned by the previous `N - 1` calls. |
| `find_offer_returns_error_when_fewer_than_n_offers_available` | If the API returns fewer than N distinct offers in total, the chain surfaces an error. |
| `destroy_all_instances_issues_n_delete_requests` | Cleanup issues exactly N DELETEs, one per instance id. |
| `destroy_all_continues_when_one_delete_fails` | A failure to destroy one instance does not prevent the attempt to destroy the others; the result vector contains both outcomes. |

---

## 11. In-Process N-Stage Integration — `tests/t_integration.rs`

Surface: orchestrator + N stages, each on a separate
`DistributedNode` inside one test process, real iroh
(`RelayMode::Disabled`), stub worker. Drives the autoregressive loop
end-to-end without real model compute.

| Test name | Proves |
|---|---|
| `n_stage_stub_pipeline_returns_response[N]` | For `N ∈ {2, 3, 4, 5}`, the in-process equivalent of `pp-smoke-run` sends `InferenceRequest { prompt: "Say hello", max_tokens: 4 }` and receives non-empty `InferenceResponse` from the last stage. |
| `decode_loop_terminates_on_stub_eos[N]` | For `N ∈ {2, 3, 4}`, a stub last-stage configured to emit EOS at token `K` causes the response to arrive with exactly `K` tokens. |
| `decode_loop_terminates_on_max_tokens[N]` | For `N ∈ {2, 3, 4}`, a stub last-stage that never emits EOS with `max_tokens=4` produces a response with 4 accumulated tokens. |
| `middle_stage_failure_mid_decode_surfaces_as_error_to_orchestrator` | At `N=4`, killing the worker of a middle stage mid-loop causes the orchestrator's request future to resolve to an error within the SWIM detection window, not hang. |
| `first_stage_failure_mid_decode_surfaces_as_error_to_orchestrator` | At `N=4`, killing the worker of the first stage mid-loop is detected as above. |
| `last_stage_failure_mid_decode_surfaces_as_error_to_orchestrator` | At `N=4`, killing the worker of the last stage mid-loop is detected as above. |
| `activation_request_id_round_trip_through_chain` | At `N=4`, the `request_id` on the `NextToken` returning from last → first matches the `request_id` of the `StageActivation` first → middle → middle → last. |
| `activation_position_advances_one_per_decode_step` | At `N=3`, with `max_tokens=4`, each `StageActivation` after prefill has `position` exactly one greater than the previous, end-to-end through the chain. |

---

## 12. Sliced-vs-Full Equivalence — `tests/t_integration.rs` (gated)

**The load-bearing correctness test.** Without this, every other test
could pass with silently wrong inference.

Surface: that the N-stage sliced forward pass produces the same
output as a single-process full forward pass on the same prompt and
model. Each test runs on CPU with real `llama3.2:1b` GGUF; minutes
per case; `#[ignore]`.

| Test name | Proves |
|---|---|
| `sliced_two_stage_inference_matches_single_node_for_say_hello` | At `N=2`, the token sequence produced by the pipeline equals tinygrad's single-node `Transformer.generate()` up to `max_tokens=8` on prompt `"Say hello"`. |
| `sliced_two_stage_inference_matches_single_node_for_three_diverse_prompts` | Same equivalence holds for three prompts (one word, one sentence, one with punctuation) at `N=2`. Catches off-by-one errors in position handling, KV cache misalignment, and layer-boundary tensor shape mismatches. |
| `sliced_three_stage_inference_matches_single_node_for_say_hello` | At `N=3`, equivalence on `"Say hello"`. Adds a middle stage to the load-bearing test. |
| `sliced_three_stage_inference_matches_single_node_for_three_diverse_prompts` | At `N=3`, equivalence on the three diverse prompts. |
| `sliced_four_stage_inference_matches_single_node_for_say_hello` | At `N=4`, equivalence on `"Say hello"`. Two middle stages — proves the middle-stage path composes. |
| `sliced_n_stage_inference_is_invariant_to_n_for_argmax` | The token sequence produced is **independent of N** for `N ∈ {2, 3, 4}` on the same prompt with argmax sampling. This is the test that catches any silent state corruption introduced by the chain length. |

These tests gate any merge that touches the worker or the actor's
worker-IPC. A passing tier 8 + a failing tier 12 means the chain
**delivers a response but it is wrong**: this is the failure mode the
test exists to prevent.

---

## 13. Localised Binary E2E — `tests/t_binary.rs`

**The pre-deploy gate.** This is the test the user explicitly called
out as gating any vast.ai deploy. It exercises the real
`pp-smoke-run --seed` binary spawning real `pp-gpu-node` child
processes against each other, with stub workers (so it does not need
a GPU). It is the highest-fidelity test that can run without
provisioning hardware.

Surface: `pp-smoke-run --seed --num-stages N` invoking the real
`pp-gpu-node` binary, `N` times, with stub workers, on localhost.

### 13.1 Happy paths

| Test name | Proves |
|---|---|
| `binary_e2e_n_stub_workers_returns_response[N]` | For `N ∈ {2, 3, 5}`, running `pp-smoke-run --seed --num-stages N` prints a non-empty response and exits 0 within 90s. |
| `binary_e2e_response_contains_accumulated_token_count` | At `N=5`, the response text reflects exactly `max_tokens` accumulated tokens (stub-worker contract). |
| `binary_e2e_orchestrator_registers_pp_orchestrator_name` | At `N=3`, the orchestrator's `pp-orchestrator` name resolves from any stage's perspective post-convergence. |
| `binary_e2e_all_stages_register_pp_stage_index_names` | At `N=4`, every `pp-stage-{i}` for `i ∈ {0..N}` resolves post-convergence. |

### 13.2 Failure / cleanup paths

| Test name | Proves |
|---|---|
| `binary_e2e_first_stage_killed_orchestrator_exits_nonzero[N]` | For `N ∈ {2, 3, 5}`, killing the first `pp-gpu-node` mid-run causes `pp-smoke-run` to exit non-zero. |
| `binary_e2e_middle_stage_killed_orchestrator_exits_nonzero` | At `N=5`, killing a middle `pp-gpu-node` mid-run causes `pp-smoke-run` to exit non-zero. |
| `binary_e2e_last_stage_killed_orchestrator_exits_nonzero[N]` | For `N ∈ {2, 3, 5}`, killing the last `pp-gpu-node` mid-run causes `pp-smoke-run` to exit non-zero. |
| `binary_e2e_no_orphaned_processes_after_clean_exit[N]` | After a successful run, no `pp-gpu-node` child processes remain alive (verified via process listing). |
| `binary_e2e_no_orphaned_processes_after_failed_exit[N]` | After a failed run (any of the kill scenarios above), no `pp-gpu-node` children remain alive. |
| `binary_e2e_orchestrator_sigkilled_children_die_within_timeout` | If `pp-smoke-run` itself is killed mid-run, its child `pp-gpu-node` processes die within 10s (the `ChildGuard` `Drop` invariant generalised). |

### 13.3 Boot-order edge cases

| Test name | Proves |
|---|---|
| `binary_e2e_orchestrator_can_resolve_pp_entry_after_n_stages_register` | At `N=5`, `pp-entry` resolves at the orchestrator only after every stage has registered its per-index name (no premature `pp-entry` visibility). |
| `binary_e2e_pp_smoke_run_handles_slow_middle_stage_boot` | At `N=4`, simulated boot delay of 30s on a middle stage does not cause the orchestrator to time out on convergence (within the 90s budget). |
| `binary_e2e_pp_smoke_run_handles_slow_last_stage_boot` | Same as above, but the slow boot is on the last stage. |

### 13.4 Gate procedure

A clean pass of every test in §13.1 and §13.2 at `N=5` is the
**necessary precondition for invoking `pp-smoke-run --vastai`**. CI
enforces this by gating §14 on §13 passing.

---

## 13b. Docker-Coordinated Localised E2E — `tests/t_docker.rs`

**The Stage 11 pre-deploy gate.** Same shape as §13, but each
`pp-gpu-node` runs inside its own Docker container on `--network host`
instead of as a bare host process. Validates that the image, the
shim, and the orchestrator compose into a single command that brings
the cluster up, drives one request, and tears everything down — the
last test before the manual vast.ai run in §15. `#[ignore]`d;
requires a working Docker daemon. Run with:

```text
cargo test -p pipeline-parallel-inference --test t_docker -- --ignored
```

### 13b.1 Happy paths

| Test name | Proves |
|---|---|
| `docker_e2e_three_stage_cluster_returns_response` | `scripts/docker-e2e.sh 3` (builds the stub image, spawns 3 containers, drives one request, tears down) exits 0 with a non-empty response and no leftover containers. The single-command gate the plan calls out. |
| `docker_e2e_re_running_command_twice_both_pass` | Running the same script twice in a row both pass. Catches state that leaks across runs (cached container names, lingering iroh ports, image build flakes). |

### 13b.2 Failure / cleanup paths

| Test name | Proves |
|---|---|
| `docker_e2e_premature_container_exit_fails_fast` | Killing one stage container mid-decode (`docker kill pp-stage-1`) causes `pp-smoke-run` to exit non-zero within 60s and leave no surviving stage containers. |

### 13b.3 Gate procedure

A clean pass of every §13b test is the **necessary precondition for
invoking `pp-smoke-run --vastai`** — it proves the deployable
container image and the orchestrator coordinate correctly end-to-end
on localhost. The manual vast.ai run in §15 must not be attempted
until both §13 and §13b are green.

---

## 14. Real-Tinygrad Binary E2E (localhost) — `tests/t_binary.rs` (gated)

Surface: same binaries as §13, but the real tinygrad worker on CPU
with the real `llama3.2:1b` GGUF. `#[ignore]`. Takes minutes per
case. Closes the gap between "stubs everywhere" and "real model
weights split across stages."

| Test name | Proves |
|---|---|
| `binary_e2e_real_tinygrad_two_stage_returns_response` | `pp-smoke-run --seed --num-stages 2` with the real worker prints a non-empty response and exits 0. |
| `binary_e2e_real_tinygrad_three_stage_returns_response` | Same at `N=3` — exercises one middle stage on real weights. |
| `binary_e2e_real_tinygrad_four_stage_returns_response` | Same at `N=4` — exercises two middle stages. |
| `binary_e2e_real_tinygrad_response_matches_single_node_for_say_hello` | At `N=3`, the printed response equals the single-node `Transformer.generate()` output on `"Say hello"`. This is the §12 equivalence test, escalated through the full binary. |

---

## 15. Manual Smoke Run — not a test, documented for completeness

Surface: actual rental on vast.ai. Not run by CI. Documented as a
runbook command and a pass criterion.

| Procedure | Pass criterion |
|---|---|
| `VAST_API_KEY=... pp-smoke-run --vastai --num-stages 5 --gpu "RTX 4090"` | Exits 0 within 15 minutes; prints a non-empty response; all 5 rented instances appear destroyed in the vast.ai dashboard. |

**Preconditions before invoking:**

1. Every test in §13.1 and §13.2 at `N=5` passes locally — the
   binary-vs-binary gate.
2. Every test in §13b passes locally — the container-vs-container
   gate, which proves the actual image we ship to vast.ai boots and
   converges in `--network host` mode.

These are the two explicit pre-deploy gates. CI gates §14 on §13;
§13b is gated by the executing developer because it requires a Docker
daemon and an image build.

---

## Out of Scope (Tests We Are Not Writing)

* Performance, latency, throughput tests. The N-stage spec declares
  no perf targets — those are rung 7.
* Fault-tolerance recovery (retry, rescheduling, re-election). Out
  of scope for this rung; rung 6.
* Heterogeneous-split correctness (stages with unequal block counts).
  The planner (rung 5) introduces this; the equivalence test in §12
  asserts only that the *even* split matches the single-node output.
* Streaming token tests. No streaming API.
* Sampling correctness beyond argmax determinism. No temperature,
  top-p, etc.
* Concurrent in-flight requests. Single-request only.
* `N = 1` configurations. Use `examples/single-gpu-inference`.
* `N > 8` configurations. Not in the test matrix; no acceptance
  criterion is defined past `N=5`. Adding `N=8+` is a deliberate
  decision keyed to the planner / fault-tolerance rungs.
* Tests of reused-as-is components from `single-gpu-inference` (iroh
  transport mechanics, swactor process bridge mechanics,
  single-instance vast.ai operations). They are tested in that
  example and are not duplicated here.

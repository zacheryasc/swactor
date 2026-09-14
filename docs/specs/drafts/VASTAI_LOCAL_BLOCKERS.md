# VastAI checkpoint: local blockers

Reviewed 2026-09-14. This is the local-work handoff, in priority order.
The [remote-ready outline](VASTAI_REMOTE_READY.md) describes the work that can
proceed independently on a frozen checkpoint. The [E2E plan](VASTAI_E2E_FUZZ_PLAN.md)
remains the acceptance contract; this report does not change runtime limits or
paid-access guards.

## Decision

The main demonstrated local blocker is end-to-end qualification time, not an
unfinished deployment system or a campaign that cannot complete. Finish the
remaining safety verification and qualify a frozen identity after addressing
that blocker. Do not reopen historical failures without a current reproduction.

## 1. Make the complete ordered workflow fit its time envelope

**Status: demonstrated failure; highest priority.**

`target/ordered-final-3/ordered-acceptance.json` records:

| Stage | Result | Elapsed |
|---|---|---:|
| Five-node, 12-round Gate A | PASS | 331.13 s |
| Complete local campaign | PASS | 274.08 s |
| Combined warm workflow at that point | FAIL | 605.23 s |

The workflow limit is 600 seconds. It expired before failure-case, contract/model,
and scripted-provider checks ran. Saving only 5.23 seconds is therefore not
sufficient: the remaining required stages also need time within that envelope.

### Next work

1. Use the retained stage/round timing evidence to identify the dominant work.
   Keep build/cache preparation separate from measured warm execution.
2. Reproduce the slow stage or operation directly. Make changes only against a
   measured bottleneck, not a speculative broad runtime rewrite.
3. Verify the focused path after each fix. Preserve campaign shape, real public
   bindings, independent observations, fault coverage, and cleanup guarantees.
4. Measure the remaining Gate B stages to establish the headroom actually needed.
   Full ordered execution is final qualification, not the profiling loop.

### Completion condition

The complete warm workflow, including every required Gate B stage, passes in
600 seconds; the campaign remains within its 300-second envelope. Obtain the
required three consecutive warm passes rather than accepting a shortened run.

## 2. Establish the remaining safety and failure-path evidence

**Status: verification gap, not a demonstrated current functional failure.**

The newest ordered run stopped before its failure-case, contract/model, and
scripted-provider stages. There is a separate successful failure-case run and
passing focused tests, but these do not establish complete ordered Gate B.

### Next work

Use the existing checks to verify:

- admission rejection before acquisition, exact five-node acquisition bounds,
  and offer/host selection;
- preparation failures and injected crashes with durable contract accounting;
- cleanup-owner recovery, typed contract absence, unrelated-resource
  preservation, and credential redaction;
- case failure cleanup, replay/shrink isolation, and recovery without acquisition.

Run the scripted provider only through its existing loopback-restricted path;
no paid resources are needed for this local work. Reuse
`tools/myelin-e2e-fuzz/scripted_safety_gate.sh` and the current contract tests.
Focused safety checks are diagnostics until included in ordered qualification.

If a check fails, capture its exact reproduction and make that the active local
bug. Do not label unexecuted scenarios as known broken behavior.

### Completion condition

The full existing failure/safety checks pass with retained artifacts and are
included after Gate A in the final ordered run. A successful destroy request
alone is not cleanup proof.

## 3. Qualify a frozen checkpoint and hand it to paid testing

**Status: integration prerequisite after the first two items.**

At review time the release binaries matched the retained ordered evidence, but
the source digest did not. The ordered record itself is failed, not an
attestation that can authorize paid work.

### Next work

- Freeze the final source checkout, deployment artifacts, and runtime images.
- Use `tools/myelin-e2e-fuzz/ordered_acceptance.py` for the full ordered workflow;
  keep its identity/provenance checks and required stage sequence intact.
- Record all three successful warm runs and separately handle the plan's cold
  timing requirement. Do not report a warm-cache build as cold evidence.
- If source, binaries, or images change, create new qualification evidence.
  Independent remote diagnostics must not mutate this frozen checkout.
- Hand the remote track the exact qualified checkpoint, image identity, and
  fresh attestation. Keep real-provider execution subject to the existing
  admission and cleanup requirements.

### Completion condition

A successful, fresh, identity-matching ordered attestation is available for the
paid path. The current CLI requires at least three warm runs and an attestation
valid for no more than 24 hours. Qualification completion and paid launch must
be coordinated; a stored historical pass is not permanent authorization.

## What not to work on without new evidence

- The old active-stream replacement failure is not the current blocker. A newer
  complete local campaign passed. Reopen it only on a current failing case.
- Do not start another broad runtime, transport, or harness redesign merely
  because the final qualification is incomplete.
- Do not remove safety gates or reduce scenario coverage to obtain a pass.
- Keep unrelated tooling and UI polish outside this critical path.

## Verification already performed for this checkpoint

All of these current-source checks passed during the review:

```text
cargo test --locked -p myelin-e2e-fuzz -p myelin-control-contract --lib
  169 harness tests + 5 shared-contract tests
cargo test --locked -p myelin --lib contextual_process_guarantees
  4 tests
cargo test --locked -p iroh-driver --test telemetry_transport
  4 tests
cargo test --locked -p myelin-e2e-fuzz --bin myelin-e2e-fuzz ordered_gates_
  4 tests
```

Total: 186 passing tests. These are focused checks, not the complete workspace
suite or new ordered acceptance. No remote resources or paid execution were
started during review. Historical `target/` evidence is local to the development
machine and must be preserved separately from these committed reports.

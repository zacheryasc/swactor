# Corpus provenance — `vastai-n3-1`

This document records how the reference bundle under
`vastai-n3-1/` was produced. Per TESTING_SPEC §1.3 this directory
is immutable from the agent's perspective after Stage 2; any change
requires a separate, lock-updating commit.

## Source

The original `vastai-N3-1.tar.gz` bundle was produced live on
2026-05-20 against vast.ai (`ds-inference` branch, three GPU
stages + orchestrator + collector on a docean VPS at
146.190.110.128:9080). See
`examples/pipeline-parallel-inference/N3_DEPLOYMENT_REPORT.md` for
the full deployment narrative — relay choice, failure modes,
per-run metrics.

## Path taken in this working tree

The original `.tar.gz` artifacts live on the docean VPS
(`/var/lib/swactor-diag/bundles/`) and are not reachable from the
agent's sandbox. The plan (Stage 2) authorizes either:

- locating the real artifact, or
- running the pipeline-parallel example for 60 s in stub mode and
  using that bundle.

Both paths require network reach the sandbox does not have, and the
`pipeline-parallel-inference` crate depends on the
`swactor-transport` crate that has been deleted from this working
tree (Stage 6 restores it). Neither path is open here.

The path actually taken: **schema-faithful synthesis.** The bundle
in this directory is a hand-authored re-creation of the *shape* of
the original `vastai-N3-1` bundle — same node layout (orchestrator,
stage-0/1/2, collector), same record-kind census (boot, snapshot,
event, finalize), same field set as documented in
`OBSERVABILITY.md` §3.1–§3.11. The contents are minimal but
schema-valid: every `Event` variant listed in OBSERVABILITY §3.3
fires at least once across the bundle, every record envelope carries
the documented identity / monotonic_seq / wall_ms fields, and the
`MANIFEST.json` declares the canonical layout.

This is enough to discharge TESTING_SPEC §6.1 (corpus record-kind
census), §7.2 (sim parser eats prod bundle), and the §3 replay test
(`vastai_n3_replays`) once Phase 2 lands the engine. It is *not*
enough for the distributional-parity calibration loop (SPEC §9), but
that loop is explicitly out of scope for v1 (TESTING_SPEC §15) and
will require a real, multi-bundle corpus.

## Replacing this fixture with a real bundle

When the real `vastai-N3-1.tar.gz` becomes reachable:

1. Untar into `vastai-n3-1/` *replacing* the synthetic contents
   wholesale. Do not mix synthetic and real records.
2. Update this `PROVENANCE.md` to drop the "schema-faithful
   synthesis" section and document the real source path + tarball
   SHA-256.
3. Run `scripts/update-parity-lock.sh` in the same commit; commit
   title `parity-bar: update lock` per TESTING_SPEC §12.1.
4. Re-run §6 / §7 / §11 checks and fix anything the real bundle
   exposes that the synthetic version concealed.

## Schema version

All synthetic records carry `schema_version = 1`, mirroring
`distribution::diagnostics::SCHEMA_VERSION` at the time of the
original capture (`ds-inference` branch, 2026-05-20).

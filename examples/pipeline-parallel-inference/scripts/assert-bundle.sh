#!/usr/bin/env bash
# assert-bundle.sh — bundle invariant checks for the diagnostics e2e gate.
#
# Given a finalized tarball produced by `swactor-diag-collector`, runs
# the post-processor against it and verifies the structural properties
# the S12 plan requires:
#
#   1. The manifest lists the orchestrator and every stage.
#   2. Each node has at least one snapshot.
#   3. Events from each tier (1: SWIM/dial/message; 2: iroh-internal;
#      3: probe/host/process) appear somewhere in the bundle.
#   4. `swactor-diag-postproc` exits 0.
#   5. The produced `summary.md` is non-empty.
#
# Usage:
#   assert-bundle.sh <bundle.tar.gz> <expected_stage_count> [<postproc-binary>]
#
# Exits 0 on success, prints a diagnostic to stderr and exits 1 on any
# failure. Designed to be called from `docker-diag-e2e.sh` but usable
# standalone for ad-hoc verification of a captured bundle.
set -euo pipefail

BUNDLE="${1:?usage: assert-bundle.sh <bundle.tar.gz> <expected_stages> [postproc]}"
EXPECTED_STAGES="${2:?usage: assert-bundle.sh <bundle.tar.gz> <expected_stages> [postproc]}"
POSTPROC_BIN="${3:-swactor-diag-postproc}"

if [ ! -s "$BUNDLE" ]; then
    echo "assert-bundle: $BUNDLE does not exist or is empty" >&2
    exit 1
fi

WORKDIR="$(mktemp -d -t pp-diag-assert.XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT

EXTRACTED="$WORKDIR/extracted"
mkdir -p "$EXTRACTED"
tar -xzf "$BUNDLE" -C "$EXTRACTED"

# The bundle's top-level dir is named after the run id. We don't need to
# know the name in advance — walk one level down.
RUN_DIR=$(find "$EXTRACTED" -mindepth 1 -maxdepth 1 -type d | head -n 1)
if [ -z "$RUN_DIR" ]; then
    echo "assert-bundle: bundle has no top-level run directory" >&2
    ls -lR "$EXTRACTED" >&2
    exit 1
fi
echo "assert-bundle: run dir $RUN_DIR"

# (1) Manifest sanity.
MANIFEST="$RUN_DIR/MANIFEST.json"
if [ ! -s "$MANIFEST" ]; then
    echo "assert-bundle: MANIFEST.json missing or empty in $RUN_DIR" >&2
    exit 1
fi
# Friendly node labels are written into the per-node subdirs (e.g.
# `orchestrator`, `stage-0`, ...). Count them, and check that
# orchestrator + every stage 0..N-1 are present. The label scheme is
# the collector's `bundle::assemble` contract.
node_dirs=()
while IFS= read -r -d '' dir; do
    node_dirs+=("$(basename "$dir")")
done < <(find "$RUN_DIR" -mindepth 1 -maxdepth 1 -type d -print0 | sort -z)
echo "assert-bundle: node dirs: ${node_dirs[*]:-<none>}"

declare -A seen
for d in "${node_dirs[@]}"; do
    seen["$d"]=1
done

missing=()
[ -n "${seen[orchestrator]:-}" ] || missing+=("orchestrator")
for ((i = 0; i < EXPECTED_STAGES; i++)); do
    [ -n "${seen[stage-$i]:-}" ] || missing+=("stage-$i")
done
if [ "${#missing[@]}" -gt 0 ]; then
    echo "assert-bundle: bundle missing nodes: ${missing[*]}" >&2
    echo "(found: ${node_dirs[*]})" >&2
    exit 1
fi
echo "assert-bundle: orchestrator + $EXPECTED_STAGES stages all present"

# (2) Each node has at least one snapshot. The collector bundle layout
# (see `collector::bundle::assemble`) groups records into per-kind
# subdirs under each node label: `<label>/snapshots/snapshot-*.json`,
# `<label>/events/events-*.json`. Use `-path` so the check survives
# future renames if anyone reshapes the layout deeper.
for d in "${node_dirs[@]}"; do
    snap_count=$(find "$RUN_DIR/$d" -type f -name 'snapshot-*.json' | wc -l)
    if [ "$snap_count" -lt 1 ]; then
        echo "assert-bundle: $d has zero snapshots" >&2
        echo "(node tree:" >&2
        find "$RUN_DIR/$d" -maxdepth 3 >&2 || true
        echo ")" >&2
        exit 1
    fi
done
echo "assert-bundle: every node has at least one snapshot"

# (3) Events span all three tiers. The collector persists batched events
# as `events-*.json`; each file is a JSON array of EventRecords whose
# `event.type` discriminator names the variant. The variant names are
# the API-shape contract from plan.md, stable across the diagnostics
# crate's lifetime.
# Match the pretty-printed shape `"type": "Variant"` produced by
# `serde_json::to_vec_pretty` (one space between key and value).
TIER1_PATTERN='"type": "\(SwimTransition\|DialStarted\|DialOutcome\|MessageSent\|MessageReceived\|SwimMetadataSent\|SwimMetadataReceived\)"'
TIER2_PATTERN='"type": "\(IrohConnTypeChanged\|RelayChanged\|ConnectionCacheHit\|ConnectionCacheMiss\|ConnectionCacheInvalidated\|NodeMapUpdate\)"'
TIER3_PATTERN='"type": "\(ProbeSent\|ProbeReceived\)"'

event_files=$(find "$RUN_DIR" -type f -name 'events-*.json' -print)
if [ -z "$event_files" ]; then
    echo "assert-bundle: no events-*.json files in bundle" >&2
    exit 1
fi

# shellcheck disable=SC2086
have_tier1=$(grep -l "$TIER1_PATTERN" $event_files 2>/dev/null | head -n 1 || true)
# shellcheck disable=SC2086
have_tier2=$(grep -l "$TIER2_PATTERN" $event_files 2>/dev/null | head -n 1 || true)
# shellcheck disable=SC2086
have_tier3=$(grep -l "$TIER3_PATTERN" $event_files 2>/dev/null | head -n 1 || true)

missing_tiers=()
[ -n "$have_tier1" ] || missing_tiers+=("Tier 1 (SWIM/dial/message)")
[ -n "$have_tier2" ] || missing_tiers+=("Tier 2 (iroh-internal)")
[ -n "$have_tier3" ] || missing_tiers+=("Tier 3 (probes)")
if [ "${#missing_tiers[@]}" -gt 0 ]; then
    echo "assert-bundle: bundle missing event tiers: ${missing_tiers[*]}" >&2
    echo "event files:" >&2
    echo "$event_files" >&2
    exit 1
fi
echo "assert-bundle: events span Tier 1, 2, 3"

# (4)+(5) Run the post-processor and inspect the summary.
POSTPROC_OUT="$WORKDIR/postproc-out"
mkdir -p "$POSTPROC_OUT"
if ! "$POSTPROC_BIN" "$BUNDLE" -o "$POSTPROC_OUT"; then
    echo "assert-bundle: swactor-diag-postproc failed" >&2
    exit 1
fi
SUMMARY="$POSTPROC_OUT/summary.md"
if [ ! -s "$SUMMARY" ]; then
    echo "assert-bundle: summary.md is missing or empty" >&2
    ls -lR "$POSTPROC_OUT" >&2
    exit 1
fi
echo "assert-bundle: summary.md $(wc -c < "$SUMMARY") bytes, $(wc -l < "$SUMMARY") lines"
echo "assert-bundle: PASS"

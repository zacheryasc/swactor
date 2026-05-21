#!/usr/bin/env bash
# Parity-bar hash-lock check (TESTING_SPEC §12.1).
#
# Computes a stable digest of every file under
# `crates/simulation/tests/parity-bar/` (plus the locked
# `TESTING_SPEC.md`) and compares it against the value committed in
# `.locked-hashes`.
#
# Exit 0 iff the digest matches. Mismatch prints a diff hint and
# exits 1. The hash is computed over sorted (path, sha256) pairs so
# the result is independent of `find`'s walk order across
# filesystems.

set -euo pipefail

ROOT="${1:-$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel 2>/dev/null \
    || ( cd "$(dirname "$0")/.." && pwd ) )}"

PARITY_DIR="$ROOT/crates/simulation/tests/parity-bar"
SPEC="$ROOT/crates/simulation/TESTING_SPEC.md"
LOCK_FILE="$PARITY_DIR/.locked-hashes"

if [[ ! -d "$PARITY_DIR" ]]; then
    echo "check-parity-lock: parity-bar directory missing: $PARITY_DIR" >&2
    exit 1
fi
if [[ ! -f "$LOCK_FILE" ]]; then
    echo "check-parity-lock: lock file missing: $LOCK_FILE" >&2
    echo "  run scripts/update-parity-lock.sh to create it" >&2
    exit 1
fi

# Build a sorted (path, sha256) manifest, then sha256 that.
compute_digest() {
    {
        find "$PARITY_DIR" -type f ! -name '.locked-hashes' -print0 \
            | sort -z \
            | xargs -0 sha256sum
        sha256sum "$SPEC"
    } | sed -E "s|$ROOT/||" | sha256sum | awk '{print $1}'
}

LIVE=$(compute_digest)
LOCKED=$(grep -E '^[[:space:]]*[0-9a-f]{64}[[:space:]]*$' "$LOCK_FILE" | head -n1 | awk '{print $1}')

if [[ -z "$LOCKED" ]]; then
    echo "check-parity-lock: lock file does not contain a SHA-256 hash" >&2
    exit 1
fi

if [[ "$LIVE" != "$LOCKED" ]]; then
    echo "check-parity-lock: parity-bar digest changed." >&2
    echo "  expected: $LOCKED" >&2
    echo "  computed: $LIVE" >&2
    echo "  run scripts/update-parity-lock.sh in a separate commit" >&2
    echo "  titled 'parity-bar: update lock' (TESTING_SPEC §12.1)." >&2
    exit 1
fi

echo "check-parity-lock: OK ($LIVE)"

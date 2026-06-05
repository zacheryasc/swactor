#!/usr/bin/env bash
# One-liner datastream demo: build, bring the cluster up, stream to stdout.
#
#   ./tests/docker/datastream-demo.sh
#
# The image is multi-stage — Docker compiles the binaries inside the builder
# stage, so you need nothing on the host but Docker. Brings the cluster up in
# the foreground so the collector's live, frame-by-frame stream prints to your
# terminal; Ctrl-C tears it back down. (First build is slow — it compiles the
# workspace; later runs are cached.)
set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE="docker compose -f tests/docker/docker-compose.datastream.yml"

$COMPOSE build
trap '$COMPOSE down' EXIT INT TERM
$COMPOSE up

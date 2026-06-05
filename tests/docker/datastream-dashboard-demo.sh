#!/usr/bin/env bash
# Datastream demo, browser-dashboard edition: build, bring the cluster up, and
# serve the live dashboard from the demuxed telemetry stream.
#
#   ./tests/docker/datastream-dashboard-demo.sh
#
# Same cluster and data as ./datastream-demo.sh — relay, seed, two workers, each
# shipping its telemetry datastream — but the UDP sink is the HTTP dashboard
# (swactor-datastream-dashboard) instead of the stdout collector. It demuxes the
# frames and renders one node in the existing browser UI.
#
# Open the dashboard once the cluster is up (port printed below; default 18080).
# Ctrl-C tears it down. Set DASH_PORT=<port> if the default host port is taken.
# (First build is slow — it compiles the workspace; later runs are cached.)
set -euo pipefail
cd "$(dirname "$0")/../.."

DASH_PORT="${DASH_PORT:-18080}"
export DASH_PORT

COMPOSE="docker compose \
  -f tests/docker/docker-compose.datastream.yml \
  -f tests/docker/docker-compose.datastream.dashboard.yml"

$COMPOSE build
trap '$COMPOSE down' EXIT INT TERM
echo
echo "  Dashboard:  http://localhost:${DASH_PORT}"
echo
$COMPOSE up

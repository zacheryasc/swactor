#!/usr/bin/env bash
# PID-1 supervisor for the pipeline-parallel runtime container.
#
# The worker used to run as PID 1 (`exec pp-gpu-node`), so SSH depended on
# vast's host-side helper winning a race against our exec, and a worker crash
# killed the whole container — no shell left to read the traceback. This script
# instead owns PID 1: it brings up sshd deterministically, runs the worker as a
# child, and survives the worker's exit so the node stays reachable for
# postmortem.
set -u

# Guard against a double invocation (e.g. image ENTRYPOINT and vast onstart both
# firing): the first holder wins, any later one just parks so it can't start a
# second sshd/worker pair.
if ! mkdir /run/pp_entrypoint.lock 2>/dev/null; then
    echo "pp-entrypoint: already running (lock held); parking" >&2
    exec sleep infinity
fi

# ── SSH: deterministic key + sshd, independent of vast's helper ──────────────
mkdir -p /root/.ssh && chmod 700 /root/.ssh
: > /root/.ssh/authorized_keys
# vast injects the account key into the container env as PUBLIC_KEY for
# SSH-launch instances; accept either spelling. A deploy key may also be baked
# at build time (Dockerfile ARG DEPLOY_PUBKEY) as a fallback that does not
# depend on vast env injection.
if [ -n "${PUBLIC_KEY:-}" ]; then printf '%s\n' "$PUBLIC_KEY" >> /root/.ssh/authorized_keys; fi
if [ -n "${SSH_PUBLIC_KEY:-}" ]; then printf '%s\n' "$SSH_PUBLIC_KEY" >> /root/.ssh/authorized_keys; fi
if [ -f /etc/pp_deploy_key.pub ]; then cat /etc/pp_deploy_key.pub >> /root/.ssh/authorized_keys; fi
chmod 600 /root/.ssh/authorized_keys

if [ ! -s /root/.ssh/authorized_keys ]; then
    echo "pp-entrypoint: WARNING no SSH public key found (PUBLIC_KEY/SSH_PUBLIC_KEY unset, no baked key); SSH will reject logins" >&2
fi

mkdir -p /run/sshd
ssh-keygen -A 2>/dev/null || true
# Listen on 22, where vast's SSH proxy forwards.
/usr/sbin/sshd -e
echo "pp-entrypoint: sshd up on :22" >&2

# ── Worker: run as a child, tee output to a file readable over SSH ───────────
WORKER_LOG=/var/log/pp-worker.log
echo "pp-entrypoint: launching pp-gpu-node (log -> $WORKER_LOG)" >&2
set -o pipefail
/usr/local/bin/pp-gpu-node 2>&1 | tee "$WORKER_LOG"
code=${PIPESTATUS[0]}

# ── Operator runbooks (manual, over SSH) ─────────────────────────────────────
# Iterating on a live node without re-leasing:
#
#   Worker hot-reload (no restart) — edit the Python in place, then SIGHUP:
#     scp -P <port> pp_tinygrad_worker.py root@<host>:/usr/local/share/pp_tinygrad_worker.py
#     ssh <host> 'kill -HUP $(pidof pp-gpu-node)'
#   pp-gpu-node tears down its worker and re-execs the on-disk script; the
#   swactor process (and SWIM membership) stays up across the swap.
#
#   Swactor-binary swap — stop the binary, stage the new one, re-exec under
#   PID 1's env (preserves STAGE/SEED_ADDR/PP_STAGE_SECRET so the node id is
#   unchanged). `.new` staging avoids ETXTBSY on the mapped ELF:
#     ssh <host> 'pkill -x pp-gpu-node'                    # drops to the hold below
#     scp -P <port> pp-gpu-node root@<host>:/usr/local/bin/pp-gpu-node.new
#     ssh <host> 'mv -f /usr/local/bin/pp-gpu-node.new /usr/local/bin/pp-gpu-node && \
#       chmod +x /usr/local/bin/pp-gpu-node && \
#       setsid bash -c "while IFS= read -r -d \"\" kv; do export \"\$kv\"; done \
#         < /proc/1/environ; exec /usr/local/bin/pp-gpu-node" \
#       >/var/log/pp-restart.log 2>&1 </dev/null &'
#
# ── Crash policy: do NOT restart. Keep PID 1 / sshd alive for postmortem. ────
echo "pp-entrypoint: pp-gpu-node exited with code $code; NOT restarting (node held for postmortem)" >&2
echo "pp-entrypoint: --- last 40 lines of $WORKER_LOG ---" >&2
tail -n 40 "$WORKER_LOG" >&2 || true
exec sleep infinity

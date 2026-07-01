#!/usr/bin/env bash
set -u

if ! mkdir /run/mvp_entrypoint.lock 2>/dev/null; then
    echo "mvp-entrypoint: already running (lock held); parking" >&2
    exec sleep infinity
fi

mkdir -p /root/.ssh && chmod 700 /root/.ssh
: > /root/.ssh/authorized_keys
if [ -n "${PUBLIC_KEY:-}" ]; then printf '%s\n' "$PUBLIC_KEY" >> /root/.ssh/authorized_keys; fi
if [ -n "${SSH_PUBLIC_KEY:-}" ]; then printf '%s\n' "$SSH_PUBLIC_KEY" >> /root/.ssh/authorized_keys; fi
if [ -f /etc/mvp_deploy_key.pub ]; then cat /etc/mvp_deploy_key.pub >> /root/.ssh/authorized_keys; fi
chmod 600 /root/.ssh/authorized_keys

if [ ! -s /root/.ssh/authorized_keys ]; then
    echo "mvp-entrypoint: WARNING no SSH public key found (PUBLIC_KEY/SSH_PUBLIC_KEY unset, no baked key); SSH will reject logins" >&2
fi

mkdir -p /run/sshd /var/log /var/cache/mvp-models
ssh-keygen -A 2>/dev/null || true
/usr/sbin/sshd -e
echo "mvp-entrypoint: sshd up on :22" >&2

NODE_LOG=/var/log/mvp-node.log
echo "mvp-entrypoint: launching mvp-node (log -> $NODE_LOG)" >&2
set -o pipefail
/usr/local/bin/mvp-node "$@" 2>&1 | tee "$NODE_LOG"
code=${PIPESTATUS[0]}

echo "mvp-entrypoint: mvp-node exited with code $code; NOT restarting (node held for postmortem)" >&2
echo "mvp-entrypoint: --- last 80 lines of $NODE_LOG ---" >&2
tail -n 80 "$NODE_LOG" >&2 || true
exec sleep infinity

#!/usr/bin/env bash
set -u

if ! mkdir /run/myelin_entrypoint.lock 2>/dev/null; then
    echo "myelin-entrypoint: already running (lock held); parking" >&2
    exec sleep infinity
fi

mkdir -p /root/.ssh && chmod 700 /root/.ssh
: > /root/.ssh/authorized_keys
if [ -n "${PUBLIC_KEY:-}" ]; then printf '%s\n' "$PUBLIC_KEY" >> /root/.ssh/authorized_keys; fi
if [ -n "${SSH_PUBLIC_KEY:-}" ]; then printf '%s\n' "$SSH_PUBLIC_KEY" >> /root/.ssh/authorized_keys; fi
if [ -f /etc/myelin_deploy_key.pub ]; then cat /etc/myelin_deploy_key.pub >> /root/.ssh/authorized_keys; fi
chmod 600 /root/.ssh/authorized_keys

if [ ! -s /root/.ssh/authorized_keys ]; then
    echo "myelin-entrypoint: WARNING no SSH public key found (PUBLIC_KEY/SSH_PUBLIC_KEY unset, no baked key); SSH will reject logins" >&2
fi

mkdir -p /run/sshd /var/log /var/cache/myelin-models
ssh-keygen -A 2>/dev/null || true
/usr/sbin/sshd -e
echo "myelin-entrypoint: sshd up on :22" >&2

NODE_LOG=/var/log/myelin-node.log
echo "myelin-entrypoint: launching myelin-node (log -> $NODE_LOG)" >&2
set -o pipefail
/usr/local/bin/myelin-node "$@" 2>&1 | tee "$NODE_LOG"
code=${PIPESTATUS[0]}

echo "myelin-entrypoint: myelin-node exited with code $code; NOT restarting (node held for postmortem)" >&2
echo "myelin-entrypoint: --- last 80 lines of $NODE_LOG ---" >&2
tail -n 80 "$NODE_LOG" >&2 || true
exec sleep infinity

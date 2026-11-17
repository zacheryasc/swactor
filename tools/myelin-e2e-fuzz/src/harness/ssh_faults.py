#!/usr/bin/python3
"""Harness-only deployment barriers around the real SSH transport."""
import os
import shlex
import sys


BOUNDARY_WAIT = """
import ctypes, os, select, sys
from pathlib import Path
path = Path(sys.argv[1])
libc = ctypes.CDLL(None, use_errno=True)
fd = libc.inotify_init1(os.O_CLOEXEC | os.O_NONBLOCK)
if fd < 0:
    raise OSError(ctypes.get_errno(), 'subscribe boundary release')
try:
    if libc.inotify_add_watch(fd, os.fsencode(path.parent), 0x200 | 0x40) < 0:
        raise OSError(ctypes.get_errno(), 'watch boundary release')
    while path.exists():
        if select.select([fd], [], [], 30)[0]:
            os.read(fd, 65536)
finally:
    os.close(fd)
"""


def inject(remote):
    prefix, separator, quoted = remote.partition(" sh -lc ")
    if not separator:
        return remote
    words = shlex.split(quoted)
    if len(words) != 1 or not words[0].startswith("set -eu\nroot=/opt/myelin; bundle_digest="):
        return remote
    transaction = words[0]
    definition = '\n'.join([
        'boundaries="$root/deployment-boundaries"',
        'boundary() {',
        '    mkdir -p "$boundaries" && touch "$boundaries/$gen.$1.reached" || exit 69',
        '    python3 -c ' + shlex.quote(BOUNDARY_WAIT) + ' "$boundaries/$gen.$1.hold"',
        '}',
        '',
    ])
    # Fail closed if the operational transaction changes: a campaign must not
    # silently skip its requested fault boundary. These are shell operation
    # boundaries, not hooks compiled into Myelin.
    insertions = [
        ('process_start() {\n', definition + 'process_start() {\n'),
        ('    cat > "$stage/bundle.tar"', '    boundary transfer_started\n    cat > "$stage/bundle.tar"'),
        ('# A cache hit is only content evidence.', 'if [ "$transfer" -eq 0 ]; then\n    boundary transfer_started\nfi\n# A cache hit is only content evidence.'),
        ('python3 -c "$installation" verify "$release" "$digest" "$exec_digest" ||\n', 'boundary before_launch\npython3 -c "$installation" verify "$release" "$digest" "$exec_digest" ||\n'),
        ('start_ticks=$(process_start "$pid") || exit 75\n', 'start_ticks=$(process_start "$pid") || exit 75\nboundary after_launch\n'),
        ('workers=$(pgrep -x myelin-worker 2>/dev/null || true); set -- $workers\n', 'boundary before_receipt\nworkers=$(pgrep -x myelin-worker 2>/dev/null || true); set -- $workers\n'),
    ]
    for original, replacement in insertions:
        if transaction.count(original) != 1:
            raise ValueError('deployment operation boundary changed: ' + original.strip())
        transaction = transaction.replace(original, replacement, 1)
    return prefix + separator + shlex.quote(transaction)


def main():
    args = sys.argv[1:]
    if args:
        args[-1] = inject(args[-1])
    real_ssh = os.environ['MYELIN_HARNESS_REAL_SSH']
    os.execv(real_ssh, [real_ssh, *args])


if __name__ == '__main__':
    main()

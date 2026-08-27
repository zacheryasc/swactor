#!/usr/bin/env python3
import argparse
import base64
import os
import runpy
import sys
import signal
import tempfile
import zlib


def main() -> None:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file")
    source.add_argument("--source-base64")
    source.add_argument("--source-env")
    source.add_argument("--source-env-zlib")
    args = parser.parse_args()

    if args.file is not None:
        runpy.run_path(args.file, run_name="__main__")
        return

    encoded = args.source_base64
    if args.source_env is not None:
        encoded = os.environ.get(args.source_env)
        if encoded is None:
            raise RuntimeError(
                f"generated program environment variable is missing: {args.source_env}"
            )
    if args.source_env_zlib is not None:
        encoded = os.environ.get(args.source_env_zlib)
        if encoded is None:
            raise RuntimeError(
                "compressed generated program environment variable is missing: "
                f"{args.source_env_zlib}"
            )
    if encoded is None:
        raise RuntimeError("generated program source is missing")

    program = base64.b64decode(encoded, validate=True)
    if args.source_env_zlib is not None:
        program = zlib.decompress(program)
    if os.environ.get("MYELIN_E2E_HOLD_BEFORE_BOOTSTRAP") == "1":
        signal.pause()
    with tempfile.NamedTemporaryFile(prefix="myelin-e2e-", suffix=".py", delete=False) as handle:
        handle.write(program)
        path = handle.name
    try:
        sys.argv = [path]
        runpy.run_path(path, run_name="__main__")
    finally:
        os.unlink(path)


if __name__ == "__main__":
    main()

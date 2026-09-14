#!/usr/bin/env python3
import base64
import os
import sys
import zlib


def main() -> None:
    if len(sys.argv) != 3:
        raise RuntimeError("expected exactly one generated program source option")
    option, value = sys.argv[1:]
    if option == "--file":
        with open(value, "rb") as source:
            program = source.read()
        filename = value
    elif option == "--source-base64":
        program = base64.b64decode(value, validate=True)
        filename = "<myelin-e2e-generated>"
    elif option in {"--source-env", "--source-env-zlib"}:
        encoded = os.environ.get(value)
        if encoded is None:
            raise RuntimeError(
                f"generated program environment variable is missing: {value}"
            )
        program = base64.b64decode(encoded, validate=True)
        if option == "--source-env-zlib":
            program = zlib.decompress(program)
        filename = "<myelin-e2e-generated>"
    else:
        raise RuntimeError(f"unsupported generated program source option: {option}")

    if os.environ.get("MYELIN_E2E_HOLD_BEFORE_BOOTSTRAP") == "1":
        import signal

        signal.pause()
    sys.argv = [filename]
    namespace = {
        "__name__": "__main__",
        "__file__": filename,
        "__package__": None,
        "__loader__": None,
        "__spec__": None,
        "__cached__": None,
    }
    exec(compile(program, filename, "exec"), namespace)


if __name__ == "__main__":
    main()

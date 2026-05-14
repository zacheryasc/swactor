"""T-worker: tinygrad worker protocol tests.

Spawns tinygrad_worker.py --stub as a subprocess and verifies the
stdin/stdout JSON protocol defined in SPEC.md §2.3.
"""

import json
import os
import signal
import subprocess
import sys
import time

import pytest

WORKER_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "tinygrad_worker.py")
PYTHON = sys.executable


def spawn_worker():
    """Spawn the worker in --stub mode and return the Popen handle."""
    proc = subprocess.Popen(
        [PYTHON, WORKER_SCRIPT, "--stub"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc


def read_line(proc, timeout=5):
    """Read one line from the worker's stdout, with a timeout."""
    import selectors
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    events = sel.select(timeout=timeout)
    if not events:
        raise TimeoutError(f"No output from worker within {timeout}s")
    line = proc.stdout.readline()
    sel.close()
    if not line:
        raise EOFError("Worker closed stdout")
    return json.loads(line.strip())


def send_line(proc, obj):
    """Write a JSON line to the worker's stdin."""
    proc.stdin.write(json.dumps(obj) + "\n")
    proc.stdin.flush()


def send_raw(proc, text):
    """Write raw text to the worker's stdin."""
    proc.stdin.write(text + "\n")
    proc.stdin.flush()


class TestWorkerStartup:
    """Worker prints {"status": "ready"} on startup."""

    def test_worker_emits_ready_on_startup(self):
        proc = spawn_worker()
        try:
            msg = read_line(proc)
            assert msg == {"status": "ready"}, f"Expected ready status, got: {msg}"
        finally:
            proc.terminate()
            proc.wait(timeout=5)


class TestWorkerInference:
    """Valid request produces a valid response with non-empty text."""

    def test_valid_request_returns_non_empty_response(self):
        proc = spawn_worker()
        try:
            ready = read_line(proc)
            assert ready["status"] == "ready"

            send_line(proc, {"prompt": "Say hello", "max_tokens": 8})
            response = read_line(proc)

            assert "response" in response, f"Response missing 'response' field: {response}"
            assert isinstance(response["response"], str)
            assert len(response["response"]) > 0, "Response text must be non-empty"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_multiple_requests_in_sequence(self):
        """Worker handles multiple requests without restarting."""
        proc = spawn_worker()
        try:
            read_line(proc)  # ready

            for prompt in ["Hello", "World", "Test"]:
                send_line(proc, {"prompt": prompt, "max_tokens": 8})
                response = read_line(proc)
                assert "response" in response
                assert len(response["response"]) > 0
        finally:
            proc.terminate()
            proc.wait(timeout=5)


class TestWorkerMalformedInput:
    """Malformed JSON produces {"error": "..."} and worker continues."""

    def test_malformed_json_returns_error_and_continues(self):
        proc = spawn_worker()
        try:
            read_line(proc)  # ready

            # Send garbage
            send_raw(proc, "this is not json {{{")
            err = read_line(proc)
            assert "error" in err, f"Expected error response, got: {err}"

            # Worker should still be alive and handle the next valid request
            send_line(proc, {"prompt": "Still alive?", "max_tokens": 8})
            response = read_line(proc)
            assert "response" in response, "Worker should continue after malformed input"
            assert len(response["response"]) > 0
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_partial_json_returns_error(self):
        proc = spawn_worker()
        try:
            read_line(proc)  # ready

            send_raw(proc, '{"prompt": "incomplete')
            err = read_line(proc)
            assert "error" in err
        finally:
            proc.terminate()
            proc.wait(timeout=5)


class TestWorkerEOFShutdown:
    """Closing stdin (EOF) causes worker to exit cleanly with code 0."""

    def test_eof_causes_clean_exit(self):
        proc = spawn_worker()
        read_line(proc)  # ready

        # Close stdin
        proc.stdin.close()

        # Worker should exit cleanly
        exit_code = proc.wait(timeout=5)
        assert exit_code == 0, f"Worker exited with code {exit_code}, expected 0"

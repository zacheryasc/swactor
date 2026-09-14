#!/usr/bin/env python3
"""Local VastAI wire fixture and independent real-CLI safety scenarios.

Server: scripted_provider.py PORT CONTROL_JSON REQUEST_LOG [READY_FD]
Gate:   scripted_provider.py --gate ROOT OUTPUT HARNESS_BINARY

Each server owns its contracts, request ledger, and fault controls. The ready
pipe is written only after HTTP bind/listen succeeds. No credential is logged.
"""

import json
import os
from pathlib import Path
import select
import secrets
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


UNRELATED_ID = 800000
UNRELATED_LABEL = "unrelated-account-resource-marker"
UNRELATED_CONTRACTS = {UNRELATED_ID: UNRELATED_LABEL}
UNRELATED_WIRE_CONTRACTS = {str(UNRELATED_ID): UNRELATED_LABEL}
KEY_ENV = "MYELIN_SCRIPTED_VASTAI_KEY"
CREDENTIAL_ENVS = (KEY_ENV, "VAST_API_KEY", "MYELIN_VASTAI_API_KEY", "VASTAI_API_KEY")


def credential_values(env):
    return tuple(env[name].encode() for name in CREDENTIAL_ENVS if env.get(name))


def require_credential_absence(content, env, surface):
    if any(value in content for value in credential_values(env)):
        raise AssertionError(f"credential leaked into {surface}")


def scan_credentials(directory, env):
    overlap = max(map(len, credential_values(env)), default=1) - 1
    files = 0
    total_bytes = 0
    for parent, directories, names in os.walk(directory, followlinks=False):
        for name in directories + names:
            artifact = Path(parent) / name
            if artifact.is_symlink():
                raise AssertionError("credential scan cannot attest a symlinked artifact")
        for name in names:
            artifact = Path(parent) / name
            require_credential_absence(os.fsencode(artifact), env, "artifact path")
            with artifact.open("rb") as source:
                carry = b""
                while chunk := source.read(1024 * 1024):
                    total_bytes += len(chunk)
                    require_credential_absence(carry + chunk, env, "durable artifact")
                    carry = (carry + chunk)[-overlap:] if overlap else b""
            files += 1
    return {"files": files, "bytes": total_bytes, "injected_credentials": len(credential_values(env))}


def default_offers():
    return [
        {"id": 1000 + index, "host_id": 2000 + index, "gpu_name": "RTX A2000",
         "dph_total": 0.05, "gpu_ram": 24000, "compute_cap": 860,
         "verification": "verified", "reliability2": 0.99,
         "inet_down": 1000.0, "inet_up": 1000.0,
         "internet_down_cost_per_tb": 0.0, "internet_up_cost_per_tb": 0.0}
        for index in range(1, 6)
    ]


def instance_status(status_mode, contract, label, ssh_endpoint=None):
    host, port = ssh_endpoint or ("", 0)
    return {"id": contract, "label": label, "actual_status": status_mode,
            "intended_status": "running", "public_ipaddr": host, "ssh_port": port,
            "status_msg": "scripted provider failed" if status_mode == "error" else "",
            "disk_usage": 0.0}


class State:
    def __init__(self, control_path, log_path):
        self.lock = threading.RLock()
        self.control_path = Path(control_path)
        self.log_path = Path(log_path)
        self.contracts = dict(UNRELATED_CONTRACTS)
        self.created = {}
        existing = self.control().get("initial_contracts", {})
        self.contracts.update({int(contract): label for contract, label in existing.items()})
        self.created.update({int(contract): label for contract, label in existing.items()})
        self.ssh_keys = []
        self.destroyed = set()
        self.pending = {}
        self.next_contract = 9000
        self.create_count = 0
        self.list_count = 0
        self.offer_count = 0
        self.next_listing = 0.0
        self.late_inserted = False
        self.delete_release = threading.Event()
        self.create_release = threading.Event()
        self.offer_release = threading.Event()
        self.sequence = 0

    def control(self):
        try:
            return json.loads(self.control_path.read_text())
        except FileNotFoundError:
            return {}

    def log(self, method, path, authorized, **fields):
        with self.lock:
            self.sequence += 1
            entry = {"sequence": self.sequence, "method": method, "path": path,
                     "authorized": authorized, "elapsed_ns": time.monotonic_ns(), **fields}
            with self.log_path.open("a") as handle:
                handle.write(json.dumps(entry) + "\n")
            return self.sequence

    def reveal(self, control):
        for contract, (label, after_list, after_time) in list(self.pending.items()):
            if self.list_count >= after_list and time.monotonic() >= after_time:
                self.contracts[contract] = label
                del self.pending[contract]
                self.log("EVENT", "contract-visible", True, contract_id=contract, label=label)


class Handler(BaseHTTPRequestHandler):
    state = None

    def log_message(self, *_args):
        pass

    def parse_request(self):
        if self.raw_requestline.startswith(b"SSH-"):
            self.state.log("SSH", "bootstrap-connection", True)
            self.state.create_release.wait()
            return False
        if not super().parse_request():
            return False
        if any(value in self.path.encode() for value in credential_values(os.environ)):
            self.state.log("LEAK", "request-target", False)
            self._reply(400, {"error": "credential in request target"})
            return False
        return True

    @property
    def authorized(self):
        expected = os.environ.get(KEY_ENV)
        return bool(expected) and self.headers.get("Authorization") == f"Bearer {expected}"

    def _reply(self, code, payload, **headers):
        if code >= 400 and self.state.control().get("credential_echo"):
            payload = dict(payload, diagnostic=os.environ[KEY_ENV])
            self.state.log("EVENT", "credential-echo", True, status=code)
        body = json.dumps(payload).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for name, value in headers.items():
                self.send_header(name.replace("_", "-"), str(value))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Deliberately cancelled requests are part of the cleanup scenario.
            pass

    def _body(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}"
        require_credential_absence(body, os.environ, "provider request body")
        return json.loads(body)

    def do_GET(self):
        state = self.state
        control = state.control()
        path = urlparse(self.path).path
        if not self.authorized:
            state.log("GET", path, False)
            return self._reply(401, {"error": "unauthorized"})
        if path == "/api/v0/bundles/":
            with state.lock:
                state.offer_count += 1
                offer_count = state.offer_count
                state.log("GET", path, True, offer_count=offer_count)
                offers = control.get("offers", default_offers())
            if offer_count > control.get("withhold_offer_responses_after", sys.maxsize):
                state.offer_release.wait()
            return self._reply(200, {"offers": offers})
        with state.lock:
            if path == "/api/v0/ssh/":
                state.log("GET", path, True)
                return self._reply(200, {"ssh_keys": [
                    {"id": index + 1, "public_key": key}
                    for index, key in enumerate(state.ssh_keys)]})
            if path == "/__scripted__/state":
                state.log("GET", path, True)
                return self._reply(200, {"contracts": state.contracts, "created": state.created,
                                         "pending": list(state.pending), "destroyed": sorted(state.destroyed)})
            if path == "/api/v0/instances/":
                state.list_count += 1
                now = time.monotonic()
                rate_limited = (
                    state.list_count <= control.get("rate_limit_listings", 0)
                    or now < state.next_listing
                )
                if rate_limited:
                    retry_after = control.get("retry_after", 1)
                    state.next_listing = max(state.next_listing, now + retry_after)
                    state.log("GET", path, True, status=429, retry_after=retry_after)
                    return self._reply(429, {"retry_after": retry_after}, Retry_After=retry_after)
                endpoint = ("127.0.0.1", self.server.server_port) if control.get("ssh_endpoint") else None
                instances = [instance_status(control.get("status_mode", "error"), contract, label, endpoint)
                             for contract, label in sorted(state.contracts.items())]
                state.log("GET", path, True, status=200,
                          returned_ids=sorted(state.contracts))
                return self._reply(200, {"instances": instances})
            parts = path.strip("/").split("/")
            try:
                contract = int(parts[3])
            except (IndexError, ValueError):
                state.log("GET", path, True, status=404)
                return self._reply(404, {"error": "unknown path"})
            state.reveal(control)
            label = state.contracts.get(contract)
            state.log("GET", path, True, status=200 if label else 404)
            if label is None:
                return self._reply(404, {"error": "not found"})
            endpoint = ("127.0.0.1", self.server.server_port) if control.get("ssh_endpoint") else None
            return self._reply(200, {"instances": instance_status(
                control.get("status_mode", "error"), contract, label, endpoint)})

    def do_PUT(self):
        state = self.state
        control = state.control()
        path = urlparse(self.path).path
        body = self._body()
        parts = path.strip("/").split("/")
        label = body.get("label")
        state.log("PUT", path, self.authorized, label=label,
                  image=body.get("image"), disk=body.get("disk"))
        if not self.authorized:
            return self._reply(401, {"error": "unauthorized"})
        if len(parts) != 4 or parts[2] != "asks":
            return self._reply(404, {"error": "unknown path"})
        mode = control.get("create_mode", "ok")
        if mode == "withheld-acceptance":
            state.create_release.wait()
        with state.lock:
            state.create_count += 1
            mode = control.get("create_mode", "ok")
            if mode == "reject":
                return self._reply(400, {"error": "no_such_ask"})
            contract = state.next_contract
            state.next_contract += 1
            state.created[contract] = label
            if mode == "ambiguous":
                state.pending[contract] = (label, state.list_count + control.get("reveal_after_listings", 2),
                                           time.monotonic() + control.get("create_visibility_delay", 0.05))
                return self._reply(503, {"error": "create reply lost after acceptance"})
            if mode == "withheld-response":
                state.pending[contract] = (label, state.list_count + 2, time.monotonic() + 0.1)
            else:
                state.contracts[contract] = label
            if state.create_count == 1:
                for offset in range(control.get("extra_owned_contracts", 0)):
                    extra = 9500 + offset
                    state.contracts[extra] = label
                    state.created[extra] = label
            state.log("EVENT", "create-accepted-before-response", True, contract_id=contract, label=label)
        if mode == "withheld-response":
            state.create_release.wait()
        return self._reply(200, {"new_contract": contract})

    def do_DELETE(self):
        state = self.state
        control = state.control()
        path = urlparse(self.path).path
        request_sequence = state.log("DELETE", path, self.authorized)
        if not self.authorized:
            return self._reply(401, {"error": "unauthorized"})
        try:
            contract = int(path.strip("/").split("/")[3])
        except (IndexError, ValueError):
            return self._reply(404, {"error": "unknown contract"})
        if contract == control.get("withhold_delete"):
            state.delete_release.wait()
        if contract == control.get("slow_delete"):
            time.sleep(control.get("slow_delete_seconds", 1.5))
        with state.lock:
            label = state.contracts.pop(contract, None)
            state.pending.pop(contract, None)
            state.destroyed.add(contract)
            if label and control.get("late_contract_after_delete") and not state.late_inserted:
                state.late_inserted = True
                late = control["late_contract_after_delete"]
                state.contracts[late] = label
                state.created[late] = label
            state.log("EVENT", "delete-completed", True, contract_id=contract,
                      request_sequence=request_sequence, existed=label is not None)
        return self._reply(200 if label is not None else 404, {"deleted": contract})

    def do_POST(self):
        state = self.state
        path = urlparse(self.path).path
        state.log("POST", path, self.authorized)
        if not self.authorized:
            return self._reply(401, {"error": "unauthorized"})
        body = self._body()
        with state.lock:
            if path == "/api/v0/ssh/":
                key = body["ssh_key"]
                if key not in state.ssh_keys:
                    state.ssh_keys.append(key)
                return self._reply(200, {"success": True, "key": {
                    "id": state.ssh_keys.index(key) + 1, "public_key": key}})
            if path == "/__scripted__/restore":
                for contract, label in body["contracts"].items():
                    contract = int(contract)
                    state.contracts[contract] = label
                    state.destroyed.discard(contract)
                state.delete_release.clear()
            elif path == "/__scripted__/release-deletes":
                state.delete_release.set()
            else:
                return self._reply(404, {"error": "unknown fault control"})
        return self._reply(200, {"applied": True})


def serve():
    Handler.state = State(sys.argv[2], sys.argv[3])
    server = ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler)
    server.daemon_threads = True
    if len(sys.argv) > 4:
        with os.fdopen(int(sys.argv[4]), "w") as ready:
            ready.write(json.dumps({"port": server.server_port, "pid": os.getpid()}) + "\n")
            ready.flush()
    server.serve_forever()


def run_process(command, directory, env, stdout_path, stderr_path, timeout=150):
    require_credential_absence(b"\0".join(os.fsencode(part) for part in command), env, "process arguments")
    started = time.monotonic()
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        process = subprocess.Popen(command, cwd=directory, env=env, stdout=stdout,
                                   stderr=stderr, start_new_session=True)
        try:
            status = process.wait(timeout=timeout)
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    return {"exit_status": status, "elapsed_seconds": time.monotonic() - started}


def crash_runner(command, root, env, directory, request_path, failure, boundary):
    require_credential_absence(b"\0".join(os.fsencode(part) for part in command), env, "process arguments")
    started = time.monotonic()
    with (directory / "stdout.log").open("w") as stdout, (directory / "stderr.log").open("w") as stderr:
        process = subprocess.Popen(command, cwd=root, env=env, stdout=stdout,
                                   stderr=stderr, start_new_session=True)
        try:
            until = time.monotonic() + 90
            while time.monotonic() < until:
                rows = [json.loads(line) for line in request_path.read_text().splitlines()] if request_path.exists() else []
                state_paths = list((directory / "campaign").glob("*/paid-state.json"))
                admission = None
                if len(state_paths) == 1:
                    paid = json.loads(state_paths[0].read_text())
                    admission_path = Path(paid["state_dir"]) / "paid-admission.json"
                    if admission_path.exists():
                        admission = json.loads(admission_path.read_text())
                creates = [row for row in rows
                           if row["method"] == "PUT" and row["path"].startswith("/api/v0/asks/")]
                accepted = sum(row["path"] == "create-accepted-before-response" for row in rows)
                offers = sum(row["path"] == "/api/v0/bundles/" for row in rows)
                reached = admission is not None and {
                    "admission-before-create-reservation":
                        offers >= 3 and not admission["create_reservations"] and not admission["contracts"],
                    "create-reserved-before-provider-acceptance":
                        len(creates) == 5 and len(admission["create_reservations"]) == 5
                        and accepted == 0 and not admission["contracts"],
                    "provider-accepted-before-response-accounting":
                        accepted == 5 and len(admission["create_reservations"]) == 5
                        and not admission["contracts"],
                    "contract-accounted-before-bootstrap":
                        len(admission["contracts"]) == 5 and not admission["initial_bootstraps"],
                    "bootstrap-reserved-before-prepared-commit":
                        len(admission["initial_bootstraps"]) == 5
                        and admission["mode"] == "preparing",
                }[boundary]
                if reached:
                    break
                assert process.poll() is None, f"runner exited before {boundary}"
                time.sleep(0.02)
            else:
                raise AssertionError(f"{boundary} was not observed before runner kill")
            assert len(state_paths) == 1 and admission is not None
            for role in ("runner", "orchestrator", "owner", "supervisor"):
                identity = admission["cleanup"][role]
                assert identity, f"missing admitted {role} identity"
                require_credential_absence(
                    Path(f"/proc/{identity['pid']}/cmdline").read_bytes(), env, f"{role} arguments")
            if failure == "workstation-loss":
                # The provider is outside the failed workstation. Stop its local
                # supervision first so no owner can be restarted during the crash.
                signal_cleanup_process(admission_path, "supervisor", signal.SIGKILL)
            if failure in ("owner-loss-automatic", "workstation-loss"):
                signal_cleanup_process(admission_path, "owner", signal.SIGKILL)
            if failure in ("orchestrator-loss", "workstation-loss"):
                signal_cleanup_process(admission_path, "orchestrator", signal.SIGKILL)
            if failure == "orchestrator-loss":
                process.wait(timeout=60)
            else:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
            before = state_paths[0].read_bytes()
            repeated = run_process(command, root, env, directory / "repeat.stdout.log", directory / "repeat.stderr.log")
            assert repeated["exit_status"] != 0, "repeated paid campaign overwrote ownership"
            assert state_paths[0].read_bytes() == before, "refusal modified original paid ownership"
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    return {"exit_status": process.returncode, "elapsed_seconds": time.monotonic() - started,
            "crash_boundary": boundary,
            "runner_killed_before_accounting": failure != "orchestrator-loss",
            "cleanup_owner_killed": failure in ("owner-loss-automatic", "workstation-loss"),
            "original_cleanup": admission["cleanup"],
            "original_contracts": admission["contracts"],
            "original_initial_bootstraps": admission["initial_bootstraps"],
            "original_create_reservations": admission["create_reservations"],
            "post_crash_sequence": rows[-1]["sequence"]}


def signal_cleanup_process(admission_path, role, sig):
    identity = json.loads(admission_path.read_text())["cleanup"][role]
    assert identity, f"missing cleanup {role} identity"
    pidfd = os.pidfd_open(identity["pid"])
    try:
        proc = Path(f"/proc/{identity['pid']}")
        fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
        command = (proc / "cmdline").read_bytes().split(b"\0")
        assert int(fields[19]) == identity["start_ticks"], "cleanup process incarnation changed"
        assert Path("/proc/sys/kernel/random/boot_id").read_text().strip() == identity["boot_id"]
        if role == "orchestrator":
            assert Path(os.fsdecode(command[0])).name == "myelin-orchestrator"
            assert str(admission_path.parent).encode() in command
        else:
            assert f"--paid-cleanup-{role}".encode() in command and str(admission_path).encode() in command
        signal.pidfd_send_signal(pidfd, sig)
        assert select.select([pidfd], [], [], 5)[0], f"{role} crash did not stop the admitted incarnation"
    finally:
        os.close(pidfd)
    return identity


def stop_scripted_cleanup_owners(directory):
    paths = [directory / "private" / "paid-admission.json"]
    for paid_path in (directory / "campaign").glob("*/paid-state.json"):
        paid = json.loads(paid_path.read_text())
        paths.append(Path(paid["state_dir"]) / "paid-admission.json")
    for admission_path in paths:
        if not admission_path.exists():
            continue
        cleanup = json.loads(admission_path.read_text()).get("cleanup") or {}
        spending_stopped = cleanup.get("spending_stopped", False)
        # A completed worker still belongs to its supervisor's child.wait().
        # Let that supervisor reap and exit naturally before forced teardown.
        roles = ("owner", "supervisor") if spending_stopped else ("supervisor", "owner")
        for role in roles:
            identity = (json.loads(admission_path.read_text()).get("cleanup") or {}).get(role)
            if not identity:
                continue
            pidfd = None
            try:
                pidfd = os.pidfd_open(identity["pid"])
                if select.select([pidfd], [], [], 0)[0]:
                    continue
                proc = Path(f"/proc/{identity['pid']}")
                fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
                if fields[0] in ("Z", "X"):
                    continue
                assert int(fields[19]) == identity["start_ticks"], "cleanup process incarnation changed"
                assert Path("/proc/sys/kernel/random/boot_id").read_text().strip() == identity["boot_id"]
                if spending_stopped and select.select([pidfd], [], [], 5)[0]:
                    continue
                command = (proc / "cmdline").read_bytes().split(b"\0")
                # Exit can race both the stat and cmdline reads. Empty argv
                # alone is not proof of exit and must never authorize a signal.
                if select.select([pidfd], [], [], 0)[0]:
                    continue
                fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
                if fields[0] in ("Z", "X"):
                    continue
                if not any(command) and select.select([pidfd], [], [], 5)[0]:
                    continue
                assert int(fields[19]) == identity["start_ticks"], "cleanup process incarnation changed"
                assert f"--paid-cleanup-{role}".encode() in command and str(admission_path).encode() in command
                signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                assert select.select([pidfd], [], [], 5)[0], f"cleanup {role} did not stop"
            except (FileNotFoundError, ProcessLookupError):
                pass
            finally:
                if pidfd is not None:
                    os.close(pidfd)


def retained_owner_loss(binary, root, env, directory, request, ledger):
    private = directory / "private"
    private.mkdir(mode=0o700)
    admission_path = private / "paid-admission.json"
    command = [str(binary), "--scripted-retained-lifecycle", str(admission_path),
               "--api-key-env", KEY_ENV]
    initial = run_process(command, root, env, directory / "prepare.stdout.log", directory / "prepare.stderr.log")
    assert initial["exit_status"] == 0, "scripted retained admission did not prepare"
    before = json.loads(admission_path.read_text())
    assert before["mode"] == "prepared" and before["cleanup"]["retained_development_fixture"]
    resumed = run_process(command, root, env, directory / "redeploy.stdout.log", directory / "redeploy.stderr.log")
    assert resumed["exit_status"] == 0, "explicit retained generation was not admitted"
    retained = json.loads(admission_path.read_text())
    grant = retained["retained_deployments"]["scripted-retained-generation"]
    assert set(grant["bootstraps"]) == set(retained["contracts"]), "generation did not cover exact contracts"
    for field in ("contracts", "create_reservations", "initial_bootstraps"):
        assert retained[field] == before[field], f"redeployment changed {field}"
    for field in ("limits", "started_unix_ms", "stop_unix_ms", "supervisor"):
        assert retained["cleanup"][field] == before["cleanup"][field], f"redeployment changed original {field}"
    # Both foreground processes have exited; no orchestrator/runner can issue
    # manual cleanup. Only the independently supervised worker can delete.
    for role in ("runner", "orchestrator"):
        process = retained["cleanup"][role]
        proc = Path(f"/proc/{process['pid']}/stat")
        if proc.exists():
            fields = proc.read_text().rsplit(") ", 1)[1].split()
            assert int(fields[19]) != process["start_ticks"] or fields[0] in ("Z", "X")
    prior_sequence = ledger()[-1]["sequence"] if ledger() else 0
    killed = signal_cleanup_process(admission_path, "owner", signal.SIGKILL)
    until = time.monotonic() + 30
    while time.monotonic() < until:
        after = json.loads(admission_path.read_text())
        state = request("/__scripted__/state")
        if (after["cleanup"]["complete"] and after["cleanup"]["spending_stopped"]
                and state["contracts"] == UNRELATED_WIRE_CONTRACTS and not state["pending"]):
            break
        time.sleep(0.05)
    else:
        raise AssertionError("owner loss did not automatically stop retained spending")
    assert after["cleanup"]["owner"] != killed, "owner was not restarted"
    assert after["mode"] == "cleanup_only", "restart reopened paid permission"
    for field in ("limits", "started_unix_ms", "stop_unix_ms", "supervisor"):
        assert after["cleanup"][field] == before["cleanup"][field], f"owner restart changed {field}"
    for field in ("contracts", "create_reservations", "initial_bootstraps", "retained_deployments"):
        assert after[field] == retained[field], f"owner restart changed {field}"
    ceiling_ms = min(before["deadline_unix_ms"], before["cleanup"]["stop_unix_ms"] + 300_000)
    assert time.time_ns() // 1_000_000 < ceiling_ms, "original ceiling was exceeded"
    rows = ledger()
    assert all(row["authorized"] for row in rows), "unauthenticated lifecycle request"
    assert not any(row["method"] == "PUT" or row["path"] == "/api/v0/bundles/" for row in rows), "retained lifecycle searched or acquired"
    assert not any(row["method"] == "DELETE"
                   and int(row["path"].rstrip("/").split("/")[-1]) in UNRELATED_CONTRACTS
                   for row in rows if row["path"].startswith("/api/v0/instances/"))
    known = set(map(int, retained["contracts"].values()))
    listings = [row for row in rows if row["sequence"] > prior_sequence
                and row["path"] == "/api/v0/instances/" and row.get("status") == 200]
    assert listings and not known.intersection(listings[-1]["returned_ids"]), "missing typed exact absence after owner loss"
    assert known <= set(state["destroyed"]), "cleanup omitted an exact retained identity"
    (directory / "provider-final-state.json").write_text(json.dumps(state, indent=2))
    return {"exit_status": 0, "creates": 0, "retained_redeployment_admitted": True,
            "crash_boundary": "prepared-commit-cleanup-owner-loss",
            "initial_reservations_unchanged": True, "automatic_owner_restart": True,
            "acquisition_slots": len(after["create_reservations"]),
            "initial_bootstrap_slots": len(after["initial_bootstraps"]),
            "manual_cleanup_commands": 0, "original_ceilings_preserved": True,
            "created_contract_ids": [], "existing_contract_ids": sorted(known),
            "destroyed_contract_ids": sorted(state["destroyed"]), "exact_absence": True,
            "unrelated_preserved": True, "scope": "admission-and-real-cleanup-processes"}


def gate(root, output, binary):
    if not __debug__:
        raise RuntimeError("scripted safety proofs require Python assertions; optimization is forbidden")
    gate_started = time.monotonic()
    root, output, binary = Path(root).resolve(), Path(output).resolve(), Path(binary).resolve()
    from ordered_acceptance import ROOT, TEST_COMMAND, digest, source_digest

    assert root == ROOT, "gate fixtures must hash the harness workspace"
    rejection_build_artifacts = {
        name: digest(root / "target/release" / name)
        for name in ("myelin-e2e-fuzz", "myelin-orchestrator", "myelin-worker")
    }
    assert digest(binary) == rejection_build_artifacts["myelin-e2e-fuzz"], "scripted gate binary is not the qualified executable"
    rejection_source_digest = source_digest()
    rejection_image = "registry.invalid/myelin-node@sha256:" + "0" * 64
    rejected_gates = {
        "missing-gates": "paid execution requires --gate-attestation from ordered_acceptance.py; operator authorization is not gate evidence",
        "stale-gates": "paid gate attestation is missing, incomplete, stale, or incompatible",
        "failed-gates": "paid gate stages failed or were not executed in order",
        "out-of-order-gates": "missing, duplicate or out-of-order required gate warm-1-campaign",
        "changed-binaries": "paid gate evidence does not attest the current binaries, sources and image",
    }
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, **{name: "scripted-" + secrets.token_hex(24) for name in CREDENTIAL_ENVS})
    marker = env[KEY_ENV]
    key = output / "dummy_ed25519"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key),
                    "-C", "scripted-safety"], check=True)
    duplicate_offers = default_offers()
    for offer in duplicate_offers:
        offer["host_id"] = 2001
    malformed_offers = default_offers()
    malformed_offers[0]["dph_total"] = -1.0
    priced_offers = list(reversed(default_offers()))
    priced_offers += [
        dict(default_offers()[0], id=1101, dph_total=0.01),
        dict(default_offers()[0], id=1102, host_id=2102, dph_total=0.001, verification="unverified"),
        dict(default_offers()[0], id=1103, host_id=59017, dph_total=0.001),
        dict(default_offers()[0], id=1104, host_id=2104, dph_total=0.08),
    ]
    preflight = {
        "zero-cost": ("--max-total-cost-usd", "0", "explicit positive paid-run ceiling"),
        "invalid-cost": ("--max-hourly-cost-usd", "NaN", "must be finite and non-negative"),
        "zero-lifetime": ("--fixture-lifetime-secs", "0", "resource bounds must be nonzero"),
        "short-lifetime": ("--fixture-lifetime-secs", "1199", "planned campaign requires"),
        "empty-image": ("--image", " ", "runtime image must not be empty"),
        "missing-credential": (None, None, "credential environment"),
    }
    provider_admissions = {
        "selected-cost-over-budget": ("--max-total-cost-usd", "1", "selected worst-case cost"),
    }
    crash_scenarios = {
        "runner-loss": ("runner-loss", "provider-accepted-before-response-accounting"),
        "owner-loss-automatic": ("owner-loss-automatic", "provider-accepted-before-response-accounting"),
        "orchestrator-loss": ("orchestrator-loss", "provider-accepted-before-response-accounting"),
        "workstation-loss": ("workstation-loss", "provider-accepted-before-response-accounting"),
        "crash-before-create-reservation": ("runner-loss", "admission-before-create-reservation"),
        "crash-after-create-reservation": ("runner-loss", "create-reserved-before-provider-acceptance"),
        "crash-after-contract-accounting": ("runner-loss", "contract-accounted-before-bootstrap"),
        "crash-after-bootstrap-reservation": ("runner-loss", "bootstrap-reserved-before-prepared-commit"),
    }
    scenarios = {
        "missing-gates": {},
        "stale-gates": {},
        "failed-gates": {},
        "out-of-order-gates": {},
        "changed-binaries": {},
        **{name: {} for name in preflight},
        "campaign-resource-overflow": {},
        **{name: {} for name in provider_admissions},
        "runner-loss": {"create_mode": "withheld-response"},
        "owner-loss-automatic": {"create_mode": "withheld-response"},
        "orchestrator-loss": {"create_mode": "withheld-response"},
        "workstation-loss": {"create_mode": "withheld-response"},
        "crash-before-create-reservation": {"withhold_offer_responses_after": 2},
        "crash-after-create-reservation": {"create_mode": "withheld-acceptance"},
        "crash-after-contract-accounting": {"status_mode": "loading"},
        "crash-after-bootstrap-reservation": {"status_mode": "running", "ssh_endpoint": True},
        "retained-owner-loss": {"initial_contracts": {
            str(8999 + node): f"scripted-retained-42-{node}-attempt-0" for node in range(1, 6)}},
        "duplicate-hosts": {"offers": duplicate_offers},
        "insufficient-offers": {"offers": []},
        "malformed-offers": {"offers": malformed_offers},
        "cheapest-distinct-verified": {"offers": priced_offers},
        "create-rejected": {"create_mode": "reject", "credential_echo": True},
        "rate-limited-listing": {"create_mode": "reject", "rate_limit_listings": 1, "retry_after": 1},
        "delayed-ambiguous-create": {"create_mode": "ambiguous", "reveal_after_listings": 2,
                                     "create_visibility_delay": 0},
        "late-contract": {"status_mode": "running", "late_contract_after_delete": 9700,
                          "extra_owned_contracts": 2},
        "slow-delete": {"slow_delete": 9000, "slow_delete_seconds": 1.5, "extra_owned_contracts": 2,
                         "late_contract_after_delete": 9700},
        "cleanup-only-recovery": {"extra_owned_contracts": 2},
    }
    summary = {"schema_version": 3, "state": "running", "scenarios": {},
               "provider": "loopback-scripted", "paid_resources": 0}
    for name, control in scenarios.items():
        directory = output / name
        directory.mkdir(parents=True, exist_ok=False)
        control_path = directory / "control.json"
        control_path.write_text(json.dumps(control))
        request_path = directory / "requests.jsonl"
        ready_read, ready_write = os.pipe()
        provider_log = (directory / "provider.log").open("w")
        provider = subprocess.Popen([sys.executable, "-E", "-B", __file__, "0", str(control_path), str(request_path),
                                     str(ready_write)], env=env, pass_fds=(ready_write,),
                                    stdout=provider_log, stderr=provider_log)
        os.close(ready_write)
        try:
            assert select.select([ready_read], [], [], 10)[0], "provider did not signal readiness"
            ready = json.loads(os.read(ready_read, 4096))
            assert ready["pid"] == provider.pid and provider.poll() is None
            url = f"http://127.0.0.1:{ready['port']}"
            scenario_env = dict(env, VASTAI_BASE_URL=url,
                                XDG_STATE_HOME=str(directory / "private-state"))

            def request(path, body=None):
                data = None if body is None else json.dumps(body).encode()
                req = urllib.request.Request(url + path, data=data,
                                             headers={"Authorization": f"Bearer {marker}", "Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5) as response:
                    return json.load(response)

            def ledger():
                return [json.loads(line) for line in request_path.read_text().splitlines()] if request_path.exists() else []

            if name == "retained-owner-loss":
                summary["scenarios"][name] = retained_owner_loss(
                    binary, root, scenario_env, directory, request, ledger)
                (output / "scripted-safety-summary.json").write_text(json.dumps(summary, indent=2))
                continue

            command = [str(binary), "--paid-vastai", "--authorize-paid-vastai", "--scripted-provider",
                       "--seed", "20260910", "--fixture-lifetime-secs", "43200", "--max-total-cost-usd", "5",
                       "--max-hourly-cost-usd", "0.5", "--ssh-identity", str(key),
                       "--image", "myelin-node:latest", "--api-key-env", KEY_ENV,
                       "--deadline-secs", "5", "--no-build-image", "--artifacts", str(directory / "campaign")]
            # These fixtures reject before runtime-image or deployment-artifact inspection.
            if name in rejected_gates:
                command.remove("--scripted-provider")
                command[command.index("--image") + 1] = rejection_image
                if name != "missing-gates":
                    now = time.time_ns() // 1_000_000
                    stages = []
                    qualified_binary = str(root / "target/release/myelin-e2e-fuzz")
                    qualified_artifacts = directory / "qualified-local-gates"

                    def stage(stage_name, argv):
                        stages.append({"name": stage_name, "command": argv,
                                       "state": "passed", "exit_code": 0, "elapsed_secs": 0.001,
                                       "started_unix_ms": now - 1000 + len(stages) * 2,
                                       "completed_unix_ms": now - 999 + len(stages) * 2})

                    stage("build", ["cargo", "build", "--release", "-p", "myelin",
                                    "--bins", "-p", "myelin-e2e-fuzz"])
                    stage("build-test-binaries", TEST_COMMAND + ["--no-run"])
                    stage("build-deployment-artifacts",
                          [qualified_binary, "--prepare-artifacts", "--deadline-secs", "3600",
                           "--artifacts", str(qualified_artifacts / "build-deployment-artifacts")])
                    for index in range(1, 4):
                        common = [qualified_binary, "--seed", "20260910", "--deadline-secs",
                                  "120", "--no-build-image", "--image", rejection_image]
                        for suffix, flags in [
                                ("gate-a", ["--deployment-e2e", "--deployment-nodes", "5"]),
                                ("campaign", ["--campaign", "--fixture-lifetime-secs", "43200"]),
                                ("failure-cases", ["--nodes", "5", "--failure-cases"])]:
                            stage(f"warm-{index}-{suffix}", common + flags + ["--artifacts",
                                  str(qualified_artifacts / f"warm-{index}" / suffix)])
                        stage(f"warm-{index}-contract-model-safety", TEST_COMMAND)
                        stage(f"warm-{index}-scripted-provider",
                              ["bash", "tools/myelin-e2e-fuzz/scripted_safety_gate.sh",
                               str(qualified_artifacts / f"warm-{index}" / "scripted-provider")])
                    stage("verify-qualified-deployment-artifacts",
                          [qualified_binary, "--prepare-artifacts", "--deadline-secs", "120",
                           "--artifacts", str(qualified_artifacts / "verify-qualified-deployment-artifacts")])
                    # Synthetic image/deployment identities satisfy the schema, not paid
                    # qualification. Only the intended guard below may reject each fixture.
                    evidence = {"schema_version": 5, "state": "passed", "completed_unix_ms": now - 2,
                                "expires_unix_ms": now + 60_000,
                                "build_elapsed_secs": 0.02, "elapsed_secs": 0.1,
                                "build_artifacts": dict(rejection_build_artifacts),
                                "deployment_artifacts": {
                                    "schema_version": 2, "source_build_input_digest": "0" * 64,
                                    "executables": dict(rejection_build_artifacts),
                                    "wheel_input_digest": "0" * 64, "wheels": {}, "payload": {}},
                                "source_digest": rejection_source_digest,
                                "image_identities": {
                                    image: {"id": "sha256:" + str(index) * 64, "os": "linux",
                                            "architecture": "amd64", "variant": "",
                                            "repo_digests": [rejection_image],
                                            "provenance_version": 1,
                                            "source_build_input_digest": "0" * 64,
                                            "image_role": role,
                                            "parent_image_id": None if index == 0 else "sha256:" + str(index - 1) * 64}
                                    for index, (image, role) in enumerate([
                                        ("myelin-node-base:cuda12.6", "base"),
                                        ("myelin-node:latest", "node"), (rejection_image, "e2e")])},
                                "configuration": {"warm_runs": 3, "image": rejection_image,
                                                  "seed": 20260910, "case_deadline_secs": 120,
                                                  "build_images": False, "workspace": str(root),
                                                  "artifacts": str(qualified_artifacts)},
                                "stages": stages, "runs": [{"index": index, "state": "passed", "gate_a": "passed",
                                                          "inside_target": True, "campaign_elapsed_secs": 0.001,
                                                          "elapsed_secs": 0.01} for index in range(1, 4)]}
                    if name == "stale-gates":
                        evidence["expires_unix_ms"] = now - 1
                    elif name == "failed-gates":
                        stages[1]["state"], stages[1]["exit_code"] = "failed", 1
                    elif name == "out-of-order-gates":
                        stages[4]["name"], stages[5]["name"] = stages[5]["name"], stages[4]["name"]
                    elif name == "changed-binaries":
                        tested = evidence["build_artifacts"]["myelin-e2e-fuzz"]
                        evidence["build_artifacts"]["myelin-e2e-fuzz"] = (
                            ("1" if tested[0] == "0" else "0") + tested[1:])
                    attestation_path = directory / "rejected-attestation.json"
                    attestation_path.write_text(json.dumps(evidence))
                    command += ["--gate-attestation", str(attestation_path)]
            if name in preflight:
                flag, value, _ = preflight[name]
                if flag is not None:
                    command[command.index(flag) + 1] = value
                else:
                    scenario_env.pop(KEY_ENV)
            if name in provider_admissions:
                flag, value, _ = provider_admissions[name]
                command[command.index(flag) + 1] = value
            if name == "campaign-resource-overflow":
                command.append("--scripted-campaign-resource-overflow")
            if name in crash_scenarios:
                failure, boundary = crash_scenarios[name]
                result = crash_runner(
                    command, root, scenario_env, directory, request_path, failure, boundary)
            else:
                result = run_process(command, root, scenario_env, directory / "stdout.log", directory / "stderr.log")
            assert result["exit_status"] != 0, f"{name}: fault scenario incorrectly passed"
            rows = ledger()
            creates = [row for row in rows
                       if row["method"] == "PUT" and row["path"].startswith("/api/v0/asks/")]
            assert all(row["authorized"] for row in rows), f"{name}: unauthenticated provider work"
            assert len(creates) <= 5, f"{name}: exceeded five admitted acquisition attempts"
            assert len({row["path"] for row in creates}) == len(creates), f"{name}: create retried for an admitted offer"
            if (name in rejected_gates or name in preflight or name in provider_admissions
                    or name == "campaign-resource-overflow"
                    or name in ("duplicate-hosts", "insufficient-offers", "malformed-offers",
                                "crash-before-create-reservation")):
                assert not creates, f"{name}: rejected or pre-create scenario acquired contracts"
            else:
                assert creates, f"{name}: scenario never exercised admitted acquisition"
            if name in rejected_gates:
                assert not rows, "rejected gate evidence reached the provider"
                expected_guard = rejected_gates[name]
                stderr = (directory / "stderr.log").read_text()
                assert f"myelin-e2e-behavioral-fuzz: {expected_guard}" in stderr.splitlines(), (
                    f"{name}: did not reach intended rejection guard {expected_guard!r}")
                result.update({"rejection_guard": expected_guard, "provider_requests_before_rejection": 0})
            if name in preflight:
                assert not rows, f"{name}: admission failure reached provider"
                assert preflight[name][2] in (directory / "stderr.log").read_text(), f"{name}: wrong preflight failure"
                result["provider_requests_before_rejection"] = 0
            if name in provider_admissions:
                assert any(row["path"] == "/api/v0/bundles/" for row in rows), f"{name}: offer-cost admission was not exercised"
                assert provider_admissions[name][2] in (directory / "stderr.log").read_text(), f"{name}: wrong provider admission failure"
                result["provider_requests_before_rejection"] = len(rows)
            if name == "campaign-resource-overflow":
                assert not rows, f"{name}: campaign resource admission reached provider"
                assert "campaign resources exceed the hard admission ceilings" in (
                    directory / "stderr.log").read_text(), f"{name}: wrong campaign resource admission failure"
                result["provider_requests_before_rejection"] = 0
            if name == "malformed-offers":
                assert any(row["path"] == "/api/v0/bundles/" for row in rows), "malformed offer response was not exercised"
            if control.get("credential_echo"):
                assert any(row["path"] == "credential-echo" for row in rows), "credential echo was not exercised"
            state_paths = list((directory / "campaign").rglob("paid-state.json"))
            paid_path = state_paths[0] if state_paths else None
            if creates or name in crash_scenarios:
                assert len(state_paths) == 1, f"{name}: missing unique durable cleanup state"
                paid = json.loads(paid_path.read_text())
                if name not in crash_scenarios:
                    assert paid["phase"] not in ("planned", "acquiring", "prepared"), f"{name}: acquisition remained authorized"
                assert all(row["label"] in paid["owned_labels"] for row in creates), f"{name}: unauthorized owned label"
                assert all(int(row["path"].rstrip("/").split("/")[-1]) in paid["selected_offer_ids"]
                           for row in creates), f"{name}: unselected offer acquired"
                if name == "cheapest-distinct-verified":
                    assert paid["selected_offer_ids"] == [1101, 1002, 1003, 1004, 1005], "price/verification/distinct-host policy selected the wrong offers"
                    result["selected_offer_ids"] = paid["selected_offer_ids"]

            def cleanup(suffix, seconds=15):
                before = ledger()[-1]["sequence"] if ledger() else 0
                outcome = run_process([str(binary), "--cleanup-only", str(paid_path),
                                       "--deadline-secs", str(seconds), "--api-key-env", KEY_ENV,
                                       "--artifacts", str(directory / "cleanup-artifacts")],
                                      root, scenario_env, directory / f"{suffix}.stdout.log",
                                      directory / f"{suffix}.stderr.log", timeout=seconds + 15)
                new_rows = [row for row in ledger() if row["sequence"] > before]
                assert not any(row["method"] == "PUT" or row["path"] == "/api/v0/bundles/"
                               for row in new_rows), f"{name}: cleanup-only searched or acquired"
                return outcome, new_rows

            if name == "workstation-loss":
                recovered, _ = cleanup("workstation-recovery")
                assert recovered["exit_status"] == 0, "workstation cleanup-only recovery failed"
                recovered_admission = json.loads((Path(paid["state_dir"]) / "paid-admission.json").read_text())
                assert recovered_admission["mode"] == "cleanup_only"
                for field in ("limits", "started_unix_ms", "stop_unix_ms"):
                    assert recovered_admission["cleanup"][field] == result["original_cleanup"][field], "workstation recovery renewed cleanup ceilings"
                result["cleanup_only"] = recovered
                result["manual_cleanup_commands"] = 1

            if name in set(crash_scenarios) - {"workstation-loss"}:
                admission_path = Path(paid["state_dir"]) / "paid-admission.json"
                until = time.monotonic() + 30
                while time.monotonic() < until:
                    state = request("/__scripted__/state")
                    admission = json.loads(admission_path.read_text())
                    if (state["contracts"] == UNRELATED_WIRE_CONTRACTS
                            and not state["pending"] and admission["cleanup"]["complete"]):
                        break
                    time.sleep(0.05)
                else:
                    raise AssertionError("independent supervision did not automatically stop spending")
                for field in ("limits", "started_unix_ms", "stop_unix_ms", "supervisor"):
                    assert admission["cleanup"][field] == result["original_cleanup"][field]
                assert admission["mode"] == "cleanup_only"
                if name == "owner-loss-automatic":
                    assert admission["cleanup"]["owner"] != result["original_cleanup"]["owner"]
                result["automatic_cleanup"] = True
                result["manual_cleanup_commands"] = 0

            if name in crash_scenarios:
                restored = json.loads((Path(paid["state_dir"]) / "paid-admission.json").read_text())
                assert restored["initial_bootstraps"] == result["original_initial_bootstraps"], "crash recovery resumed initial bootstrap"
                assert restored["create_reservations"] == result["original_create_reservations"], "crash recovery changed acquisition reservations"
                assert restored["contracts"] == result["original_contracts"], "crash recovery changed contract accounting"
                result["acquisition_slots"] = len(restored["create_reservations"])
                result["initial_bootstrap_slots"] = len(restored["initial_bootstraps"])
                assert not any(row["sequence"] > result["post_crash_sequence"]
                               and (row["method"] == "PUT" or row["path"] == "/api/v0/bundles/")
                               for row in ledger()), "crash recovery searched or acquired"

            if name == "delayed-ambiguous-create":
                initial = request("/__scripted__/state")
                if initial["pending"] or len(initial["contracts"]) > 1:
                    assert json.loads(paid_path.read_text())["phase"] == "cleanup_only", "late create obligation was abandoned"
                recovery, _ = cleanup("ambiguous-recovery")
                result["cleanup_only"] = recovery
                assert recovery["exit_status"] == 0, "observed ambiguous identities failed cleanup-only recovery"
                assert any(row["path"] == "contract-visible" for row in ledger()), "delayed discovery was not exercised"

            if name == "cleanup-only-recovery":
                prior = request("/__scripted__/state")
                assert len(prior["created"]) >= 3, "independent cleanup siblings were not constructed"
                control["withhold_delete"] = 9000
                control_path.write_text(json.dumps(control))
                request("/__scripted__/restore", {"contracts": prior["created"]})
                failed, cleanup_rows = cleanup("withheld-delete", seconds=1)
                assert failed["exit_status"] != 0 and failed["elapsed_seconds"] < 5, "withheld deletion did not yield bounded failure"
                assert json.loads(paid_path.read_text())["phase"] == "cleanup_only", "foreground expiry abandoned durable cleanup"
                pending = request("/__scripted__/state")
                assert set(map(int, pending["contracts"])) == set(UNRELATED_CONTRACTS) | {9000}, "withheld delete blocked healthy cleanup"
                assert any(row["path"] == "delete-completed" and row["contract_id"] != 9000 for row in cleanup_rows)
                request("/__scripted__/release-deletes", {})
                recovered, recovery_rows = cleanup("cleanup-recovery")
                assert recovered["exit_status"] != 0, "duplicate-label accounting must remain a failure after spending stops"
                assert not any(row["method"] == "PUT" or row["path"] == "/api/v0/bundles/"
                               for row in recovery_rows), "cleanup-only recovery reopened acquisition"
                result["withheld_delete"] = failed
                result["cleanup_only"] = recovered

            final = request("/__scripted__/state")
            (directory / "provider-final-state.json").write_text(json.dumps(final, indent=2))
            assert final["contracts"] == UNRELATED_WIRE_CONTRACTS and not final["pending"], f"{name}: exact attributable absence not proved"
            rows = ledger()
            assert not any(row["method"] == "DELETE"
                           and int(row["path"].rstrip("/").split("/")[-1]) in UNRELATED_CONTRACTS
                           for row in rows if row["path"].startswith("/api/v0/instances/")), f"{name}: unrelated resource touched"
            created_ids = set(map(int, final["created"]))
            if created_ids:
                successful_listings = [row for row in rows if row["path"] == "/api/v0/instances/" and row.get("status") == 200]
                assert successful_listings and not created_ids.intersection(successful_listings[-1]["returned_ids"]), "missing final typed provider absence census"
                assert created_ids <= set(final["destroyed"]), "destroy accounting omitted an attributable identity"
            if name == "rate-limited-listing":
                rate_limited = [row for row in rows if row.get("status") == 429]
                assert rate_limited, "listing rate limit not exercised"
                for limited in rate_limited:
                    subsequent = next(row for row in rows if row["sequence"] > limited["sequence"]
                                      and row["path"] == "/api/v0/instances/")
                    assert subsequent["elapsed_ns"] - limited["elapsed_ns"] >= limited["retry_after"] * 1e9, "Retry-After was violated"
            if name == "slow-delete":
                completions = [row for row in rows if row["path"] == "delete-completed"]
                slow = next(row for row in completions if row["contract_id"] == 9000)
                healthy = [row for row in completions if row["contract_id"] in (9500, 9501)]
                assert len(healthy) == 2 and all(row["elapsed_ns"] < slow["elapsed_ns"] for row in healthy), "slow delete serialized healthy deletion"
                late = next(row for row in completions if row["contract_id"] == 9700)
                assert late["elapsed_ns"] < slow["elapsed_ns"], "pending delete blocked late-contract discovery"
            for application_root in (directory / "campaign", directory / "private-state", directory / "private"):
                for artifact in application_root.rglob("*"):
                    if artifact.is_file():
                        content = artifact.read_bytes()
                        assert not any(label.encode() in content for label in UNRELATED_CONTRACTS.values()), "application persisted unrelated account data"
            result.update({"creates": len(creates), "created_contract_ids": sorted(created_ids),
                           "destroyed_contract_ids": sorted(final["destroyed"]), "exact_absence": True,
                           "unrelated_preserved": True,
                           "listing_requests": sum(row["path"] == "/api/v0/instances/" for row in rows)})
            summary["scenarios"][name] = result
            (output / "scripted-safety-summary.json").write_text(json.dumps(summary, indent=2))
        except BaseException as error:
            summary["state"] = "failed"
            summary["failed_scenario"] = name
            detail = str(error)
            for value in credential_values(env):
                detail = detail.replace(value.decode(), "[REDACTED]")
            summary["error"] = detail
            raise
        finally:
            try:
                stop_scripted_cleanup_owners(directory)
            finally:
                os.close(ready_read)
                provider.terminate()
                try:
                    provider.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    provider.kill()
                    provider.wait()
                provider_log.close()
                try:
                    scanned = scan_credentials(directory, env)
                    summary["scenarios"].setdefault(name, {})["credential_scan"] = scanned
                except BaseException:
                    summary["state"] = "failed"
                    summary["failed_scenario"] = name
                    summary["credential_absence"] = False
                    raise
                finally:
                    summary["elapsed_seconds"] = time.monotonic() - gate_started
                    (output / "scripted-safety-summary.json").write_text(json.dumps(summary, indent=2))
    summary["credential_scan"] = scan_credentials(output, env)
    summary["gate_b_coverage"] = {
        "4_offer_selection": [
            "duplicate-hosts", "insufficient-offers", "malformed-offers",
            "cheapest-distinct-verified",
        ],
        "5_admission": [
            *preflight, *provider_admissions, "campaign-resource-overflow",
        ],
        "6_shared_acquisition_bound": [
            *crash_scenarios, "create-rejected", "delayed-ambiguous-create",
            "retained-owner-loss",
        ],
        "7_bootstrap_and_redeployment_bound": [
            "crash-after-bootstrap-reservation", "retained-owner-loss",
        ],
        "8_preparation_crash_accounting": [
            "crash-before-create-reservation", "crash-after-create-reservation",
            "runner-loss", "crash-after-contract-accounting",
            "crash-after-bootstrap-reservation", "retained-owner-loss",
        ],
        "9_cleanup_only_failure_recovery": [
            "runner-loss", "owner-loss-automatic", "orchestrator-loss",
            "workstation-loss", "retained-owner-loss", "cleanup-only-recovery",
        ],
        "15_concurrent_exact_cleanup": [
            "late-contract", "slow-delete", "cleanup-only-recovery",
        ],
        "16_credential_redaction": ["create-rejected", "all-scenario-artifact-scans"],
    }
    summary["state"] = "passed"
    summary["credential_absence"] = True
    summary["elapsed_seconds"] = time.monotonic() - gate_started
    (output / "scripted-safety-summary.json").write_text(json.dumps(summary, indent=2))
    (output.parent / "vastai-gate-b-scripted.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    if sys.argv[1] == "--gate":
        try:
            gate(*sys.argv[2:])
        except Exception as error:
            print(f"scripted safety gate failed ({type(error).__name__}); inspect its summary and logs", file=sys.stderr)
            sys.exit(1)
    else:
        serve()

#!/usr/bin/env python3
"""Local VastAI wire fixture and retained cleanup-supervisor safety scenario.

Server:  scripted_provider.py PORT CONTROL_JSON REQUEST_LOG [READY_FD]
Cleanup: scripted_provider.py --retained-cleanup ROOT OUTPUT HARNESS_BINARY

Each server owns its contracts, request ledger, and fault controls. The ready
pipe is written only after HTTP bind/listen succeeds. No credential is logged.
This checks cleanup only, not runtime deployment or full campaign qualification.
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
                state.reveal(control)
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
        assert f"--paid-cleanup-{role}".encode() in command and str(admission_path).encode() in command
        signal.pidfd_send_signal(pidfd, sig)
        assert select.select([pidfd], [], [], 5)[0], f"{role} crash did not stop the admitted incarnation"
    finally:
        os.close(pidfd)
    return identity


def stop_scripted_cleanup_owners(directory):
    paths = [directory / "private" / "paid-admission.json"]
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


def retained_cleanup_gate(root, output, binary):
    if not __debug__:
        raise RuntimeError("scripted safety proofs require Python assertions; optimization is forbidden")
    gate_started = time.monotonic()
    root, output, binary = Path(root).resolve(), Path(output).resolve(), Path(binary).resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ)
    for name in CREDENTIAL_ENVS:
        env[name] = "scripted-" + secrets.token_hex(24)
    marker = env[KEY_ENV]
    name = "retained-owner-loss"
    directory = output / name
    directory.mkdir()
    control_path = directory / "control.json"
    control_path.write_text(json.dumps({"initial_contracts": {
        str(8999 + node): f"scripted-retained-42-{node}-attempt-0" for node in range(1, 6)}}))
    request_path = directory / "requests.jsonl"
    summary = {"schema_version": 1, "state": "running", "scenarios": {},
               "provider": "loopback-scripted", "paid_resources": 0,
               "scope": "retained-cleanup-lifecycle",
               "qualification": "cleanup only; not runtime deployment or full campaign"}
    summary_path = output / "scripted-cleanup-summary.json"
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

        summary["scenarios"][name] = retained_owner_loss(
            binary, root, scenario_env, directory, request, ledger)
        for application_root in (directory / "private-state", directory / "private"):
            for artifact in application_root.rglob("*"):
                if artifact.is_file():
                    content = artifact.read_bytes()
                    assert not any(label.encode() in content for label in UNRELATED_CONTRACTS.values()), "application persisted unrelated account data"
    except BaseException as error:
        summary["state"] = "failed"
        detail = str(error)
        for value in credential_values(env):
            detail = detail.replace(value.decode(), "[REDACTED]")
        summary["error"] = detail
        raise
    finally:
        try:
            stop_scripted_cleanup_owners(directory)
        except BaseException:
            summary["state"] = "failed"
            raise
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
                summary["credential_scan"] = scan_credentials(output, env)
                summary["credential_absence"] = True
            except BaseException:
                summary["state"] = "failed"
                summary["credential_absence"] = False
                raise
            finally:
                summary["elapsed_seconds"] = time.monotonic() - gate_started
                summary_path.write_text(json.dumps(summary, indent=2))
    summary["state"] = "passed"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))







if __name__ == "__main__":
    if sys.argv[1:2] == ["--retained-cleanup"]:
        try:
            retained_cleanup_gate(*sys.argv[2:])
        except Exception as error:
            print(f"scripted retained cleanup failed ({type(error).__name__}); inspect its summary and logs", file=sys.stderr)
            sys.exit(1)
    elif len(sys.argv) in (4, 5) and sys.argv[1].isdigit():
        serve()
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

#!/usr/bin/env python3
"""Run the complete local gates in order; retain every timing, including failures.

Build artifacts first. Warm runs still time fixture creation, deployment,
readiness, workload, recovery, fault injection, evidence and cleanup. This
runner never acquires paid resources or removes caches to manufacture a cold run.
Use --build-images to record source/build provenance during the real Docker
builds. Without it, every cached image must already prove the same qualified
inputs and parent image identities. Container workers/wheels are ABI-specific:
qualification binds their build inputs, not equality with host artifact bytes.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "target/release/myelin-e2e-fuzz"


TEST_PACKAGES = [
    "swactor", "myelin-e2e-fuzz", "swactor-vastai", "myelin-control-contract", "provisioning",
    "myelin", "data-plane", "iroh-driver", "distribution", "swactor-process",
    "swactor-process-context",
]
TEST_COMMAND = ["cargo", "test"] + [
    argument for package in TEST_PACKAGES for argument in ["-p", package]
] + ["--tests"]
def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def source_digest():
    paths = [ROOT / name for name in (
        "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".dockerignore", "clippy.toml")]
    excluded = {"target", ".git", ".venv", "__pycache__", "node_modules"}
    for name in (".cargo", "src", "tests", "crates", "xtask", "apps/myelin", "tools/myelin-e2e-fuzz",
                 "tools/vastai", "tools/actor-control-flow-lint"):
        for directory, directories, files in os.walk(ROOT / name, followlinks=False):
            for entry in directories + files:
                if (Path(directory) / entry).is_symlink():
                    raise RuntimeError(f"source identity rejects symlink {directory}/{entry}")
            directories[:] = [entry for entry in directories if entry not in excluded]
            paths.extend(Path(directory) / entry for entry in files)
    result = hashlib.sha256()
    for path in sorted(paths):
        result.update(os.fsencode(path.relative_to(ROOT)))
        result.update(b"\0")
        result.update(digest(path).encode())
        result.update(b"\0")
    return result.hexdigest()


def persist(path, evidence):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as output:
        json.dump(evidence, output, separators=(",", ":"))
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    parent = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def probe(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"metadata command failed: {command[0]}: {result.stderr}")
    return result.stdout.strip()


def runtime_image_identities(image):
    roles = {"myelin-node-base:cuda12.6": "base", "myelin-node:latest": "node", image: "e2e"}
    identities = {}
    for name in sorted({"myelin-node-base:cuda12.6", "myelin-node:latest", image}):
        value = json.loads(probe(["docker", "image", "inspect", name]))[0]
        if "@sha256:" in name:
            manifest = json.loads(probe(["docker", "manifest", "inspect", name]))
            if (manifest.get("schemaVersion") != 2 or "manifests" in manifest
                    or manifest.get("config", {}).get("digest") != value["Id"]):
                raise RuntimeError("runtime image requires a platform-specific registry manifest "
                                   "whose config digest matches the tested local image")
        labels = value.get("Config", {}).get("Labels") or {}
        prefix = "org.swactor.myelin.e2e."
        if labels.get(prefix + "provenance-version") != "1":
            raise RuntimeError(f"runtime image {name} lacks build-time source provenance; rebuild it")
        identities[name] = {
            "id": value["Id"], "os": value["Os"], "architecture": value["Architecture"],
            "variant": value.get("Variant", ""),
            "repo_digests": sorted(value.get("RepoDigests") or []),
            "provenance_version": 1,
            "source_build_input_digest": labels.get(prefix + "source-build-input-digest", ""),
            "image_role": labels.get(prefix + "image-role", ""),
            "parent_image_id": labels.get(prefix + "parent-image-id"),
        }
        if identities[name]["image_role"] != roles[name]:
            raise RuntimeError(f"runtime image {name} has the wrong build provenance role")
    return identities


def verify_image_provenance(identities, image, source_build_input_digest):
    if len(identities) != 3 or any(
            value["source_build_input_digest"] != source_build_input_digest
            for value in identities.values()):
        raise RuntimeError("runtime images were not built from the qualified source/build inputs")
    base = identities["myelin-node-base:cuda12.6"]
    node = identities["myelin-node:latest"]
    runtime = identities[image]
    if (base["parent_image_id"] is not None
            or node["parent_image_id"] != base["id"]
            or runtime["parent_image_id"] != node["id"]):
        raise RuntimeError("runtime image provenance does not match the qualified parent images")


def image_build_command(role, image, source_build_input_digest, parent_image_id=None):
    suffix = {"base": ".base", "node": "", "e2e": ".e2e"}[role]
    command = ["docker", "build", "-f", "apps/myelin/node-image/Dockerfile" + suffix,
               "-t", image]
    labels = {
        "provenance-version": "1",
        "source-build-input-digest": source_build_input_digest,
        "image-role": role,
    }
    if parent_image_id is not None:
        labels["parent-image-id"] = parent_image_id
        command += ["--build-arg", "BASE_IMAGE=myelin-e2e-parent:" + parent_image_id.removeprefix("sha256:")]
    for key, value in labels.items():
        command += ["--label", "org.swactor.myelin.e2e." + key + "=" + value]
    return command + ["."]


def verify_qualified_inputs(evidence):
    identity_path = Path(evidence["artifact_identity_path"])
    if json.loads(identity_path.read_text()) != evidence["deployment_artifacts"]:
        raise RuntimeError("qualified artifact manifest changed during ordered gates")
    if evidence["source_digest"] != source_digest() or any(
            digest(ROOT / "target/release" / name) != expected
            for name, expected in evidence["build_artifacts"].items()):
        raise RuntimeError("tested source or binaries changed during ordered gates")
    if evidence["image_identities"] != runtime_image_identities(evidence["configuration"]["image"]):
        raise RuntimeError("deployment runtime images changed during ordered gates")


def execute(name, command, directory, evidence, evidence_path, env=None, warm_started=None):
    started = time.monotonic()
    env = dict(os.environ if env is None else env,
               CARGO_TARGET_DIR=str(ROOT / "target"))
    log = directory / f"{name}.log"
    stage = {"name": name, "command": [str(part) for part in command],
             "log": str(log), "state": "running",
             "started_unix_ms": time.time_ns() // 1_000_000}
    evidence["stages"].append(stage)
    persist(evidence_path, evidence)
    print(f"starting {name}: {log}", flush=True)
    try:
        if warm_started is not None and time.monotonic() - warm_started >= 600:
            raise RuntimeError("complete warm workflow exhausted its timing ceiling; later gates not run")
        if "image_identities" in evidence:
            verify_qualified_inputs(evidence)
        if "deployment_artifacts" in evidence:
            identity_path = Path(evidence["artifact_identity_path"])
            if json.loads(identity_path.read_text()) != evidence["deployment_artifacts"]:
                raise RuntimeError("qualified artifact manifest changed during ordered gates")
            env["MYELIN_E2E_ARTIFACT_IDENTITY"] = str(identity_path)
        with log.open("wb") as output:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=output,
                                    stderr=subprocess.STDOUT)
        stage["exit_code"] = result.returncode
        stage["state"] = "passed" if result.returncode == 0 else "failed"
    except BaseException as error:
        stage["state"] = "interrupted"
        stage["error"] = str(error)
        raise
    finally:
        stage["elapsed_secs"] = time.monotonic() - started
        stage["completed_unix_ms"] = time.time_ns() // 1_000_000
        persist(evidence_path, evidence)
    if result.returncode:
        raise RuntimeError(f"{name} failed ({result.returncode}); see {log}; later gates not run")
    if name.endswith("-campaign") and stage["elapsed_secs"] > 300:
        raise RuntimeError(f"{name} exceeded the complete campaign timing ceiling; later gates not run")
    if warm_started is not None and time.monotonic() - warm_started > 600:
        raise RuntimeError("complete warm workflow exceeded its timing ceiling; later gates not run")
    print(f"passed {name}: {stage['elapsed_secs']:.3f}s", flush=True)
    return stage["elapsed_secs"]


def run(args):
    root = args.artifacts.resolve()
    # Do not reuse coverage or overwrite evidence from an earlier invocation.
    root.mkdir(parents=True, exist_ok=False)
    path = root / "ordered-acceptance.json"
    evidence = {"schema_version": 5, "state": "running", "stages": [], "runs": [],
                "machine": {"platform": platform.platform(),
                            "cpu_count": os.cpu_count(),
                            "cpuinfo": Path("/proc/cpuinfo").read_text(),
                            "memory": Path("/proc/meminfo").read_text()},
                "configuration": {"seed": args.seed, "warm_runs": args.warm_runs,
                                  "case_deadline_secs": args.deadline_secs,
                                  "image": args.image, "build_images": args.build_images,
                                  "workspace": str(ROOT), "artifacts": str(root)},
                "paid_execution": "not_attempted"}
    started = time.monotonic()
    try:
        evidence["source_digest"] = source_digest()
        evidence["storage"] = probe(["df", "-T", str(root)])
        evidence["images_before_build"] = probe(
            ["docker", "image", "list", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}"])
        execute("build", ["cargo", "build", "--release", "-p", "myelin",
                          "--bins", "-p", "myelin-e2e-fuzz"], root, evidence, path)
        execute("build-test-binaries", TEST_COMMAND + ["--no-run"], root, evidence, path)
        execute("build-deployment-artifacts",
                [str(BINARY), "--prepare-artifacts", "--deadline-secs", "3600",
                 "--artifacts", str(root / "build-deployment-artifacts")],
                root, evidence, path)
        identity_path = root / "build-deployment-artifacts/build-identity.json"
        evidence["deployment_artifacts"] = json.loads(identity_path.read_text())
        evidence["artifact_identity_path"] = str(identity_path)
        if args.build_images:
            parent = None
            for role, image in [("base", "myelin-node-base:cuda12.6"),
                                ("node", "myelin-node:latest"), ("e2e", args.image)]:
                if parent is not None:
                    parent_tag = "myelin-e2e-parent:" + parent.removeprefix("sha256:")
                    execute(f"build-image-{role}-parent", ["docker", "tag", parent, parent_tag],
                            root, evidence, path)
                execute(f"build-image-{role}", image_build_command(
                    role, image, evidence["deployment_artifacts"]["source_build_input_digest"],
                    parent), root, evidence, path)
                if parent is not None and json.loads(
                        probe(["docker", "image", "inspect", parent_tag]))[0]["Id"] != parent:
                    raise RuntimeError("runtime image parent changed during build")
                parent = json.loads(probe(["docker", "image", "inspect", image]))[0]["Id"]
        evidence["build_artifacts"] = {
            name: digest(ROOT / "target/release" / name)
            for name in ["myelin-e2e-fuzz", "myelin-orchestrator", "myelin-worker"]}
        if evidence["source_digest"] != source_digest():
            raise RuntimeError("source changed while building acceptance artifacts")
        evidence["image_identities"] = runtime_image_identities(args.image)
        verify_image_provenance(evidence["image_identities"], args.image,
                                evidence["deployment_artifacts"]["source_build_input_digest"])
        if "@sha256:" in args.image and args.image not in evidence["image_identities"][args.image]["repo_digests"]:
            raise RuntimeError("runtime image is not bound to the requested registry manifest")
        evidence["build_elapsed_secs"] = time.monotonic() - started
        # Building with a pre-existing cache is not a cold benchmark.
        evidence["cold_measurement"] = "not_claimed; existing cache retained"
        for index in range(args.warm_runs):
            directory = root / f"warm-{index + 1}"
            directory.mkdir()
            record = {"index": index + 1, "state": "running"}
            evidence["runs"].append(record)
            run_start = time.monotonic()
            common = [str(BINARY), "--seed", str(args.seed), "--deadline-secs",
                      str(args.deadline_secs), "--no-build-image", "--image", args.image]
            try:
                execute(f"warm-{index + 1}-gate-a",
                        common + ["--deployment-e2e", "--deployment-nodes", "5",
                                  "--artifacts", str(directory / "gate-a")],
                        directory, evidence, path, warm_started=run_start)
                record["gate_a"] = "passed"
                campaign_secs = execute(f"warm-{index + 1}-campaign",
                        common + ["--campaign", "--fixture-lifetime-secs", "43200",
                                  "--artifacts", str(directory / "campaign")],
                        directory, evidence, path, warm_started=run_start)
                record["campaign_elapsed_secs"] = campaign_secs
                execute(f"warm-{index + 1}-failure-cases",
                        common + ["--nodes", "5", "--failure-cases", "--artifacts",
                                  str(directory / "failure-cases")], directory, evidence, path,
                        warm_started=run_start)
                execute(f"warm-{index + 1}-contract-model-safety",
                        TEST_COMMAND, directory, evidence, path, warm_started=run_start)
                execute(f"warm-{index + 1}-scripted-provider",
                        ["bash", "tools/myelin-e2e-fuzz/scripted_safety_gate.sh",
                         str(directory / "scripted-provider")], directory, evidence, path,
                        env=dict(os.environ, SAFETY_GATE_ROOT=str(ROOT),
                                 SCRIPTED_HARNESS_BINARY=str(BINARY)), warm_started=run_start)
                record["state"] = "passed"
            except BaseException:
                record["state"] = "failed"
                raise
            finally:
                record["elapsed_secs"] = time.monotonic() - run_start
                record["inside_target"] = (record["state"] == "passed"
                    and record.get("campaign_elapsed_secs", float("inf")) <= 300
                    and record["elapsed_secs"] <= 600)
                persist(path, evidence)
            if not record["inside_target"]:
                raise RuntimeError(f"warm run {index + 1} passed behavior but exceeded timing target")
        execute("verify-qualified-deployment-artifacts",
                [str(BINARY), "--prepare-artifacts", "--deadline-secs", str(args.deadline_secs),
                 "--artifacts", str(root / "verify-qualified-deployment-artifacts")],
                root, evidence, path)
        if json.loads((root / "verify-qualified-deployment-artifacts/build-identity.json").read_text()) != evidence["deployment_artifacts"]:
            raise RuntimeError("resolver-selected deployment artifacts changed during ordered gates")
        verify_qualified_inputs(evidence)
        evidence["state"] = "passed"
    except BaseException as error:
        evidence["state"] = "failed"
        evidence["error"] = str(error)
        raise
    finally:
        evidence["elapsed_secs"] = time.monotonic() - started
        evidence["completed_unix_ms"] = time.time_ns() // 1_000_000
        evidence["expires_unix_ms"] = evidence["completed_unix_ms"] + 86_400_000
        persist(path, evidence)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--image", default="myelin-e2e-speed:local")
    parser.add_argument("--build-images", action="store_true")
    parser.add_argument("--seed", type=int, default=20260910)
    parser.add_argument("--deadline-secs", type=int, default=120)
    parser.add_argument("--warm-runs", type=int, default=3)
    args = parser.parse_args()
    if args.warm_runs < 3 or args.deadline_secs <= 0:
        parser.error("at least three warm runs and a positive operation deadline are required")
    if args.image in {"myelin-node-base:cuda12.6", "myelin-node:latest"}:
        parser.error("the workload image must be distinct from its base and node images")
    if args.build_images and "@sha256:" in args.image:
        parser.error("build with a mutable image tag, publish it, then qualify its immutable "
                     "registry reference without --build-images")
    try:
        run(args)
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

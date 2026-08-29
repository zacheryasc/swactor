//! Build/image helpers, Docker resource utilities, and telemetry census.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Read as IoRead, Seek, SeekFrom};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};

use crate::harness::GENERATED_LAUNCHER;

const TRANSIENT_ACTOR_TYPES: [&str; 6] = [
    "swactor_process::",
    "swactor_process_context::",
    "myelin::contextual_process::ContextualOutputRelay",
    "myelin::orchestration::control::ContextualReplyObserver",
    "data_plane::host::",
    "data_plane::source::FileBlobSourceActor",
];

pub(crate) fn build_myelin_binaries(workspace: &Path) -> Result<(), String> {
    run_checked(
        Command::new("cargo").current_dir(workspace).args([
            "build",
            "--release",
            "-p",
            "myelin",
            "--bins",
        ]),
        "build Myelin binaries",
    )
}

pub(crate) fn build_workload_image(workspace: &Path, image: &str) -> Result<(), String> {
    run_checked(
        Command::new("docker").current_dir(workspace).args([
            "build",
            "-f",
            "apps/myelin/node-image/Dockerfile.base",
            "-t",
            "myelin-node-base:cuda12.6",
            ".",
        ]),
        "build Myelin base image",
    )?;
    run_checked(
        Command::new("docker").current_dir(workspace).args([
            "build",
            "-f",
            "apps/myelin/node-image/Dockerfile",
            "-t",
            "myelin-node:latest",
            ".",
        ]),
        "build Myelin node image",
    )?;
    run_checked(
        Command::new("docker").current_dir(workspace).args([
            "build",
            "-f",
            "apps/myelin/node-image/Dockerfile.e2e",
            "-t",
            image,
            ".",
        ]),
        "build Myelin E2E workload image",
    )
}

fn run_checked(command: &mut Command, label: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("{label}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} exited {status}"))
    }
}

pub(crate) fn reserve_port() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("reserve dashboard port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("read reserved dashboard port: {error}"))
}

pub(crate) fn capture_lines(
    reader: impl std::io::Read + Send + 'static,
    lines: Arc<Mutex<VecDeque<String>>>,
) {
    thread::spawn(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            let mut lines = lines.lock().unwrap_or_else(|error| error.into_inner());
            if lines.len() == 512 {
                lines.pop_front();
            }
            lines.push_back(line);
        }
    });
}

pub(crate) fn http_json(method: &str, url: &str, body: Option<Value>) -> Result<Value, String> {
    let response = match (method, body) {
        ("GET", None) => ureq::get(url).timeout(Duration::from_secs(5)).call(),
        ("POST", Some(body)) => ureq::post(url)
            .timeout(Duration::from_secs(5))
            .set("content-type", "application/json")
            .send_string(&body.to_string()),
        _ => return Err(format!("unsupported harness HTTP request {method} {url}")),
    }
    .map_err(|error| format!("{method} {url}: {error}"))?;
    let mut bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {method} {url} response: {error}"))?;
    if bytes.is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode {method} {url} response: {error}"))
    }
}

#[derive(Default)]
pub(crate) struct TelemetryResourceCensus {
    offset: u64,
    carry: Vec<u8>,
    next_line: usize,
    active: BTreeMap<String, String>,
    arenas: BTreeMap<String, Value>,
    poisoned: Vec<Value>,
}

impl TelemetryResourceCensus {
    pub(crate) fn update(&mut self, path: &Path) -> Result<Value, String> {
        let mut file = fs::File::open(path).map_err(|error| {
            format!("open telemetry resource census {}: {error}", path.display())
        })?;
        let length = file
            .metadata()
            .map_err(|error| format!("inspect telemetry resource census: {error}"))?
            .len();
        if length < self.offset {
            *self = Self::default();
        }
        file.seek(SeekFrom::Start(self.offset))
            .map_err(|error| format!("seek telemetry resource census: {error}"))?;
        let mut appended = Vec::new();
        file.read_to_end(&mut appended)
            .map_err(|error| format!("read telemetry resource census: {error}"))?;
        let appended_len = u64::try_from(appended.len())
            .map_err(|_| "telemetry resource census exceeded u64".to_owned())?;
        self.offset = self
            .offset
            .checked_add(appended_len)
            .ok_or_else(|| "telemetry resource census offset overflowed".to_owned())?;
        self.carry.extend_from_slice(&appended);
        if let Some(last_newline) = self.carry.iter().rposition(|byte| *byte == b'\n') {
            let remainder = self.carry.split_off(last_newline + 1);
            let complete = std::mem::replace(&mut self.carry, remainder);
            let text = std::str::from_utf8(&complete)
                .map_err(|error| format!("telemetry archive is not UTF-8: {error}"))?;
            self.ingest(text)?;
        }
        Ok(self.snapshot())
    }

    pub(crate) fn ingest(&mut self, text: &str) -> Result<(), String> {
        for line in text.lines() {
            self.next_line = self.next_line.saturating_add(1);
            let frame: Value = serde_json::from_str(line)
                .map_err(|error| format!("decode telemetry frame {}: {error}", self.next_line))?;
            let channel = frame
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if channel != "runtime.actors" && channel != "mvp.arena" {
                continue;
            }
            let stream = frame
                .get("stream")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let payload = frame
                .pointer("/payload/value")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("telemetry frame {} has no UTF-8 payload", self.next_line))
                .and_then(|payload| {
                    serde_json::from_str::<Value>(payload).map_err(|error| {
                        format!("decode telemetry payload {}: {error}", self.next_line)
                    })
                })?;
            if contains_poison(&payload) {
                self.poisoned
                    .push(json!({"stream": stream, "payload": payload}));
            }
            if channel == "mvp.arena" {
                self.arenas.insert(stream.to_owned(), payload);
                continue;
            }
            let Some(event) = payload.get("event").and_then(Value::as_str) else {
                continue;
            };
            let Some(address) = payload.pointer("/actor/address").and_then(Value::as_str) else {
                continue;
            };
            let key = format!("{stream}/{address}");
            match event {
                "started" => {
                    if let Some(actor_type) =
                        payload.pointer("/actor/actor_type").and_then(Value::as_str)
                    {
                        self.active.insert(key, actor_type.to_owned());
                    }
                }
                "stopped" => {
                    self.active.remove(&key);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Value {
        let active_actors = self
            .active
            .iter()
            .filter(|(_, actor_type)| {
                TRANSIENT_ACTOR_TYPES
                    .iter()
                    .any(|prefix| actor_type.contains(prefix))
            })
            .map(|(identity, actor_type)| json!({"identity": identity, "type": actor_type}))
            .collect::<Vec<_>>();
        json!({
            "active_actors": active_actors,
            "arenas": &self.arenas,
            "poisoned": &self.poisoned,
        })
    }
}

pub(crate) fn transient_actor_identities(health: &Value) -> BTreeSet<String> {
    health
        .pointer("/resources/active_actors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|actor| actor.get("identity").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

pub(crate) fn pending_resource_cleanup(
    health: &Value,
    resource_baseline: &BTreeSet<String>,
) -> Option<String> {
    for node in health
        .get("nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let live = node
            .pointer("/observation/event/executions")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        if live != 0 {
            return Some(format!("{live} contextual processes to exit"));
        }
    }
    let running_nodes = health
        .get("running_nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .collect::<BTreeSet<_>>();
    let active = health
        .pointer("/resources/active_actors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|actor| {
            let Some(identity) = actor.get("identity").and_then(Value::as_str) else {
                return true;
            };
            if resource_baseline.contains(identity) {
                return false;
            }
            identity
                .split_once('/')
                .and_then(|(stream, _)| stream.split_once('#'))
                .and_then(|(logical_node, _)| logical_node.parse::<u64>().ok())
                .is_none_or(|logical_node| running_nodes.contains(&logical_node))
        })
        .count();
    if active != 0 {
        return Some(format!("{active} transient actors to stop"));
    }
    let Some(arenas) = health
        .pointer("/resources/arenas")
        .and_then(Value::as_object)
    else {
        return Some("telemetry arena census".to_owned());
    };
    for node in &running_nodes {
        let arena = arenas
            .iter()
            .filter(|(stream, _)| {
                stream
                    .split_once('#')
                    .and_then(|(logical_node, _)| logical_node.parse::<u64>().ok())
                    == Some(*node)
            })
            .max_by_key(|(stream, _)| {
                stream
                    .split_once('#')
                    .and_then(|(_, generation)| generation.parse::<u64>().ok())
                    .unwrap_or(0)
            })
            .map(|(_, arena)| arena);
        let Some(arena) = arena else {
            return Some(format!("arena census for node {node}"));
        };
        for field in ["live_bytes", "active_leases", "pending_leases"] {
            let count = arena.get(field).and_then(Value::as_u64).unwrap_or(u64::MAX);
            if count != 0 {
                return Some(format!(
                    "node {node} arena {field} to reach zero (observed {count})"
                ));
            }
        }
    }
    None
}

pub(crate) fn contains_poison(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields.iter().any(|(key, value)| {
            (key == "poisoned" && value == &Value::Bool(true))
                || (key == "panics" && value.as_u64().is_some_and(|count| count != 0))
                || contains_poison(value)
        }),
        Value::Array(values) => values.iter().any(contains_poison),
        _ => false,
    }
}

pub(crate) fn list_containers(prefix: &str) -> Result<Vec<String>, String> {
    let output = Command::new("docker")
        .args(["ps", "-aq", "--filter", &format!("name=^{prefix}")])
        .output()
        .map_err(|error| format!("list harness containers: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "list harness containers exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect())
}

/// List generated-launcher command lines across harness containers.
///
/// Returns `Ok(None)` when `docker top` cannot introspect some container —
/// it is stopped, paused, or mid-transition, so it hosts no workload right
/// now. Callers treat that as a retry signal rather than a failure, since
/// failure-injection cases routinely query containers the orchestrator is
/// concurrently tearing down.
pub(crate) fn list_workload_processes(prefix: &str) -> Result<Option<Vec<String>>, String> {
    let mut processes = Vec::new();
    for container in list_containers(prefix)? {
        let output = Command::new("docker")
            .args(["top", &container, "-eo", "pid,args"])
            .output()
            .map_err(|error| format!("inspect harness container {container}: {error}"))?;
        if !output.status.success() {
            return Ok(None);
        }
        for command in String::from_utf8_lossy(&output.stdout).lines().skip(1) {
            if command.contains(GENERATED_LAUNCHER) {
                processes.push(format!("{container}: {command}"));
            }
        }
    }
    Ok(Some(processes))
}

pub(crate) fn remove_container(container: &str) -> Result<(), String> {
    let status = Command::new("docker")
        .args(["rm", "-f", container])
        .status()
        .map_err(|error| format!("remove harness container {container}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "remove harness container {container} exited {status}"
        ))
    }
}

pub(crate) fn remove_containers(prefix: &str) -> Result<(), String> {
    let ids = list_containers(prefix)?;
    if ids.is_empty() {
        return Ok(());
    }
    let status = Command::new("docker")
        .arg("rm")
        .arg("-f")
        .args(ids)
        .status()
        .map_err(|error| format!("remove harness containers: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("remove harness containers exited {status}"))
    }
}

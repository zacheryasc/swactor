# Swactor Managed Process Specification

Id: 8
Last modified: b887e941cbe6f1e209339abd0375507aca9bfe52
Last reviewed:
> Any edit to this spec must update `Last modified` above to the current `git HEAD` commit.

`swactor-process` provides a Swactor actor interface for launching, supervising,
stopping, and observing one operating-system child process per process actor.

The crate owns process lifecycle/control only. Child stdin/stdout/stderr are not
managed or observed by this crate.

---

## 1. Public API

The crate exports:

```text
ProcessSpec
ProcessLifecycleObservability
ProcessOutputConfig
ProcessCommand
ProcessOutput
ExitStatus
spawn_local_process
send_process_command
```

Pipeline and YAML exports are separate crate features and are not part of the
managed-process protocol described here.

### 1.1 ProcessSpec

```text
ProcessSpec {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
    working_dir: Option<PathBuf>,
    label: Option<String>,
}
```

`command` is passed directly to `std::process::Command::new`.

`args` are passed as direct argv entries. The crate does not split, quote,
unquote, expand, or shell-parse argument strings.

`env` contains child environment overrides.

`working_dir`, when present, is passed as the child current working directory.

`label`, when present, is the lifecycle datastream label source. When `label` is
absent, the label source is the basename of `command`.

### 1.2 ProcessOutputConfig

```text
ProcessOutputConfig::disabled(upstream: ActorAddress) -> ProcessOutputConfig
ProcessOutputConfig::datastream_mirror(
    upstream: ActorAddress,
    producer: DatastreamProducer,
) -> ProcessOutputConfig
ProcessOutputConfig::upstream(&self) -> ActorAddress
ProcessOutputConfig::observability(&self) -> ProcessLifecycleObservability
```

`upstream` is the actor address that receives every public `ProcessOutput`.

`disabled` sends only upstream `ProcessOutput`.

`datastream_mirror` sends upstream `ProcessOutput` and also mirrors each
lifecycle/control output to one datastream channel.

### 1.3 ProcessLifecycleObservability

```text
ProcessLifecycleObservability::Disabled
ProcessLifecycleObservability::DatastreamMirror
```

This setting controls lifecycle/control mirroring only. It does not enable child
stdin/stdout/stderr handling.

### 1.4 ProcessCommand

```text
ProcessCommand::Stop {
    kill_after: Option<Duration>,
}
```

`Stop` asks the supervisor to terminate the child process. `kill_after`, when
present, is the grace duration before kill escalation.

### 1.5 ProcessOutput

```text
ProcessOutput::Started { pid: u32 }
ProcessOutput::SpawnFailed { error: String }
ProcessOutput::Exited { status: ExitStatus }
ProcessOutput::Error { error: String }
```

`Started` means the OS child spawned and `pid` is the child process id.

`SpawnFailed` means the process actor was created but the OS child did not spawn.

`Exited` means the OS child reached a terminal status.

`Error` means the process supervisor or process actor hit an operational failure
other than OS spawn failure.

### 1.6 ExitStatus

```text
ExitStatus::Code(i32)
ExitStatus::Signal(i32)
ExitStatus::Unknown
```

### 1.7 Spawn and command helpers

```text
spawn_local_process(
    ctx: &Ctx,
    sender: &ExternalSender,
    spec: ProcessSpec,
    output: ProcessOutputConfig,
) -> Result<ActorAddress, Error>
```

`spawn_local_process` validates lifecycle output configuration, creates one
process actor, wires its private supervisor wake path, and returns the process
actor address.

Success from `spawn_local_process` means the process actor was created. It does
not mean the OS child spawned successfully. OS spawn success or failure is
reported later as `ProcessOutput`.

```text
send_process_command(
    sender: &ExternalSender,
    process: ActorAddress,
    command: ProcessCommand,
) -> Result<(), Error>
```

`send_process_command` is the public helper for sending process commands. It
wraps the public `ProcessCommand` in the actor's private mailbox type.

---

## 2. Runtime topology

One process actor owns one OS child process lifecycle.

The actor owns:

- lifecycle state;
- the configured upstream output address;
- optional lifecycle datastream mirror state;
- a private supervisor thread handle.

The private supervisor thread owns:

- the child process handle and pid;
- blocking-prone child exit polling;
- terminate/kill signal delivery;
- the stop kill deadline.

The child process owns its own execution.

The supervisor thread is not a public actor and not a public extension point.

---

## 3. Child spawn behavior

The supervisor starts the child with:

```text
Command::new(&spec.command)
cmd.args(&spec.args)
cmd.env(key, value) for each spec.env entry
cmd.current_dir(dir) when spec.working_dir is Some(dir)
cmd.stdin(Stdio::null())
cmd.stdout(Stdio::null())
cmd.stderr(Stdio::null())
cmd.spawn()
```

The crate does not invoke a shell unless the caller explicitly sets `command` to
a shell executable and supplies shell arguments.

Child stdin/stdout/stderr are connected to null handles. The managed-process
protocol does not expose stdin writes, stdout/stderr output events, PTY resize,
or arbitrary signal commands.

If `cmd.spawn()` fails, the actor emits exactly one terminal
`ProcessOutput::SpawnFailed { error }` and does not emit `Started`, `Exited`, or
`Error` for that spawn failure.

---

## 4. Lifecycle output delivery

The actor sends each public `ProcessOutput` to the configured upstream actor.

When `ProcessOutputConfig::datastream_mirror` is used, the actor also mirrors
each output to the configured datastream producer. Datastream submit failure is
ignored and does not suppress upstream output or emit `ProcessOutput::Error`.

Public lifecycle/control output order follows observed lifecycle:

- successful spawn emits `Started` before any terminal `Exited`;
- spawn failure emits `SpawnFailed` without `Started` or `Exited`;
- supervisor failure emits `Error`;
- after a terminal output, later public stop attempts emit no additional
  `ProcessOutput`.

---

## 5. Lifecycle datastream labels and records

The label source is:

1. `ProcessSpec::label`, when present;
2. otherwise, the final non-empty path segment of `ProcessSpec::command`;
3. otherwise, the full `command` string.

The label sanitizer:

- trims source whitespace;
- lowercases ASCII alphanumeric characters;
- preserves `_` and `-`;
- replaces every other character with `_`;
- collapses repeated `_`;
- trims leading and trailing `_`;
- rejects an empty sanitized result with:

```text
invalid process lifecycle label: empty segment
```

The lifecycle channel name is:

```text
proc.<label>.lifecycle
```

When lifecycle mirroring is enabled, the channel is registered as:

```text
ChannelContent::JsonRecord {
    schema: Some("swactor_process.lifecycle.v1")
}
```

Duplicate lifecycle labels on the same datastream stream are rejected before the
process actor is spawned with an error containing:

```text
duplicate process lifecycle datastream channel: proc.<label>.lifecycle on stream <stream>
```

Lifecycle JSON records are:

```json
{"event":"started","pid":123}
{"event":"spawn_failed","error":"..."}
{"event":"exited","status":{"kind":"code","value":0}}
{"event":"exited","status":{"kind":"signal","value":9}}
{"event":"exited","status":{"kind":"unknown"}}
{"event":"error","error":"..."}
```

The managed-process core registers no `proc.<label>.stdout` or
`proc.<label>.stderr` channels.

---

## 6. Stop semantics

Use `send_process_command` with `ProcessCommand::Stop { kill_after }` to request
shutdown.

If stop is requested before the child reports successful spawn, the actor queues
the stop intent. When the child later starts, the actor still emits `Started`
first, asks the supervisor to terminate the child, and later emits terminal
`Exited` unless a supervisor failure occurs.

If stop is requested before an OS spawn failure is reported, the actor still
emits only `SpawnFailed` for that failed spawn.

If the child is running, stop sends terminate to the child process.

If `kill_after` is `Some(duration)`, the supervisor sends kill after that
deadline if the child has not exited.

If `kill_after` is `None`, the supervisor does not schedule kill escalation.

Duplicate stop while already stopping is a no-op. It does not tighten, extend,
or replace the original kill deadline.

Stop after terminal output is a no-op if the actor is still alive. If the actor
has already stopped, sending the command may fail at the runtime address layer;
that failure does not produce process output.

---

## 7. Actor and supervisor cleanup

The actor uses a private mailbox wrapper so supervisor wake messages and public
process commands share one actor input type without changing the Swactor runtime.

The actor drains supervisor events when it starts, when it receives a supervisor
wake, and before/after applying a public stop command.

The supervisor sends a private `ThreadFinished` event when its thread reaches the
end of process supervision. The actor stops itself only after a terminal state is
recorded and the supervisor handle has been cleared.

If the actor stops while the supervisor is still present, it sends best-effort
shutdown to the supervisor without blocking for a waiting join.

Dropping the supervisor handle sends best-effort shutdown and joins only when
the thread is already finished.

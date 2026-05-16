# single-gpu-inference — Behavioral Spec

This document describes what the `single-gpu-inference` example actually does at
runtime. It is built around the **actor graph** — what each actor's job is, who
it talks to, and what flows along each edge.

Audience: an engineer who has cloned the repo and wants to debug a failure or
extend the deployment. Scope: the **vast.ai** deployment path. The localhost
path is the same actors with relay disabled and is covered briefly at the end.

> **Status:** As of `cb789c3` the deployment runs end-to-end. The original
> `SPEC.md` was the design document; this file describes what was built.

---

## 1. Topology

Two machines, two control planes between them.

```
┌─────────────────────────┐                                  ┌─────────────────────────┐
│  Operator laptop        │ ─── vast.ai REST (HTTPS) ──────► │  vast.ai container      │
│  single-gpu-inference   │     find / create / poll /       │  gpu-node               │
│                         │     logs / destroy               │  + tinygrad_worker.py   │
│                         │                                  │                         │
│                         │ ◄══ iroh QUIC (via relay) ══════►│                         │
└─────────────────────────┘     • SWIM gossip                └─────────────────────────┘
                                • actor envelopes
```

- **vast.ai REST** is laptop-only. Provisioning and teardown.
- **iroh QUIC** is bidirectional and carries everything else: SWIM membership
  gossip (cluster join, name registration) and actor envelopes (the
  `InferenceRequest`/`InferenceResponse` pair).
- Both ends are typically behind NAT, so iroh relay infrastructure is required
  to bootstrap connectivity; iroh upgrades to a direct QUIC path opportunistically.

---

## 2. Actor topology

### 2.1 The graph

There is one `swactor::Runtime` per machine. The laptop runtime is almost
empty — the laptop is a client, so its only actor is the response inbox. The
remote runtime contains the inference actor tree.

```
   ╔═══════════════════════ Laptop runtime ═══════════════════════╗            ╔═══════════════════════════ gpu-node runtime ════════════════════════════╗
   ║                                                              ║            ║                                                                         ║
   ║   ┌─────────────┐                                            ║            ║          ╔═══════════════════╗                                          ║
   ║   │ main thread │                                            ║            ║          ║   RequestBridge   ║                                          ║
   ║   └──────┬──────┘                                            ║            ║          ║  name="inference" ║                                          ║
   ║          │ send_to(bridge_addr,                              ║            ║          ╚═════════╤═════════╝                                          ║
   ║          │   InferenceRequest{reply_to=inbox_addr})          ║   wire     ║                    │ InferenceActorMsg::Request(req)                    ║
   ║          ▼                                                   ║   QUIC     ║                    ▼                                                    ║
   ║   ┌──────────────────┐                                       ║            ║          ╔═════════════════════════════╗     InferenceActorStatus       ║
   ║   │ TransportRouter  │ ─► IrohActorTransport ────────────────╫═══════════►║          ║                             ║ ───────────────────►┌────────┐ ║
   ║   │  bridge_addr →   │    (target: remote endpoint)          ║ Inference- ║          ║       InferenceActor        ║   (Started, Ready,  │ Inbox< │ ║
   ║   │  remote          │                                       ║ Request    ║          ║                             ║    Exited)          │ Status>│ ║
   ║   └──────────────────┘                                       ║            ║          ║   state:                    ║                     └────┬───┘ ║
   ║                                                              ║            ║          ║   • ready : bool            ║                          │try_ ║
   ║                                                              ║            ║          ║   • process_alive : bool    ║                          │recv ║
   ║                                                              ║            ║          ║   • pending_replies (FIFO)  ║                          ▼     ║
   ║                                                              ║            ║          ║   • output_buffer : String  ║                  ┌─────────────┐║
   ║                                                              ║            ║          ║                             ║                  │ main thread │║
   ║                                                              ║            ║          ╚════╤═════════════════════╤══╝                  └─────────────┘║
   ║                                                              ║            ║               │                     │                                   ║
   ║                                                              ║            ║   Inference-  │                     │ ProcessCommand::                  ║
   ║                                                              ║            ║   Response    │                     │   WriteStdin{json}                ║
   ║                                                              ║            ║   (to         │                     ▼                                   ║
   ║                                                              ║            ║   reply_to)   │             ╔════════════════╗                          ║
   ║                                                              ║   wire     ║               │             ║  ProcessActor  ║                          ║
   ║   ┌──────────────────────┐                                   ║   QUIC     ║               │             ║ (swactor_      ║                          ║
   ║   │ Inbox<               │ ◄─────────────────────────────────╫════════════╫───────────────┘             ║  process)      ║                          ║
   ║   │   InferenceResponse> │   codec decodes →                 ║ Inference- ║                             ╚════╤═════════╤═╝                          ║
   ║   └──────────┬───────────┘   rt.deliver_raw                  ║ Response   ║                                  │         │                            ║
   ║              │ try_recv                                      ║            ║                                  │         │ ProcessNotification        ║
   ║              ▼                                               ║            ║                                  │         │ (Started/Output/Exited)    ║
   ║   ┌─────────────┐                                            ║            ║                                  │         ▼                            ║
   ║   │ main thread │                                            ║            ║                                  │ ╔════════════════╗                   ║
   ║   └─────────────┘                                            ║            ║                                  │ ║ ProcessBridge  ║                   ║
   ║                                                              ║            ║                                  │ ╚════════╤═══════╝                   ║
   ║                                                              ║            ║                                  │          │ InferenceActorMsg::       ║
   ║                                                              ║            ║                                  │          │   Process(notif)          ║
   ║                                                              ║            ║                                  │          └─► (back to InferenceActor)║
   ║                                                              ║            ║                                  │                                      ║
   ║                                                              ║            ║   stdin/stdout pipes             ▼                                      ║
   ║                                                              ║            ║   (not actor traffic)        ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐               ║
   ║                                                              ║            ║                                tinygrad_worker.py (Python)              ║
   ║                                                              ║            ║                              └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘               ║
   ╚══════════════════════════════════════════════════════════════╝            ╚═════════════════════════════════════════════════════════════════════════╝

Legend:  ╔═══╗ actor   ┌───┐ inbox / runtime infra   ┌─ ─ ─┐ non-actor (OS pipes)
         ────► actor send   ════►  wire (iroh QUIC)   ─ ─ ─►  pipes
```

### 2.2 At a glance

| Actor                       | Runtime       | Created by                  | Role |
|-----------------------------|---------------|-----------------------------|------|
| `RequestBridge`             | gpu-node      | `gpu-node` main             | Inbound network adapter; entry point for all `InferenceRequest`s |
| `InferenceActor`            | gpu-node      | `gpu-node` main             | The brain: supervises the worker, queues replies, owns response routing |
| `ProcessActor`              | gpu-node      | `InferenceActor::on_start`  | Owns the Python child process and its pipes |
| `ProcessBridge`             | gpu-node      | `InferenceActor::on_start`  | Type adapter; rewraps `ProcessNotification` for the inference actor |
| `Inbox<InferenceActorStatus>` | gpu-node    | `gpu-node` main             | Lifecycle observer (used by main thread to wait for `WorkerReady`) |
| `Inbox<InferenceResponse>`  | laptop        | `single-gpu-inference` main | Where the final response lands |

The laptop spawns **no business actors**. It is a client.

---

## 3. Actor roles

Each subsection follows the same pattern: **role · runtime · inputs · outputs ·
state · why it exists**.

### 3.1 `RequestBridge`

- **Role:** entry point. Receives `InferenceRequest`s from the network and
  forwards them to `InferenceActor`.
- **Runtime:** gpu-node.
- **Inputs:** `InferenceRequest` (delivered by the codec registry from inbound
  QUIC streams).
- **Outputs:** `InferenceActorMsg::Request(req)` → `InferenceActor`.
- **State:** none, beyond the target address.
- **Why it exists:** swactor actors have a single `Incoming` type.
  `InferenceActor` needs to accept *both* requests from the network *and*
  notifications from the process. Its `Incoming` is a union enum
  (`InferenceActorMsg`); `RequestBridge` is the thin adapter that converts the
  raw network type into the right variant. **`bridge_addr` is what's registered
  under the cluster name `"inference"`** — it's the network's public-facing
  handle.

### 3.2 `InferenceActor`

- **Role:** the brain. Supervises the worker process, dispatches prompts, and
  routes responses back to the original caller.
- **Runtime:** gpu-node.
- **Inputs:** `InferenceActorMsg`, which is a union of:
  - `Request(InferenceRequest)` — a prompt to run, carrying a `reply_to`
    address.
  - `Process(ProcessNotification)` — lifecycle/IO events from the Python child.
- **Outputs:**
  - `ProcessCommand::WriteStdin{data}` → `ProcessActor` (JSON prompt + `\n`).
  - `InferenceResponse{text}` → the request's `reply_to` (cross-runtime,
    travels back over iroh).
  - `InferenceActorStatus` events → optional `status_addr` (the lifecycle
    inbox).
- **State:**
  - `process_addr`, `bridge_addr` — handles to the actors it spawned.
  - `ready : bool` — has the worker emitted `{"status":"ready"}`?
  - `process_alive : bool` — has the worker exited?
  - `pending_replies : VecDeque<ActorAddress>` — FIFO queue of `reply_to`
    addresses for in-flight prompts.
  - `output_buffer : String` — accumulates stdout bytes until a full `\n`-delimited line.
  - `worker_pid : Option<u32>` — for diagnostics.
- **Why it exists:** the actor that *means* something domain-wise. It is the
  reified "inference service" — everything else exists to feed it or relay for
  it.

The FIFO queue is the only mechanism for matching responses to requests. The
worker produces output strictly in order, so popping the front of
`pending_replies` for each `{"response": …}` line is sufficient. **There is no
request id.** This is fine because the worker is single-threaded and replies in
arrival order.

### 3.3 `ProcessActor`

- **Role:** owns the Python child process and its OS pipes.
- **Runtime:** gpu-node.
- **Inputs:** `ProcessCommand` (`Subscribe`, `WriteStdin`, `Close`, …).
- **Outputs:** `ProcessNotification` (`Started`, `Output`, `Exited`, `Error`)
  to its subscribers.
- **State:** the child PID, the stdin/stdout/stderr pipes, a buffer of pending
  notifications.
- **Why it exists:** it's a generic actor provided by the `swactor_process`
  crate. Not specific to this example. It is the only thing in the system that
  actually fork-execs the Python interpreter and reads its stdout.

### 3.4 `ProcessBridge`

- **Role:** type adapter from `ProcessNotification` → `InferenceActorMsg`.
- **Runtime:** gpu-node.
- **Inputs:** `ProcessNotification` (subscribed via
  `ProcessCommand::Subscribe`).
- **Outputs:** `InferenceActorMsg::Process(notif)` → `InferenceActor`.
- **State:** target address only.
- **Why it exists:** same reason as `RequestBridge` — the inference actor has
  one `Incoming` type. `ProcessBridge` is the thin adapter for the other
  source. Note: `ProcessBridge` is created by `InferenceActor::on_start` and
  subscribed to the `ProcessActor` in the same step. It is private to the
  inference actor's lifetime.

### 3.5 `Inbox<InferenceActorStatus>`

- **Role:** out-of-band lifecycle channel.
- **Runtime:** gpu-node.
- **Inputs:** `InferenceActorStatus` variants (`ProcessStarted`,
  `WorkerReady{pid}`, `ProcessExited{status}`).
- **Outputs:** none; drained by `gpu-node`'s main thread via `try_recv`.
- **Why it exists:** the `gpu-node` main thread needs to block until the
  worker is actually ready before entering its serve loop, and wants to log
  worker-death events. An inbox is the simplest cross-thread observer
  primitive the runtime provides.

### 3.6 `Inbox<InferenceResponse>` (laptop side)

- **Role:** receiving end of the call.
- **Runtime:** laptop.
- **Inputs:** `InferenceResponse` (delivered by the codec from inbound QUIC
  streams).
- **Outputs:** none; the laptop's main thread reads via `try_recv`.
- **Why it exists:** **its `ActorAddress` is what the laptop puts in
  `req.reply_to`.** The remote uses that address to direct the response back —
  no out-of-band reply channel needed.

---

## 4. Dataflows

Each scenario below is a message-sequence walk through the actor graph. Steps
that cross machines are flagged with `── wire ──`. Steps that touch the OS
(fork, pipe IO) are flagged with `── OS ──`.

### 4.1 Worker boot

When `gpu-node` starts, the inference actor tree is spawned and the Python
worker comes up. This typically takes 3–10 minutes due to model download +
CUDA kernel compilation.

```
Step  Actor / source         Event
────  ─────────────────────  ─────────────────────────────────────────────────────────
  1   gpu-node main          rt.spawn(InferenceActor)
  2   InferenceActor         on_start runs:
                                a. spawn_local_process(spec) ─► ProcessActor
                                b. ctx.spawn(ProcessBridge{target: self_addr})
                                c. ctx.send(ProcessActor, Subscribe{address: bridge_addr})
                                d. store process_addr, bridge_addr
  3   gpu-node main          ctx.spawn(RequestBridge{target: inference_addr})
                                rt.node.register_name("inference", bridge_addr)
── OS ───────────────────────────────────────────────────────────────────────
  4   ProcessActor           OS-fork+exec: python3 tinygrad_worker.py
  5   ProcessActor           emits ProcessNotification::Started
  6   ProcessBridge          wraps ─► InferenceActorMsg::Process(Started)
  7   InferenceActor         process_alive = true
                                ctx.send(status_addr, ProcessStarted)
... (tinygrad imports, fetches GGUF, loads model into VRAM — minutes) ...
  8   tinygrad_worker.py     prints {"status":"ready","pid":N} to stdout
── OS ───────────────────────────────────────────────────────────────────────
  9   ProcessActor           reads line; emits ProcessNotification::Output{data}
 10   ProcessBridge          wraps ─► InferenceActorMsg::Process(Output)
 11   InferenceActor         output_buffer += data; finds '\n'; parses JSON;
                                ready = true; worker_pid = N;
                                ctx.send(status_addr, WorkerReady{pid:N})
 12   gpu-node main          status_inbox.try_recv() ─► WorkerReady; exits startup wait
```

### 4.2 Request → Response (happy path)

The end-to-end RPC. This is the scenario the example was built to prove.

```
Step  Actor / source         Event
────  ─────────────────────  ─────────────────────────────────────────────────────────
  1   laptop main            rt.send_to(bridge_addr,
                                InferenceRequest{prompt, ..., reply_to: inbox_addr})
  2   laptop TransportRouter resolves bridge_addr ─► IrohActorTransport
                                codec encodes; opens QUIC uni stream; writes wire
                                envelope (32B dest + tag + payload); finishes stream
── wire ────────────────────────────────────────────────────────────────────
  3   gpu-node main loop     drain_and_collect_reply_addrs():
                                reads stream → decode_wire()
                                envelope.type_tag = "smoke::InferenceRequest"
                                deserialize → extract req.reply_to
                                codecs.receive() → rt.deliver_raw(bridge_addr,
                                                                  InferenceRequest)
  4   RequestBridge          ctx.send(target, InferenceActorMsg::Request(req))
  5   InferenceActor         handle:
                                guard: ready && process_alive (else: empty reply, return)
                                pending_replies.push_back(req.reply_to)
                                ctx.send(process_addr,
                                  WriteStdin{json {prompt, max_tokens, temperature} + \n})
  6   gpu-node main loop     for each reply_to collected in step 3:
                                look up the only alive SWIM peer (the laptop)
                                build IrohActorTransport(laptop endpoint)
                                router.add_route(reply_to, transport)
── OS ───────────────────────────────────────────────────────────────────────
  7   ProcessActor           writes bytes to worker stdin
  8   tinygrad_worker.py     forward pass on CUDA; prints {"response":"..."}
  9   ProcessActor           emits ProcessNotification::Output{data}
 10   ProcessBridge          wraps ─► InferenceActorMsg::Process(Output)
 11   InferenceActor         output_buffer += data; '\n' delimits a line;
                                parses JSON; pending_replies.pop_front() → reply_to
                                ctx.send(reply_to, InferenceResponse{text})
 12   gpu-node TransportRouter  resolves reply_to (route added in step 6) ─►
                                IrohActorTransport (laptop endpoint)
                                codec encodes; QUIC uni stream to laptop
── wire ────────────────────────────────────────────────────────────────────
 13   laptop main loop       drain_actor_messages():
                                decode_wire() → codecs.receive()
                                → rt.deliver_raw(inbox_addr, InferenceResponse)
 14   laptop main            response_inbox.try_recv() → text; print; loop exits
```

The interesting moment is step 6: **the response route is built lazily**. The
remote has no idea what the laptop's inbox address is until it sees the
inbound request, at which point it cracks the envelope open and registers a
route from the response's `reply_to` to the only alive peer.

### 4.3 Concurrent requests

The actor graph supports overlapping requests naturally:

```
Step  Actor / source         Event
────  ─────────────────────  ─────────────────────────────────────────────────────────
  …   laptop                 sends Req#1{reply_to:A}, then Req#2{reply_to:B}
  …   InferenceActor         pending_replies = [A, B]
                                writes prompt#1 to stdin, then prompt#2 to stdin
  …   tinygrad_worker.py     replies in order: {"response":"r1"}, {"response":"r2"}
  …   InferenceActor         line#1 → pop_front=A → send InferenceResponse{r1} to A
                                line#2 → pop_front=B → send InferenceResponse{r2} to B
```

The worker is single-threaded and writes complete responses sequentially. The
queue's FIFO discipline plus the worker's ordering invariant is what keeps
responses paired with their callers.

If the worker emits a `{"error": …}` line, `InferenceActor` logs it but does
not pop the queue — that pending caller will never get a response. (See §7.)

### 4.4 Worker dies after ready

Best-effort cleanup, then stay alive for diagnostics.

```
Step  Actor / source         Event
────  ─────────────────────  ─────────────────────────────────────────────────────────
  1   tinygrad_worker.py     crashes (segfault / OOM / exception)
── OS ───────────────────────────────────────────────────────────────────────
  2   ProcessActor           detects child exit;
                                emits ProcessNotification::Exited{status}
  3   ProcessBridge          wraps ─► InferenceActorMsg::Process(Exited)
  4   InferenceActor         process_alive = false; ready = false
                                drain pending_replies: send InferenceResponse{text:""}
                                to each (so the laptop sees an empty, not a hang)
                                ctx.send(status_addr, ProcessExited{status})
  5   gpu-node main          status_inbox.try_recv() → ProcessExited
                                logs "worker exited, keeping main loop alive
                                       for diagnostics"
                                main loop does NOT exit — SWIM gossip continues
  6   laptop                 sees empty responses (or times out at 300s);
                                fetches remote logs via vast.ai REST;
                                destroys instance; exits 1
```

The "don't exit" choice in step 5 is deliberate. If `gpu-node` exited, the
container would die, SWIM would mark it dead, and the laptop would have no
way to fetch logs to figure out *why* the worker crashed. Keeping the
process alive lets vast.ai's log endpoint capture the worker's stderr.

### 4.5 Request arrives before worker is ready

This can happen if the laptop somehow sends a request before SWIM reports the
peer alive. In practice the laptop's pump loop won't send until name
resolution succeeds, but the `InferenceActor` is defensive about it.

```
Step  Actor / source         Event
────  ─────────────────────  ─────────────────────────────────────────────────────────
  1   InferenceActor         handle Request: !ready
                                ctx.send(req.reply_to, InferenceResponse{text:""})
                                return (do NOT enqueue or write stdin)
  2   laptop                 try_recv sees empty text; pump loop logs "retrying"
                                and continues — there is no automatic resend
```

So an empty-string `InferenceResponse` is the in-band "not ready" signal.

---

## 5. Machines, processes, environment

The actor graph is hosted by a small number of OS processes:

| Machine        | Process                  | Source                              | What it owns |
|----------------|--------------------------|-------------------------------------|--------------|
| Laptop         | `single-gpu-inference`   | `src/bin/single_gpu_inference.rs`   | tokio runtime, iroh Endpoint (`RelayMode::Default`), `IrohDriver`, swactor `Runtime`, the response inbox |
| Container      | `gpu-node`               | `src/bin/gpu_node.rs`               | tokio runtime, iroh Endpoint (`RelayMode::Default`), `IrohDriver`, swactor `Runtime`, the inference actor tree |
| Container      | `tinygrad_worker.py`     | `tinygrad_worker.py`                | Python interpreter with tinygrad 0.12.0; child of `gpu-node`; no network of its own (except the initial GGUF fetch) |

Container image: `nvidia/cuda:12.6.3-devel-ubuntu24.04`, with Python 3 and
tinygrad pre-installed. Built from `Dockerfile`.

Environment provided by the orchestrator at container boot:

| Var          | Set by        | Purpose                                                      |
|--------------|---------------|--------------------------------------------------------------|
| `SEED_ADDR`  | `create_instance` env | 64-char hex node id of the laptop's iroh endpoint    |
| `SEED_RELAY` | `create_instance` env | Laptop's home relay URL (needed for WAN NAT traversal) |
| `CUDA=1`     | Dockerfile    | tinygrad uses the CUDA backend                              |
| `WORKER_SCRIPT` | Dockerfile | Path `gpu-node` uses to launch the Python worker            |

Neither machine needs a public IP or open inbound ports. All inbound traffic
arrives via the iroh relay infrastructure.

---

## 6. Wire protocols

Three distinct protocols carry data; one carries pipe traffic inside the
container.

### 6.1 Actor envelopes — ALPN `swactor/actor/1`

Application-level messages between actors on different runtimes. Format
(from `iroh_transport.rs::encode_wire`):

```
┌─────────────────┬───────────────┬──────────────┬─────────────────┐
│ dest_addr 32B   │ tag_len u32   │ type_tag UTF8│ payload bytes   │
└─────────────────┴───────────────┴──────────────┴─────────────────┘
```

One envelope per QUIC uni stream. Connections are cached
(`IrohActorTransport.conn`) so subsequent sends reuse the connection; streams
are per-message and finished immediately.

| `type_tag`                | Direction      | Payload (JSON) |
|---------------------------|----------------|----------------|
| `smoke::InferenceRequest` | laptop → remote | `{prompt, max_tokens, temperature, reply_to}` |
| `smoke::InferenceResponse`| remote → laptop | `{text}` |

### 6.2 SWIM gossip

Membership and metadata, owned by the `distribution` crate (separate ALPN).
Carries probes between members (`probe_interval = 10`, `probe_timeout = 15`),
indirect probes, and piggybacked metadata gossip. **Name registrations
propagate via this metadata channel** — the laptop's
`driver.node().resolve_name("inference")` is reading state that was gossiped
from the remote.

### 6.3 vast.ai REST — HTTPS to `cloud.vast.ai`

Laptop-only.

| Operation        | Method | URL                                              |
|------------------|--------|--------------------------------------------------|
| Find offer       | GET    | `/api/v0/bundles/?q=<json>`                      |
| Create instance  | PUT    | `/api/v0/asks/{offer_id}/`                       |
| Poll status      | GET    | `/api/v0/instances/{contract_id}/`               |
| Request logs     | PUT    | `/api/v0/instances/request_logs/{contract_id}/`  |
| Fetch logs       | GET    | S3 URL returned by `request_logs`                |
| Destroy          | DELETE | `/api/v0/instances/{contract_id}/`               |

The `create_instance` body sets the Docker image, the env (`SEED_ADDR`,
`SEED_RELAY`), an `onstart` command (`exec /usr/local/bin/gpu-node 2>&1`),
and disk size.

### 6.4 Worker IPC — OS pipes inside the container

`ProcessActor` owns these pipes; nothing else in the system touches them.
Newline-delimited JSON.

| Direction        | Shape                                                                |
|------------------|----------------------------------------------------------------------|
| worker → parent  | `{"status": "ready", "pid": <int>}` — emitted once after model load. |
| parent → worker  | `{"prompt": ..., "max_tokens": ..., "temperature": ...}`              |
| worker → parent  | `{"response": "..."}`                                                |
| worker → parent  | `{"error": "..."}` on bad JSON or generation failure                 |

`InferenceActor::process_output_line` parses these and maps them to the right
actor message (either a status event or a response to the queue's front).

---

## 7. Failure modes and recovery

Failures are documented per-stage. "Recovery" means automated behavior in the
current code — if no recovery is listed, the failure is fatal after destroying
any allocated instance.

### 7.1 Provisioning

| Failure                                            | Detected by                                     | Behavior |
|----------------------------------------------------|-------------------------------------------------|----------|
| No vast.ai offers match the filter                 | `find_offer` returns empty                      | Exit 1. Nothing to clean up. |
| `create_instance` returns non-success              | HTTP status check                               | Exclude offer id, retry with next-cheapest (up to 3 attempts). |
| Instance never reaches `running`                    | `wait_for_running` exhausts 60 polls, sees `exited`/`error`, or `intended_status=stopped`. Host-side OCI/CDI errors surface as `status_msg` containing "Error" or "failed". | Destroy instance, exclude offer, retry. |

### 7.2 Cluster join

| Failure                            | Detected by                                          | Behavior |
|------------------------------------|------------------------------------------------------|----------|
| SWIM never converges (no peer goes to `alive` within 120s) | `driver.snapshot().members` loop on the laptop | Fetch last 40 lines of instance logs, destroy instance, exclude offer, retry. The usual cause: broken host (image pull failure, CDI errors, blocked outbound UDP). |
| Name `"inference"` never resolves (60s) | `resolve_name` returns `None`                  | Destroy instance, exit 1. No retry: convergence already happened so the host is healthy; this means the bridge crashed before registering. |

### 7.3 Inference

| Failure                                  | Detected by                                  | Behavior |
|------------------------------------------|----------------------------------------------|----------|
| Worker never reports `ready` (600s)      | `gpu-node` startup loop on the remote        | `gpu-node` exits 1. Container exits; SWIM marks it dead; laptop times out on convergence and treats it like §7.2. |
| Worker exits *after* reporting ready     | See §4.4                                     | Pending replies drained with empty text; `gpu-node` main loop stays alive for diagnostics; laptop times out and fetches logs. |
| Worker emits `{"error": ...}`            | `process_output_line`                        | Logged to stderr. **Pending queue is not popped** — the caller will time out. (Known gap; a real service would pop with an explicit error response.) |
| Empty response (request arrived before worker was ready) | See §4.5                          | Laptop ignores empty responses in its pump loop. No automatic resend. |
| Inference timeout (300s)                 | Laptop pump loop                             | Fetch last 60 lines of remote logs, destroy instance, exit 1. |

### 7.4 Teardown

| Failure                  | Detected by                  | Behavior |
|--------------------------|------------------------------|----------|
| `destroy_instance` errors | Reqwest error                | Error is dropped (`let _ = …`). Instance keeps costing money until manually destroyed. **Known gap.** |

### 7.5 Invariants worth knowing

- **Every code path that creates an instance also destroys it.** Search
  `destroy_instance` in `single_gpu_inference.rs` — three call sites cover all
  three post-provisioning failure points.
- **`pending_replies` is the only request↔response correlation.** There is no
  request id on the wire. This is safe given the worker's single-threaded,
  in-order behavior; it is **not** safe if you ever swap in a multi-worker
  backend.
- **`ActorAddress` is opaque to iroh and to vast.ai.** It's a swactor-level
  identifier. Routing is done by the swactor runtime's `TransportRouter`.
- **No per-request authentication.** Anything that can reach the remote's iroh
  endpoint and knows `bridge_addr` can submit work. This relies on the address
  being unguessable. Don't reuse the pattern in production without layering
  auth on top.

---

## 8. Localhost mode (brief)

`single-gpu-inference --seed <hex>` and a locally-running `gpu-node` use the
**same actor graph** and **same wire protocols** as the vast.ai path, with two
changes:

- Both ends use `RelayMode::Disabled` — no relay needed on loopback.
- No vast.ai REST traffic. The operator runs `gpu-node` directly, copies its
  `GPU_NODE_ADDR` hex out of the log, and passes it to
  `single-gpu-inference --seed`.

Everything else — the bridges, the `pending_replies` queue, the
`InferenceRequest`/`InferenceResponse` flow, the worker stdin/stdout JSON
protocol — is identical. This is the mode used by `tests/t_binary.rs` for
end-to-end testing without a GPU or an API key.

# Actor Control Audit Ideas

**status**: early draft

Grounding from `crates/mvp-system`: the specs already give a useful audit line. `MVP_SYSTEM_SPEC.md` says the orchestrator is run authority, swactor owns the control plane, actors establish/observe/tear down components, and tensor bytes are explicitly not actor-mailbox traffic. `MVP_NODE_PROVISIONING_SPEC.md` also gives a key exception: provider I/O and SSH bootstrap are temporary pre-swactor paths; after convergence, swactor is the live control path.

Suggested somewhat-deterministic identification passes:

1. **Execution-boundary denylist scan**   -- Yes, and the inverse, any code not called from an Actor::handle(...) needs inspection.
   AST-scan for `std::thread::spawn`, `tokio::spawn`, `Handle::spawn`, `spawn_blocking`, `Command::new(...).spawn`, `Runtime::new`, and `block_on`. Anything not inside an actor, runtime bootstrap, hot-path byte pump, or pre-swactor bootstrap allowlist is a candidate.

2. **Process ownership audit**  
   Find every `std::process::Child`, `ChildStdin`, `ChildStdout`, `ChildStderr`, and `Command::new`. Require each long-lived child to have an actor owner, stop message, exit observation path, and teardown report; otherwise it is likely imperative supervision.

3. **Network listener audit**   
   Scan for `TcpListener`, `UnixListener`, `UnixStream`, `UnixDatagram`, `accept`, and per-connection threads/tasks. A listener is acceptable if it immediately decodes ingress into actor messages; if it owns request state or invokes domain operations directly, flag it.

4. **Channel-as-shadow-mailbox audit**  
   Scan for `std::sync::mpsc`, `tokio::sync::mpsc`, `oneshot`, `watch`, `broadcast`, and custom queues. Channels outside actor shells often mean a parallel control surface; classify each as actor ingress adapter, data hot-path helper, test harness, or suspect.


7. **Actor reachability taint analysis**   -- Yes see my comments on 1
   Treat `impl ActorInterface::handle` and actor constructors as roots, then build a call graph. Side-effectful functions reachable only from bins/tests/background threads but not actor roots become candidates for migration.


8. **Side-effect import layering rule**  
   Flag `std::process`, `std::net`, `tokio::net`, `std::fs`, Docker/VastAI/SSH clients, driver joins, and datastream emitters in modules that are supposed to be pure domain state machines. Pure cores should emit commands/events, not perform effects.


10. **Runtime creation inventory**  -- If this happens at all, massive red flag.
    Enumerate every `tokio::runtime::Runtime::new` and `swactor::runtime::Runtime::new`. Runtime creation should cluster at process/runtime-stack boundaries and tests; nested or ad-hoc runtimes usually indicate imperative islands.


11. **Post-handoff control-path check** -- All bootstrap monitoring should be owned by an actor, no exceptions.
    Encode the provisioning spec as an audit rule: after `swactor` convergence/handoff, SSH/bootstrap/provider code may not remain the live node control path. Scan for SSH or bootstrap-session methods that can act after convergence without going through a node actor.


13. **Datastream emission provenance check**  
    Find direct calls that emit provisioning/readiness/fault/teardown telemetry. Control-plane telemetry should be derived from actor-observed events or actor-owned adapters; direct emission from random loops can hide imperative authority.


18. **External API client audit**  
    Identify VastAI, Docker, SSH, git, and filesystem operations. Provider plugins can perform provider I/O, but they should be stateless with respect to run authority; any retained run/node state inside the client/plugin is suspect.


19. **Ownership matrix by resource**  -- Yes, but let us be careful about resource definition to catch these.
    Build a table: resource type -> owning actor -> allowed non-actor adapter -> teardown message. Missing owner for processes, sockets, rings, leases, workers, or node records is a concrete migration target.


20. **Control-plane exception registry**  -- How about a critical section boundary, so that any unactorized code gets flagged
    Maintain a small checked-in allowlist: pure core, hot tensor byte path, startup bootstrap, pre-swactor SSH bootstrap, provider I/O adapter, test harness. Every denylist hit must match one exception or be filed as non-actor control code.



22. **Backtrace-based audit mode**  -- Yes, but not with a 'registry', and only certain critical datastructures
    Wrap side-effect APIs behind crate-local helpers and, in audit builds, record a lightweight backtrace/source tag. During e2e runs, fail or report when control-plane effects happen without an actor frame or registered bootstrap exception.



24. **Spawn wrapper migration**  -- Interesting idea, consider later. Eventually want to migrate task/thread behavior to swactor runtime, but that is currently deferred to post-alpha.
    Replace direct `thread::spawn`, `tokio::spawn`, and `Command::spawn` with crate-local wrappers like `spawn_actor_adapter`, `spawn_byte_pump`, `spawn_pre_swactor_bootstrap`, `spawn_test_helper`. The wrapper name forces classification and makes unclassified spawns easy to detect.



25. **Shadow-runtime detector**  -- Multiple runtimes should be considered always wrong until future notice.
    Flag ad-hoc Tokio runtimes or swactor runtimes not created by the runtime stack/binary bootstrap. Multiple runtimes are not always wrong, but they often correlate with code escaping the actor scheduler/control surface.


28. **Readiness/fault/teardown vocabulary scan**  
    Search emitted JSON/log labels and enum variants containing `ready`, `live`, `failed`, `fault`, `stopped`, `exited`, `teardown`, `destroyed`. These are control-plane facts; require actor observation/provenance.


33. **Test-harness exclusion rule**  
    Keep tests out of the main migration signal unless they define production-like support code reused by binaries. The crate has many e2e helpers with threads/processes; classify those separately to avoid noisy false positives.


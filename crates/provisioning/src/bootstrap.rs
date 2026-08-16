//! Actor-controlled node bootstrap.
//!
//! One [`BootstrapActor`] owns one node provision attempt end-to-end. The
//! supervision (reconciler loop) talks to every bootstrap actor through the
//! same standard interface — [`BootstrapMsg`] in, [`BootstrapEvent`] out — and
//! never learns *how* a node is launched. Target-specific behavior (local OS
//! process, docker container over ssh, leased remote host over ssh, …) lives
//! behind the private [`BootstrapLogic`] extension point, resolved from a
//! [`BootstrapRegistry`] by the spec's `kind`.
//!
//! The generic actor owns the universal lifecycle state machine
//! (`Bootstrapping → Ready → Stopping → Dead`), self-drives its probes on the
//! engine, starts telemetry collection once a node reports bootstrapped, and
//! guarantees idempotent termination. Logic implementations report raw
//! observations; the actor alone decides what the supervision sees.
//!
//! Telemetry collection is injected as a [`NodeTelemetryCollector`] hook so
//! this crate stays independent of any transport; the demo's collector dials
//! the node on `TELEMETRY_ALPN` and pulls its subscription
//! (`iroh_driver::spawn_pull_collector`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::EngineHandle;

/// Default probe period for the self-driven state machine.
pub const DEFAULT_PROBE_PERIOD: Duration = Duration::from_millis(250);

/// What the bootstrap actor learned about a node attempt.
#[derive(Clone, Debug)]
pub struct NodeIdentity {
    pub attempt: u64,
    pub logical_node: String,
    /// Transport-facing node key (e.g. iroh node id, hex); opaque here.
    pub key_hex: String,
    /// Advertised transport address of the node (format owned by the
    /// collector implementation); opaque here.
    pub transport_addr: String,
}

/// Standard events every bootstrap actor reports upward. These are the
/// supervision-facing interface: identical for every bootstrap kind.
#[derive(Clone, Debug)]
pub enum BootstrapEvent {
    /// The node process came up and joined: identity known, telemetry
    /// collectible. Reported at most once per attempt.
    Bootstrapped(NodeIdentity),
    /// The attempt failed before becoming ready (spawn failure, early exit,
    /// terminal transport error). The reconciler retries with a fresh lease.
    Failed { attempt: u64, reason: String },
    /// A previously-bootstrapped node process exited (death, not bootstrap
    /// failure). The reconciler replaces it.
    Exited { attempt: u64, reason: String },
}

/// How a report leaves the actor: an app-supplied closure that routes the
/// event into the supervision's message enum.
pub type BootstrapReporter = Arc<dyn Fn(BootstrapEvent) + Send + Sync>;

/// Standard commands the supervision sends to any bootstrap actor.
#[derive(Clone, Debug)]
pub enum BootstrapMsg {
    /// Begin the attempt: launch the node process.
    Start,
    /// Terminate the node process. Idempotent; `kill_after` escalates.
    Stop { kill_after: Option<Duration> },
    /// Internal: self-driven state probe. Sending it externally is harmless.
    Probe,
    /// The node announced itself over the control plane: its transport
    /// identity facts arrived from the wire (key + advertised address),
    /// routed here by the app's announce plumbing. First delivery while
    /// bootstrapping completes the attempt; later deliveries are heartbeat
    /// duplicates and are ignored.
    Announce(NodeIdentity),
}

#[derive(Clone, Debug)]
/// Neutral description of one node launch, independent of bootstrap kind.
pub struct NodeLaunchSpec {
    /// Bootstrap kind; selects the logic from the registry.
    pub kind: String,
    pub attempt: u64,
    pub logical_node: String,
    /// Semantic argv the *node* runs (before any transport encoding).
    pub argv: Vec<String>,
    pub env: Vec<(String, String)>,
    pub workdir: Option<std::path::PathBuf>,
    pub label: Option<String>,
}

/// One raw probe observation from a logic implementation. The actor applies
/// state-machine rules (once-only reporting, phase filtering); logics stay
/// dumb readers of their foreign process.
pub enum LogicProbe {
    /// Still coming up; nothing to report.
    Pending,
    /// Node process is up and joined.
    Bootstrapped(NodeIdentity),
    /// Attempt failed while bootstrapping.
    Failed(String),
    /// Node process exited after having reported bootstrapped.
    Exited(String),
}

/// Target-specific bootstrap behavior. Implementations live in application
/// crates and may capture app state (registries, join checks, key files).
pub trait BootstrapLogic: Send {
    /// Launch the node process. Called once, in actor context, with the
    /// owning actor's address (`owner`) so the logic can register itself in
    /// app-side registries. An error is a terminal attempt failure.
    fn start(
        &mut self,
        ctx: &Ctx,
        owner: ActorAddress,
        sender: &ExternalSender,
    ) -> Result<(), String>;
    /// Non-blocking state probe (key-file reads, registry lookups, exit
    /// polls). Never blocks; called every probe period.
    fn probe(&mut self, now: SystemTime) -> LogicProbe;
    /// Terminate the node process (best effort, idempotent).
    fn terminate(&mut self, sender: &ExternalSender, kill_after: Option<Duration>);
}

/// Telemetry collection hook the actor fires once per attempt, right after
/// `Bootstrapped` is reported. Implementations dial the node and pull.
pub trait NodeTelemetryCollector: Send + Sync {
    fn collect(&self, identity: &NodeIdentity);
}

/// Creates a [`BootstrapLogic`] for one launch spec.
pub type BootstrapFactory =
    Arc<dyn Fn(&NodeLaunchSpec) -> Result<Box<dyn BootstrapLogic>, String> + Send + Sync>;

/// Kind-keyed logic registry: adding bootstrap type #N is a new factory
/// registration; the supervision never changes.
#[derive(Default)]
pub struct BootstrapRegistry {
    factories: BTreeMap<String, BootstrapFactory>,
}

impl BootstrapRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, kind: &str, factory: BootstrapFactory) {
        self.factories.insert(kind.to_owned(), factory);
    }

    /// Resolve the logic named by `spec.kind`.
    pub fn create(&self, spec: &NodeLaunchSpec) -> Result<Box<dyn BootstrapLogic>, String> {
        self.factories
            .get(&spec.kind)
            .ok_or_else(|| format!("no bootstrap logic registered for kind '{}'", spec.kind))?
            .clone()(spec)
    }
}

/// Universal per-attempt lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Launched, no readiness signal yet.
    Bootstrapping,
    /// Node joined and reported bootstrapped.
    Ready,
    /// Termination requested, waiting for the exit observation.
    Stopping,
    /// Terminal: exit observed (or start failed). No further reports.
    Dead,
}

/// Configuration for one spawned bootstrap actor.
pub struct BootstrapConfig {
    pub spec: NodeLaunchSpec,
    /// Routes events into the supervision's message enum.
    pub reporter: BootstrapReporter,
    /// Sender the logic uses for process commands.
    pub sender: ExternalSender,
    /// Optional telemetry collection hook, fired once after `Bootstrapped`.
    pub collector: Option<Arc<dyn NodeTelemetryCollector>>,
    pub probe_period: Duration,
}

/// The generic, transport-agnostic bootstrap actor. See the module docs.
pub struct BootstrapActor {
    attempt: u64,
    logic: Box<dyn BootstrapLogic>,
    reporter: BootstrapReporter,
    sender: ExternalSender,
    collector: Option<Arc<dyn NodeTelemetryCollector>>,
    phase: Phase,
    /// `Bootstrapped` reported (and collector fired) exactly once.
    announced: bool,
    /// Terminal report (Failed/Exited) emitted exactly once.
    closed: bool,
}

impl BootstrapActor {
    pub fn new(logic: Box<dyn BootstrapLogic>, config: BootstrapConfig) -> Self {
        Self {
            attempt: config.spec.attempt,
            logic,
            reporter: config.reporter,
            sender: config.sender,
            collector: config.collector,
            phase: Phase::Bootstrapping,
            announced: false,
            closed: false,
        }
    }

    fn report(&self, event: BootstrapEvent) {
        (self.reporter)(event);
    }

    fn fail(&mut self, reason: String) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.phase = Phase::Dead;
        self.report(BootstrapEvent::Failed {
            attempt: self.attempt,
            reason,
        });
    }

    fn exit(&mut self, reason: String) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.phase = Phase::Dead;
        self.report(BootstrapEvent::Exited {
            attempt: self.attempt,
            reason,
        });
    }

    fn probe(&mut self, now: SystemTime) {
        match self.phase {
            Phase::Dead => {}
            Phase::Bootstrapping => match self.logic.probe(now) {
                LogicProbe::Pending => {}
                LogicProbe::Bootstrapped(identity) => self.become_ready(identity),
                LogicProbe::Failed(reason) => self.fail(reason),
                // An exit before the join signal is a failed attempt.
                LogicProbe::Exited(reason) => self.fail(format!("node process exited: {reason}")),
            },
            Phase::Ready | Phase::Stopping => match self.logic.probe(now) {
                LogicProbe::Exited(reason) => {
                    if self.phase == Phase::Stopping && !self.announced {
                        // Stopped before it ever joined: failed attempt.
                        self.fail(format!("stopped before join: {reason}"));
                    } else {
                        self.exit(reason);
                    }
                }
                _ => {}
            },
        }
    }

    /// First readiness signal for the attempt (probe or wire announce):
    /// fire the collector and report `Bootstrapped` exactly once.
    fn become_ready(&mut self, identity: NodeIdentity) {
        if self.announced {
            return;
        }
        self.announced = true;
        self.phase = Phase::Ready;
        if let Some(collector) = &self.collector {
            collector.collect(&identity);
        }
        self.report(BootstrapEvent::Bootstrapped(identity));
    }

    /// Apply a wire announce: first delivery for this attempt completes the
    /// bootstrap; anything else (mismatched attempt, heartbeat duplicates,
    /// terminal phases) is a drop.
    fn apply_announce(&mut self, identity: NodeIdentity) {
        if identity.attempt != self.attempt {
            return; // misrouted: drop rather than misreport
        }
        if self.phase == Phase::Bootstrapping {
            self.become_ready(identity);
        }
    }
}

impl ActorInterface for BootstrapActor {
    type Incoming = BootstrapMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: BootstrapMsg) {
        match msg {
            BootstrapMsg::Start => {
                if self.phase != Phase::Bootstrapping {
                    return; // Restart of a started/stopped attempt: ignore.
                }
                if let Err(reason) = self.logic.start(ctx, ctx.self_addr(), &self.sender) {
                    self.fail(format!("launch failed: {reason}"));
                }
            }
            BootstrapMsg::Stop { kill_after } => {
                if self.phase == Phase::Dead {
                    return;
                }
                self.phase = Phase::Stopping;
                self.logic.terminate(&self.sender, kill_after);
            }
            BootstrapMsg::Probe => {
                self.probe(SystemTime::now());
                if self.phase == Phase::Dead {
                    ctx.stop_self();
                }
            }
            BootstrapMsg::Announce(identity) => self.apply_announce(identity),
        }
    }
}

/// Spawn one bootstrap actor with a self-driven probe interval: the actor,
/// not the supervision tick, owns its progression. `Start` is sent after the
/// interval is installed so the logic never races its own probes.
pub fn spawn_bootstrap_actor(
    ctx: &Ctx,
    engine: &EngineHandle,
    logic: Box<dyn BootstrapLogic>,
    config: BootstrapConfig,
) -> Result<ActorAddress, String> {
    let sender = config.sender.clone();
    let start_sender = sender.clone();
    let period = config.probe_period;
    let actor = ctx
        .spawn(BootstrapActor::new(logic, config))
        .map_err(|error| format!("spawn bootstrap actor: {error}"))?;
    let probe_engine = engine.clone();
    engine.spawn(async move {
        let mut interval = probe_engine.interval(period);
        loop {
            (&mut interval).await;
            if sender.send_to(actor, BootstrapMsg::Probe).is_err() {
                return;
            }
        }
    });
    let _ = start_sender.send_to(actor, BootstrapMsg::Start);
    Ok(actor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ScriptedLogic {
        probes: Mutex<Vec<LogicProbe>>,
        started: AtomicUsize,
        terminated: AtomicUsize,
    }

    impl ScriptedLogic {
        fn new(probes: Vec<LogicProbe>) -> Self {
            Self {
                probes: Mutex::new(probes),
                started: AtomicUsize::new(0),
                terminated: AtomicUsize::new(0),
            }
        }
    }

    impl BootstrapLogic for ScriptedLogic {
        fn start(
            &mut self,
            _ctx: &Ctx,
            _owner: ActorAddress,
            _sender: &ExternalSender,
        ) -> Result<(), String> {
            self.started.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn probe(&mut self, _now: SystemTime) -> LogicProbe {
            self.probes.lock().remove(0)
        }
        fn terminate(&mut self, _sender: &ExternalSender, _kill_after: Option<Duration>) {
            self.terminated.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn identity(attempt: u64) -> NodeIdentity {
        NodeIdentity {
            attempt,
            logical_node: format!("node-{attempt}"),
            key_hex: format!("key{attempt}"),
            transport_addr: format!("addr{attempt}"),
        }
    }

    #[test]
    fn registry_resolves_by_kind() {
        let mut registry = BootstrapRegistry::new();
        registry.register(
            "process",
            Arc::new(|_spec| Ok(Box::new(ScriptedLogic::new(vec![])) as Box<dyn BootstrapLogic>)),
        );
        let spec = NodeLaunchSpec {
            kind: "process".to_owned(),
            attempt: 1,
            logical_node: "node-1".to_owned(),
            argv: vec![],
            env: vec![],
            workdir: None,
            label: None,
        };
        assert!(registry.create(&spec).is_ok());
        let unknown = NodeLaunchSpec {
            kind: "nope".to_owned(),
            ..spec
        };
        assert!(registry.create(&unknown).is_err());
    }
    #[test]
    fn reporter_contract_carries_identity() {
        let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let reporter: BootstrapReporter = {
            let events = events.clone();
            Arc::new(move |event| {
                let name = match event {
                    BootstrapEvent::Bootstrapped(_) => "bootstrapped",
                    BootstrapEvent::Failed { .. } => "failed",
                    BootstrapEvent::Exited { .. } => "exited",
                };
                events.lock().push(name.to_owned());
            })
        };
        reporter(BootstrapEvent::Exited {
            attempt: 7,
            reason: "test".to_owned(),
        });
        assert_eq!(events.lock().as_slice(), ["exited"]);
        let id = identity(7);
        assert_eq!(id.logical_node, "node-7");
    }

    #[test]
    fn announce_completes_attempt_exactly_once() {
        let events: Arc<Mutex<Vec<BootstrapEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let mut actor = announce_actor(7, events.clone(), vec![]);

        actor.apply_announce(identity(7));
        assert_eq!(actor.phase, Phase::Ready);
        // Heartbeat duplicates: ignored.
        actor.apply_announce(identity(7));
        actor.apply_announce(identity(7));
        let logged = events.lock();
        assert_eq!(logged.len(), 1);
        assert!(matches!(&logged[0], BootstrapEvent::Bootstrapped(id) if id.key_hex == "key7"));
    }

    #[test]
    fn announce_with_mismatched_attempt_is_dropped() {
        let events: Arc<Mutex<Vec<BootstrapEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let mut actor = announce_actor(7, events.clone(), vec![]);

        actor.apply_announce(identity(8));
        assert_eq!(actor.phase, Phase::Bootstrapping);
        assert!(events.lock().is_empty());
    }

    #[test]
    fn announce_after_terminal_phase_is_dropped() {
        let events: Arc<Mutex<Vec<BootstrapEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let mut actor = announce_actor(7, events.clone(), vec![]);

        actor.apply_announce(identity(7));
        actor.exit("killed".to_owned());
        // A stale container from the dead attempt still announcing.
        actor.apply_announce(identity(7));
        let logged = events.lock();
        assert_eq!(logged.len(), 2);
        assert!(matches!(&logged[1], BootstrapEvent::Exited { attempt, .. } if *attempt == 7));
    }

    #[test]
    fn announce_after_probe_bootstrapped_does_not_double_report() {
        let events: Arc<Mutex<Vec<BootstrapEvent>>> = Arc::new(Mutex::new(Vec::new()));
        // Probe path reports Bootstrapped first…
        let mut actor = announce_actor(
            9,
            events.clone(),
            vec![LogicProbe::Bootstrapped(identity(9))],
        );
        actor.probe(SystemTime::now());
        // …then the announce for the same attempt arrives late.
        actor.apply_announce(identity(9));
        let logged = events.lock();
        assert_eq!(logged.len(), 1);
    }

    /// An actor over a scripted logic whose events land in `events`.
    fn announce_actor(
        attempt: u64,
        events: Arc<Mutex<Vec<BootstrapEvent>>>,
        probes: Vec<LogicProbe>,
    ) -> BootstrapActor {
        let reporter: BootstrapReporter = {
            let events = events.clone();
            Arc::new(move |event| events.lock().push(event))
        };
        let sender = swactor::runtime::RuntimeParts::new(swactor::config::RuntimeConfig::default())
            .runtime()
            .create_sender();
        let config = BootstrapConfig {
            spec: NodeLaunchSpec {
                kind: "process".to_owned(),
                attempt,
                logical_node: format!("node-{attempt}"),
                argv: vec![],
                env: vec![],
                workdir: None,
                label: None,
            },
            reporter,
            sender,
            collector: None,
            probe_period: DEFAULT_PROBE_PERIOD,
        };
        BootstrapActor::new(Box::new(ScriptedLogic::new(probes)), config)
    }
}

use std::thread;
use std::time::{Duration, Instant};

use distribution::node::DistributedNodeConfig;
use distribution::types::{DirectoryEntry, NodeId};
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use swactor::actor::ActorAddress;

use crate::actors::node_agent::NodeAgentActor;
use crate::actors::orchestrator::OrchestratorActor;
use crate::actors::register_mvp_actor_codecs;
use crate::distribution_stack::DistributionRuntimeStack;
use crate::orchestrator_run_fsm as orchestrator_core;
use crate::stage_controller as stage_core;

pub struct RuntimeNodeConfig {
    pub distributed: DistributedNodeConfig,
    pub relay_mode: iroh::RelayMode,
}

impl Default for RuntimeNodeConfig {
    fn default() -> Self {
        Self {
            distributed: DistributedNodeConfig::default(),
            relay_mode: iroh::RelayMode::Disabled,
        }
    }
}

pub struct RuntimeNode {
    _tokio: tokio::runtime::Runtime,
    driver: IrohDriver,
    stack: DistributionRuntimeStack,
}

impl RuntimeNode {
    pub fn start_default() -> Result<Self, RuntimeNodeError> {
        Self::start_with_codecs(RuntimeNodeConfig::default(), |_| {})
    }

    pub fn start_with_codecs(
        config: RuntimeNodeConfig,
        extend_codecs: impl FnOnce(&mut swactor_transport::CodecRegistry),
    ) -> Result<Self, RuntimeNodeError> {
        let tokio = tokio::runtime::Runtime::new()
            .map_err(|err| RuntimeNodeError::Start(format!("tokio runtime: {err}")))?;
        let mut driver = IrohDriver::with_handle(
            tokio.handle().clone(),
            IrohDriverConfig {
                secret_key: None,
                relay_mode: config.relay_mode,
                node: config.distributed.clone(),
                peer_auth: None,
                additional_alpns: vec![],
            },
        )
        .map_err(|err| RuntimeNodeError::Start(format!("iroh driver: {err}")))?;
        let stack = DistributionRuntimeStack::new_with_codecs(
            driver.node_id(),
            config.distributed,
            |registry| {
                register_mvp_actor_codecs(registry);
                extend_codecs(registry);
            },
        );
        driver.enable_actor_bridge(
            stack.runtime.clone(),
            stack.codec.clone(),
            stack.actor_bridge_routes(),
            stack.actors.swim,
            stack.relay_mirror.clone(),
            stack.route_view.clone(),
        );
        Ok(Self {
            _tokio: tokio,
            driver,
            stack,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.driver.node_id()
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.driver.endpoint_addr()
    }

    pub fn join(&mut self, coordinators: &[EndpointAddr]) {
        self.driver.join(coordinators);
    }

    pub fn register_actor_route(&mut self, actor_addr: ActorAddress, generation: u64) {
        let entry = self.driver.register_actor(actor_addr, generation);
        self.stack.register_local_actor(entry);
    }

    pub fn register_directory_entry(&self, entry: DirectoryEntry) {
        self.stack.register_local_actor(entry);
    }

    pub fn spawn_orchestrator_actor(
        &mut self,
        config: orchestrator_core::RunConfig,
        report_to: Option<ActorAddress>,
    ) -> Result<ActorAddress, RuntimeNodeError> {
        let actor = self
            .stack
            .runtime
            .spawn(OrchestratorActor::new(config, report_to))
            .map_err(|err| RuntimeNodeError::Start(format!("spawn orchestrator actor: {err}")))?;
        self.register_actor_route(actor, 1);
        Ok(actor)
    }

    pub fn spawn_node_agent_actor(
        &mut self,
        local_node_id: stage_core::NodeId,
        orchestrator: ActorAddress,
        report_to: Option<ActorAddress>,
    ) -> Result<ActorAddress, RuntimeNodeError> {
        let actor = self
            .stack
            .runtime
            .spawn(NodeAgentActor::new(local_node_id, orchestrator, report_to))
            .map_err(|err| RuntimeNodeError::Start(format!("spawn node agent actor: {err}")))?;
        self.register_actor_route(actor, 1);
        Ok(actor)
    }

    pub fn pump_once(&mut self) {
        self.stack.tick_protocol_actors(Instant::now());
        self.driver.pump_inbound_to_actors();
        self.stack.pump_runtime_once();
        self.driver.drain_outbox(&self.stack.outbox);
    }

    pub fn wait_for_routes(&mut self, actors: &[ActorAddress]) -> Result<(), RuntimeNodeError> {
        loop {
            self.pump_once();
            let ready = self
                .stack
                .route_view
                .read()
                .map(|view| actors.iter().all(|actor| view.contains_key(actor)))
                .unwrap_or(false);
            if ready {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn alive_count(&self) -> usize {
        self.stack.alive_count()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeNodeError {
    Start(String),
}

impl std::fmt::Display for RuntimeNodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start(message) => write!(f, "runtime node start failed: {message}"),
        }
    }
}

impl std::error::Error for RuntimeNodeError {}

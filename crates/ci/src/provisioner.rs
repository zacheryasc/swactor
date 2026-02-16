//! Provisioner actor: manages spot instance lifecycle via pluggable provider scripts.
//!
//! In real deployment, runs on the developer's laptop and calls cloud provider APIs.
//! In simulation, provisions are driven by the simulation harness.

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::coordinator::CoordinatorMsg;
use crate::{
    InstanceReady, ProvisionError, ProvisionRequest, ProvisionResponse, TerminateRequest,
};

/// Messages the Provisioner can receive.
#[derive(Debug, Clone)]
pub enum ProvisionerMsg {
    /// Request to provision a new spot instance.
    Provision(ProvisionRequest),
    /// Request to terminate a spot instance.
    Terminate(TerminateRequest),
    /// Simulated: provisioning result delivered asynchronously.
    SimProvisionResult {
        request: ProvisionRequest,
        result: Result<InstanceReady, ProvisionError>,
    },
}

/// Provisioner actor state.
///
/// In real deployment, this would invoke provider scripts.
/// In simulation, the sim harness controls provision outcomes.
pub struct Provisioner {
    coordinator_addr: ActorAddress,
    /// Active instances tracked for cleanup.
    active_instances: Vec<String>,
}

impl Provisioner {
    pub fn new(coordinator_addr: ActorAddress) -> Self {
        Self {
            coordinator_addr,
            active_instances: Vec::new(),
        }
    }

    pub fn active_instances(&self) -> &[String] {
        &self.active_instances
    }
}

impl ActorInterface for Provisioner {
    type Incoming = ProvisionerMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ProvisionerMsg) {
        match msg {
            ProvisionerMsg::Provision(_request) => {
                // In real deployment: invoke provider script, await result.
                // In simulation: the sim harness sends SimProvisionResult.
            }
            ProvisionerMsg::Terminate(request) => {
                self.active_instances.retain(|id| id != &request.instance_id);
                // In real deployment: invoke provider destroy script.
                // In simulation: just track the termination.
                let _ = ctx.send(
                    self.coordinator_addr,
                    CoordinatorMsg::ProvisionResponse(ProvisionResponse {
                        job_id: request.job_id.clone(),
                        result: Err(ProvisionError::NoCapacity), // placeholder, terminate doesn't need response
                    }),
                );
            }
            ProvisionerMsg::SimProvisionResult { request, result } => {
                if let Ok(ref instance) = result {
                    self.active_instances.push(instance.instance_id.clone());
                }
                let _ = ctx.send(
                    self.coordinator_addr,
                    CoordinatorMsg::ProvisionResponse(ProvisionResponse {
                        job_id: request.job_id,
                        result,
                    }),
                );
            }
        }
    }
}

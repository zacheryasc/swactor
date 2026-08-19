//! Synchronous facade for concrete Vast.ai API operations.
//!
//! The provider-I/O crate owns the Tokio runtime used to drive HTTP requests;
//! domain actors receive only operation results and never drive a runtime.

use crate::{
    CreateInstanceRequest, InstanceInfo, LabeledInstance, LifecyclePolicy, Offer,
    OfferBrowseCriteria, ProviderInstanceStatus, RunningInstance, SelectionPolicy, VastClient,
};
use std::time::Duration;

pub struct BlockingVastClient {
    client: VastClient,
    runtime: tokio::runtime::Runtime,
}

impl BlockingVastClient {
    pub fn new(client: VastClient) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("build Vast.ai I/O runtime: {error}"))?;
        Ok(Self { client, runtime })
    }

    pub fn browse_offers(&self, criteria: &OfferBrowseCriteria) -> Result<Vec<Offer>, String> {
        self.runtime.block_on(self.client.browse_offers(criteria))
    }

    pub fn search_offers(
        &self,
        policy: &SelectionPolicy,
        target_count: u32,
    ) -> Result<Vec<Offer>, String> {
        self.runtime
            .block_on(self.client.search_offers(policy, target_count))
    }

    pub fn create_instance(&self, request: &CreateInstanceRequest) -> Result<InstanceInfo, String> {
        self.runtime.block_on(self.client.create_instance(request))
    }

    pub fn instance_status(&self, contract_id: u64) -> Result<ProviderInstanceStatus, String> {
        self.runtime
            .block_on(self.client.instance_status(contract_id))
    }

    pub fn list_by_label(&self, label: &str) -> Result<Vec<LabeledInstance>, String> {
        self.runtime.block_on(self.client.list_by_label(label))
    }

    pub fn list_by_label_with_retry(
        &self,
        label: &str,
        attempts: usize,
        pace: Duration,
    ) -> Result<Vec<LabeledInstance>, String> {
        let attempts = attempts.max(1);
        for attempt in 0..attempts {
            let instances = self.list_by_label(label)?;
            if !instances.is_empty() || attempt + 1 == attempts {
                return Ok(instances);
            }
            std::thread::sleep(pace);
        }
        unreachable!("at least one Vast.ai label lookup attempt runs")
    }

    pub fn wait_for_ssh_endpoint(
        &self,
        contract_id: u64,
        label: &str,
        policy: &LifecyclePolicy,
    ) -> Result<RunningInstance, String> {
        self.runtime.block_on(
            self.client
                .wait_for_ssh_endpoint(contract_id, label, policy),
        )
    }

    pub fn destroy_instance_with_retry(&self, contract_id: u64) -> Result<(), String> {
        self.runtime
            .block_on(self.client.destroy_instance_with_retry(contract_id))
    }
}

impl Clone for BlockingVastClient {
    fn clone(&self) -> Self {
        Self::new(self.client.clone()).expect("clone Vast.ai I/O runtime")
    }
}

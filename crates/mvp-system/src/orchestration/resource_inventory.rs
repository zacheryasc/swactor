#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GpuClass {
    TestSmall,
    TestLarge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootHealth {
    Ready,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InventoryEntry {
    pub node_id: NodeId,
    pub gpu_class: GpuClass,
    pub ready: bool,
}

impl InventoryEntry {
    pub fn ready(node_id: NodeId, gpu_class: GpuClass) -> Self {
        Self {
            node_id,
            gpu_class,
            ready: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagePlacement {
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementInput {
    FixedLinear(Vec<StagePlacement>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanningRequest {
    pub run_id: RunId,
    pub stage_count: u32,
    pub entries: Vec<InventoryEntry>,
    pub placement: PlacementInput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagePlan {
    pub stage_index: u32,
    pub node_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPlan {
    pub run_id: RunId,
    pub stages: Vec<StagePlan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InventoryRejectionKind {
    UnknownNode { node_id: NodeId },
    DuplicateStage { stage_index: u32 },
    MissingStage { stage_index: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InventoryRejection {
    pub kind: InventoryRejectionKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeReport {
    BootHealth {
        node_id: NodeId,
        health: BootHealth,
    },
    CapacityChanged {
        node_id: NodeId,
        gpu_class: GpuClass,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InventoryCommand {
    AcceptPlacementNegotiation { node_id: NodeId },
    RewriteStagePlacement { node_id: NodeId },
    ProvisionStage { stage_index: u32, node_id: NodeId },
}

pub fn plan_from_inventory(request: PlanningRequest) -> Result<RunPlan, InventoryRejection> {
    let known_nodes = request
        .entries
        .iter()
        .map(|entry| entry.node_id)
        .collect::<std::collections::BTreeSet<_>>();
    let PlacementInput::FixedLinear(stages) = request.placement;
    let mut by_stage = std::collections::BTreeMap::new();
    for placement in stages {
        if !known_nodes.contains(&placement.node_id) {
            return Err(InventoryRejection {
                kind: InventoryRejectionKind::UnknownNode {
                    node_id: placement.node_id,
                },
            });
        }
        if by_stage
            .insert(placement.stage_index, placement.node_id)
            .is_some()
        {
            return Err(InventoryRejection {
                kind: InventoryRejectionKind::DuplicateStage {
                    stage_index: placement.stage_index,
                },
            });
        }
    }

    let mut stages = Vec::with_capacity(request.stage_count as usize);
    for stage_index in 0..request.stage_count {
        let node_id = by_stage.remove(&stage_index).ok_or(InventoryRejection {
            kind: InventoryRejectionKind::MissingStage { stage_index },
        })?;
        stages.push(StagePlan {
            stage_index,
            node_id,
        });
    }
    Ok(RunPlan {
        run_id: request.run_id,
        stages,
    })
}

#[cfg(test)]
pub struct InventoryHarness {
    entries: Vec<InventoryEntry>,
    commands: Vec<InventoryCommand>,
    committed_plan: Option<RunPlan>,
}

#[cfg(test)]
impl InventoryHarness {
    pub fn new(entries: Vec<InventoryEntry>) -> Self {
        Self {
            entries,
            commands: Vec::new(),
            committed_plan: None,
        }
    }

    pub fn entries(&self) -> &[InventoryEntry] {
        &self.entries
    }

    pub fn commands(&self) -> &[InventoryCommand] {
        &self.commands
    }

    pub fn committed_plan(&self) -> Option<&RunPlan> {
        self.committed_plan.as_ref()
    }

    pub fn observe_node_report(&mut self, report: NodeReport) {
        match report {
            NodeReport::BootHealth { node_id, health } => {
                if let Some(entry) = self
                    .entries
                    .iter_mut()
                    .find(|entry| entry.node_id == node_id)
                {
                    entry.ready = matches!(health, BootHealth::Ready);
                }
            }
            NodeReport::CapacityChanged { .. } => {}
        }
    }

    pub fn commit_plan(&mut self, plan: RunPlan) {
        self.committed_plan = Some(plan);
    }

    pub fn provision_stages(&mut self) {
        if let Some(plan) = &self.committed_plan {
            for stage in &plan.stages {
                self.commands.push(InventoryCommand::ProvisionStage {
                    stage_index: stage.stage_index,
                    node_id: stage.node_id,
                });
            }
        }
    }
}

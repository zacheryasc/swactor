use std::collections::BTreeMap;
use std::sync::Arc;

use datastream::DatastreamProducer;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender};
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::provisioning::{
    NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginObservationSink, PluginSink,
    ProvisionEvent, ProvisionEventKind, ProvisionLogLine, ProvisionLogStream, ProvisionPlugin,
};
use crate::telemetry::{MvpProvisionEventRecord, MvpProvisionLogRecord, mvp_provision_log_channel};

use super::codec::JsonCodec;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProvisionerMsg {
    StartNodes {
        nodes: Vec<NodeProvisionSpec>,
        reply_to: ActorAddress,
    },
    StopNodes {
        run_id: u64,
        reply_to: ActorAddress,
    },
    PluginObservation(PluginObservation),
}

impl NetworkMessage for ProvisionerMsg {
    fn type_tag() -> &'static str {
        "mvp_system::ProvisionerMsg"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProvisionerReport {
    NodeLive {
        run_id: u64,
        node_id: u64,
        stage_index: Option<u32>,
        endpoint: iroh::EndpointAddr,
        node_actor: ActorAddress,
        provider_process_id: Option<u32>,
    },
    NodeFailed {
        run_id: u64,
        node_id: u64,
        reason: String,
    },
    LogLine {
        run_id: u64,
        node_id: u64,
        stream: ProvisionLogStream,
        line: String,
    },
    NodesStopped {
        run_id: u64,
    },
}

impl NetworkMessage for ProvisionerReport {
    fn type_tag() -> &'static str {
        "mvp_system::ProvisionerReport"
    }
}

pub struct ProvisionerActor<P: ProvisionPlugin> {
    plugin: P,
    sender: ExternalSender,
    telemetry: Option<DatastreamProducer>,
    runs: BTreeMap<u64, RunProvision>,
}

struct RunProvision {
    nodes: BTreeMap<u64, NodeSlot>,
}

struct NodeSlot {
    handle: PluginNodeHandle,
    live: bool,
    stage_index: Option<u32>,
    reply_to: ActorAddress,
}

impl<P: ProvisionPlugin> ProvisionerActor<P> {
    pub fn new(plugin: P, sender: ExternalSender, telemetry: Option<DatastreamProducer>) -> Self {
        Self {
            plugin,
            sender,
            telemetry,
            runs: BTreeMap::new(),
        }
    }

    fn start_nodes(&mut self, ctx: &Ctx, nodes: Vec<NodeProvisionSpec>, reply_to: ActorAddress) {
        for spec in nodes {
            self.emit_event(ProvisionEvent {
                run_id: spec.run_id,
                node_id: spec.node_id,
                kind: ProvisionEventKind::ProvisionStart,
                message: None,
            });
            let sink = PluginSink::new(Arc::new(ActorPluginSink {
                sender: self.sender.clone(),
                addr: ctx.self_addr(),
            }));
            match self.plugin.start_node(spec.clone(), sink) {
                Ok(handle) => {
                    self.runs
                        .entry(spec.run_id)
                        .or_insert_with(|| RunProvision {
                            nodes: BTreeMap::new(),
                        })
                        .nodes
                        .insert(
                            spec.node_id,
                            NodeSlot {
                                handle,
                                live: false,
                                stage_index: spec.stage_index,
                                reply_to,
                            },
                        );
                }
                Err(reason) => {
                    self.emit_failed(spec.run_id, spec.node_id, &reason);
                    let _ = ctx.send(
                        reply_to,
                        ProvisionerReport::NodeFailed {
                            run_id: spec.run_id,
                            node_id: spec.node_id,
                            reason,
                        },
                    );
                }
            }
        }
    }

    fn stop_nodes(&mut self, ctx: &Ctx, run_id: u64, reply_to: ActorAddress) {
        if let Some(run) = self.runs.remove(&run_id) {
            for (node_id, slot) in run.nodes {
                let stop_result = self.plugin.stop_node(&slot.handle);
                let message = stop_result.err();
                self.emit_event(ProvisionEvent {
                    run_id,
                    node_id,
                    kind: ProvisionEventKind::NodeStopped,
                    message,
                });
            }
        }
        let _ = ctx.send(reply_to, ProvisionerReport::NodesStopped { run_id });
    }

    fn observe_plugin(&mut self, ctx: &Ctx, observation: PluginObservation) {
        match observation {
            PluginObservation::StdoutLine {
                run_id,
                node_id,
                line,
            } => self.forward_log(
                ctx,
                ProvisionLogLine {
                    run_id,
                    node_id,
                    stream: ProvisionLogStream::Stdout,
                    line,
                },
            ),
            PluginObservation::StderrLine {
                run_id,
                node_id,
                line,
            } => self.forward_log(
                ctx,
                ProvisionLogLine {
                    run_id,
                    node_id,
                    stream: ProvisionLogStream::Stderr,
                    line,
                },
            ),
            PluginObservation::ProviderLine {
                run_id,
                node_id,
                line,
            } => self.forward_log(
                ctx,
                ProvisionLogLine {
                    run_id,
                    node_id,
                    stream: ProvisionLogStream::Provider,
                    line,
                },
            ),
            PluginObservation::RuntimeReady {
                run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
            } => {
                let report = self.mark_live(run_id, node_id, stage_index);
                if let Some((reply_to, provider_process_id, resolved_stage_index)) = report {
                    self.emit_event(ProvisionEvent {
                        run_id,
                        node_id,
                        kind: ProvisionEventKind::NodeLive,
                        message: None,
                    });
                    let _ = ctx.send(
                        reply_to,
                        ProvisionerReport::NodeLive {
                            run_id,
                            node_id,
                            stage_index: resolved_stage_index,
                            endpoint,
                            node_actor,
                            provider_process_id,
                        },
                    );
                }
            }
            PluginObservation::Exited {
                run_id,
                node_id,
                status,
            } => {
                let failed = self.remove_exited_node(run_id, node_id, status);
                if let Some((reply_to, reason)) = failed {
                    self.emit_failed(run_id, node_id, &reason);
                    let _ = ctx.send(
                        reply_to,
                        ProvisionerReport::NodeFailed {
                            run_id,
                            node_id,
                            reason,
                        },
                    );
                }
            }
            PluginObservation::Failed {
                run_id,
                node_id,
                reason,
            } => {
                let reply_to = self
                    .runs
                    .get(&run_id)
                    .and_then(|run| run.nodes.get(&node_id))
                    .map(|slot| slot.reply_to);
                self.emit_failed(run_id, node_id, &reason);
                if let Some(reply_to) = reply_to {
                    let _ = ctx.send(
                        reply_to,
                        ProvisionerReport::NodeFailed {
                            run_id,
                            node_id,
                            reason,
                        },
                    );
                }
            }
        }
    }

    fn mark_live(
        &mut self,
        run_id: u64,
        node_id: u64,
        observed_stage_index: Option<u32>,
    ) -> Option<(ActorAddress, Option<u32>, Option<u32>)> {
        let slot = self.runs.get_mut(&run_id)?.nodes.get_mut(&node_id)?;
        if slot.live {
            return None;
        }
        slot.live = true;
        if observed_stage_index.is_some() {
            slot.stage_index = observed_stage_index;
        }
        Some((
            slot.reply_to,
            slot.handle.provider_process_id,
            slot.stage_index,
        ))
    }

    fn remove_exited_node(
        &mut self,
        run_id: u64,
        node_id: u64,
        status: Option<i32>,
    ) -> Option<(ActorAddress, String)> {
        let run = self.runs.get_mut(&run_id)?;
        let slot = run.nodes.remove(&node_id)?;
        if run.nodes.is_empty() {
            self.runs.remove(&run_id);
        }
        let exit_was_clean = status == Some(0);
        if slot.live && exit_was_clean {
            return None;
        }
        Some((
            slot.reply_to,
            format!("node process exited before clean stop: {status:?}"),
        ))
    }

    fn emit_failed(&self, run_id: u64, node_id: u64, reason: &str) {
        self.emit_event(ProvisionEvent {
            run_id,
            node_id,
            kind: ProvisionEventKind::ProvisionFailed,
            message: Some(reason.to_owned()),
        });
    }

    fn forward_log(&self, ctx: &Ctx, line: ProvisionLogLine) {
        self.emit_log(line.clone());
        if let Some(reply_to) = self
            .runs
            .get(&line.run_id)
            .and_then(|run| run.nodes.get(&line.node_id))
            .map(|slot| slot.reply_to)
        {
            let _ = ctx.send(
                reply_to,
                ProvisionerReport::LogLine {
                    run_id: line.run_id,
                    node_id: line.node_id,
                    stream: line.stream,
                    line: line.line,
                },
            );
        }
    }

    fn emit_event(&self, event: ProvisionEvent) {
        if let Some(producer) = &self.telemetry {
            producer.submit_record(&MvpProvisionEventRecord::new(event));
        }
    }

    fn emit_log(&self, line: ProvisionLogLine) {
        if let Some(producer) = &self.telemetry {
            let channel = mvp_provision_log_channel(line.node_id, line.stream);
            let record = MvpProvisionLogRecord::new(line);
            let payload = serde_json::to_vec(&record).expect("serialize provisioning log record");
            producer.submit_bytes(channel, payload);
        }
    }
}

impl<P: ProvisionPlugin + 'static> ActorInterface for ProvisionerActor<P> {
    type Incoming = ProvisionerMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            ProvisionerMsg::StartNodes { nodes, reply_to } => {
                self.start_nodes(ctx, nodes, reply_to)
            }
            ProvisionerMsg::StopNodes { run_id, reply_to } => {
                self.stop_nodes(ctx, run_id, reply_to)
            }
            ProvisionerMsg::PluginObservation(observation) => self.observe_plugin(ctx, observation),
        }
    }
}

struct ActorPluginSink {
    sender: ExternalSender,
    addr: ActorAddress,
}

impl PluginObservationSink for ActorPluginSink {
    fn observe(&self, observation: PluginObservation) {
        let _ = self
            .sender
            .send_to(self.addr, ProvisionerMsg::PluginObservation(observation));
    }
}

pub fn register_codecs(registry: &mut CodecRegistry) {
    registry.register::<ProvisionerMsg, _>(JsonCodec::<ProvisionerMsg>::default());
    registry.register::<ProvisionerReport, _>(JsonCodec::<ProvisionerReport>::default());
}

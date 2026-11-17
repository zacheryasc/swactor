use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use ::telemetry::{
    ChannelContent, Lifetime, NodeId, StreamDescriptor, StreamId, StreamOrigin, TelemetryEndpoint,
};
use anyhow::{Context, Result};
use dashboard::{DashboardHandle, RemoteTelemetry};
use distribution::types::{MemberState, NodeId as PeerId};
use iroh::{EndpointAddr, PublicKey, RelayUrl};
use iroh_driver::{IrohDriver, TELEMETRY_ALPN, spawn_pull_server};
use serde_json::json;
use swactor::runtime::Runtime;
use swactor_engine::{EngineHandle, TaskSpawner};

use crate::cluster::Membership;

pub struct NodeTelemetry {
    endpoint: Arc<TelemetryEndpoint>,
    name: String,
}

impl NodeTelemetry {
    pub fn new(state: &Path, node: PublicKey, name: &str) -> Result<Self> {
        // A restarted producer must not reuse positions from its previous stream.
        // Like the identity key, this state belongs to exactly one running node.
        let path = state.join("telemetry-generation");
        let previous = match fs::read_to_string(&path) {
            Ok(value) => value.trim().parse::<u64>().context("invalid telemetry generation")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error).context("read telemetry generation"),
        };
        let generation = previous.checked_add(1).context("telemetry generation exhausted")?;
        let pending = state.join("telemetry-generation.next");
        let mut file = File::create(&pending).context("create telemetry generation")?;
        writeln!(file, "{generation}").context("write telemetry generation")?;
        file.sync_all().context("persist telemetry generation")?;
        drop(file);
        fs::rename(pending, path).context("commit telemetry generation")?;
        #[cfg(unix)]
        File::open(state)?.sync_all().context("sync telemetry directory")?;

        Ok(Self {
            endpoint: Arc::new(TelemetryEndpoint::with_descriptor(
                StreamDescriptor {
                    stream: StreamId::new(NodeId::new(node.to_string()), Lifetime(generation)),
                    label: Some(name.to_owned()),
                    origin: StreamOrigin::RemoteNode,
                },
                4096,
                4096,
            ).with_retention(4096, 4 * 1024 * 1024)),
            name: name.to_owned(),
        })
    }

    pub fn install_stats_hook(&self, runtime: &Runtime) {
        runtime.set_stats_hook(self.endpoint.producer().stats_hook());
    }

    pub fn spawn(
        self,
        tasks: &TaskSpawner,
        engine: &EngineHandle,
        driver: Arc<IrohDriver>,
        dashboard: DashboardHandle,
        mut membership: Membership,
        operator: PublicKey,
    ) -> tokio::sync::mpsc::Sender<()> {
        let (wake, mut ticks) = tokio::sync::mpsc::channel(1);
        let connection_tasks = tasks.clone();
        let producer = self.endpoint.producer();
        let status = producer.register_channel(
            "switchboard.node",
            ChannelContent::JsonRecord { schema: None },
        );
        let remote = RemoteTelemetry::new(tasks, engine, driver.endpoint(), dashboard);
        remote.observe_local(&self.endpoint);
        let mut ingress = driver.ingress_changed();
        let started = Instant::now();
        let is_operator = driver.node_id().0 == *operator.as_bytes();
        let relay: RelayUrl = driver.relay_url().expect("configured relay").parse().expect("relay URL");
        membership.mark_changed();
        tasks.spawn(async move {
            let mut subscriptions = BTreeMap::<PeerId, u64>::new();
            let mut next_subscription = 0;
            loop {
                for (_, connection) in driver.drain_accepted_for_alpn(TELEMETRY_ALPN) {
                    spawn_pull_server(&connection_tasks, connection, self.endpoint.clone());
                }
                tokio::select! {
                    result = ingress.changed() => {
                        if result.is_err() { return; }
                    }
                    result = membership.changed(), if is_operator => {
                        if result.is_err() { return; }
                        let members = membership.borrow_and_update();
                        subscriptions.retain(|peer, subscription| {
                            let alive = members.get(peer) == Some(&MemberState::Alive);
                            if !alive { remote.unsubscribe(0, *subscription); }
                            alive
                        });
                        for (peer, state) in members.iter() {
                            if *state != MemberState::Alive || *peer == driver.node_id() {
                                continue;
                            }
                            if let std::collections::btree_map::Entry::Vacant(entry) = subscriptions.entry(*peer) {
                                let key = PublicKey::from_bytes(&peer.0).expect("authenticated node identity");
                                // These are collector keys, not orchestration run/node identities.
                                remote.subscribe(EndpointAddr::new(key).with_relay_url(relay.clone()), 0, next_subscription);
                                entry.insert(next_subscription);
                                next_subscription += 1;
                            }
                        }
                    }
                    tick = ticks.recv() => {
                        if tick.is_none() { return; }
                        let statuses = driver.join_statuses();
                        let members = membership.borrow();
                        let mut visible: BTreeSet<_> = statuses.keys().chain(members.keys()).copied().collect();
                        visible.remove(&driver.node_id());
                        let snapshot = json!({
                            "name": self.name,
                            "node_id": driver.listen_addr(),
                            "relay_url": relay.as_str(),
                            "operator_public_key": operator.to_string(),
                            "role": if is_operator { "operator" } else { "member" },
                            "uptime_secs": started.elapsed().as_secs(),
                            "directory_routes": driver.directory_route_count(),
                            "peers": visible.iter().map(|id| {
                                let peer = PublicKey::from_bytes(&id.0).expect("authenticated node identity");
                                let connected = driver.has_active_connection(id);
                                json!({
                                    "node_id": peer.to_string(),
                                    "role": if peer == operator { "operator" } else { "member" },
                                    "connected": connected,
                                    "membership": if members.get(id) == Some(&MemberState::Alive) { "alive" } else { "offline" },
                                    "join_phase": statuses.get(id).map(|status| format!("{:?}", status.phase)).unwrap_or_else(|| if connected { "Accepted" } else { "Waiting" }.to_owned()),
                                })
                            }).collect::<Vec<_>>()
                        });
                        producer.submit_bytes(status, serde_json::to_vec(&snapshot).expect("serialize node status"));
                        self.endpoint.tick();
                    }
                }
            }
        });
        wake
    }
}

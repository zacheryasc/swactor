use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use dashboard::swactor::{RUNTIME_ACTORS, RUNTIME_STATS, RUNTIME_WORKERS};
use dashboard::{DashboardConfig, DashboardHandle, start_dashboard};
use datastream::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use swactor::stats::{ActorSnapshot, StatsHook};

const NODE_ID: &str = "dashboard-swactor-dummy";
const WORKER_ACTORS: usize = 12;
const PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const PULSE_INTERVAL: Duration = Duration::from_millis(25);
const WORK_ITEM_DELAY: Duration = Duration::from_micros(200);

#[derive(Clone)]
struct PulseTick {
    seq: u64,
}

#[derive(Clone)]
struct WorkItem {
    seq: u64,
    route: u32,
    hops_left: u8,
}

#[derive(Clone)]
enum RouterMsg {
    Configure { workers: Vec<ActorAddress> },
    Beat { seq: u64 },
    Complete { worker: u32, seq: u64, route: u32 },
}

struct PulseActor {
    router: ActorAddress,
}

impl ActorInterface for PulseActor {
    type Incoming = PulseTick;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: PulseTick) {
        let _ = ctx.send(self.router, RouterMsg::Beat { seq: msg.seq });
    }
}

struct RouterActor {
    workers: Vec<ActorAddress>,
    next: usize,
    completed: u64,
}

impl RouterActor {
    fn new() -> Self {
        Self {
            workers: Vec::new(),
            next: 0,
            completed: 0,
        }
    }
}

impl ActorInterface for RouterActor {
    type Incoming = RouterMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: RouterMsg) {
        match msg {
            RouterMsg::Configure { workers } => {
                self.workers = workers;
                self.next = 0;
            }
            RouterMsg::Beat { seq } => {
                if self.workers.is_empty() {
                    return;
                }

                let burst = 48 + (seq as usize % 32);
                for route in 0..burst {
                    let target = self.workers[self.next % self.workers.len()];
                    self.next = self.next.wrapping_add(1);
                    let _ = ctx.send(
                        target,
                        WorkItem {
                            seq,
                            route: route as u32,
                            hops_left: 1 + ((seq + route as u64) % 3) as u8,
                        },
                    );
                }
            }
            RouterMsg::Complete { worker, seq, route } => {
                self.completed = self.completed.wrapping_add(1);

                if self.completed % 7 == 0 && !self.workers.is_empty() {
                    let target =
                        self.workers[(worker as usize + route as usize) % self.workers.len()];
                    let _ = ctx.send(
                        target,
                        WorkItem {
                            seq,
                            route: route.wrapping_add(1000),
                            hops_left: 1,
                        },
                    );
                }
            }
        }
    }
}

struct WorkerActor {
    id: u32,
    router: ActorAddress,
}

impl ActorInterface for WorkerActor {
    type Incoming = WorkItem;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: WorkItem) {
        if msg.hops_left > 0 {
            let _ = ctx.send(
                ctx.self_addr(),
                WorkItem {
                    seq: msg.seq,
                    route: msg.route,
                    hops_left: msg.hops_left - 1,
                },
            );
            return;
        }

        thread::sleep(WORK_ITEM_DELAY);
        let _ = ctx.send(
            self.router,
            RouterMsg::Complete {
                worker: self.id,
                seq: msg.seq,
                route: msg.route,
            },
        );
    }
}

struct QueuedSinkActor;

impl ActorInterface for QueuedSinkActor {
    type Incoming = WorkItem;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _msg: WorkItem) {
        ctx.suspend_self();
    }
}

#[derive(Default)]
struct DashboardStatsHook {
    actors: Mutex<HashMap<ActorAddress, ActorDetail>>,
}

#[derive(Clone)]
struct ActorDetail {
    worker_id: usize,
    mailbox_depth: usize,
    last_msg_type: Option<String>,
    messages_processed: u64,
    poisoned: bool,
    message_type_counts: Vec<(String, u64)>,
}

impl StatsHook for DashboardStatsHook {
    fn on_tick(&self, worker_id: usize, snapshots: &[ActorSnapshot]) {
        let mut actors = self.actors.lock();
        for snapshot in snapshots {
            actors.insert(
                snapshot.address,
                ActorDetail {
                    worker_id,
                    mailbox_depth: snapshot.mailbox_depth,
                    last_msg_type: snapshot.last_msg_type.map(str::to_owned),
                    messages_processed: snapshot.messages_processed,
                    poisoned: snapshot.poisoned,
                    message_type_counts: snapshot
                        .message_type_counts
                        .iter()
                        .map(|(name, count)| ((*name).to_owned(), *count))
                        .collect(),
                },
            );
        }
    }
}

impl DashboardStatsHook {
    fn snapshot(
        &self,
        live_workers: &[(ActorAddress, usize)],
        names: &HashMap<ActorAddress, String>,
    ) -> Vec<ActorDetailFrame> {
        let actors = self.actors.lock();
        live_workers
            .iter()
            .map(|(address, worker_id)| {
                let detail = actors.get(address);
                ActorDetailFrame {
                    address: address.to_string(),
                    name: names.get(address).cloned(),
                    worker_id: detail.map_or(*worker_id, |detail| detail.worker_id),
                    mailbox_depth: detail.map_or(0, |detail| detail.mailbox_depth),
                    last_msg_type: detail.and_then(|detail| detail.last_msg_type.clone()),
                    messages_processed: detail.map_or(0, |detail| detail.messages_processed),
                    poisoned: detail.is_some_and(|detail| detail.poisoned),
                    message_type_counts: detail
                        .map(|detail| detail.message_type_counts.clone())
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}

#[derive(Serialize)]
struct ActorDetailFrame {
    address: String,
    name: Option<String>,
    worker_id: usize,
    mailbox_depth: usize,
    last_msg_type: Option<String>,
    messages_processed: u64,
    poisoned: bool,
    message_type_counts: Vec<(String, u64)>,
}

fn main() {
    let dashboard = start_dashboard(DashboardConfig::default());
    dashboard.start_http_standalone();

    let mut runtime = Runtime::new(RuntimeConfig {
        num_threads: 4,
        max_actors: 128,
        channel_buffer_size: 4096,
        actor_message_budget: 8,
    });
    let stats_hook = Arc::new(DashboardStatsHook::default());
    runtime.set_stats_hook(stats_hook.clone());

    let router = runtime.spawn(RouterActor::new()).expect("spawn router");
    let mut names = HashMap::new();
    names.insert(router, "router".to_owned());

    let mut workers = Vec::with_capacity(WORKER_ACTORS);
    for id in 0..WORKER_ACTORS {
        let address = runtime
            .spawn(WorkerActor {
                id: id as u32,
                router,
            })
            .expect("spawn worker actor");
        names.insert(address, format!("worker-{id}"));
        workers.push(address);
    }

    let pulse = runtime.spawn(PulseActor { router }).expect("spawn pulse");
    names.insert(pulse, "pulse".to_owned());
    let queued_sink = runtime.spawn(QueuedSinkActor).expect("spawn queued sink");
    names.insert(queued_sink, "queued-sink".to_owned());
    runtime
        .send_to(
            router,
            RouterMsg::Configure {
                workers: workers.clone(),
            },
        )
        .expect("configure router");

    let runtime = runtime.run().expect("start swactor runtime");
    let stream = StreamId::new(NodeId::new(NODE_ID), Lifetime(1));
    let mut position = 0_u64;
    let mut seq = 0_u64;
    let mut ticks_until_publish = 0_u8;

    println!(
        "dashboard listening at http://127.0.0.1:{}/view/swactor/workers",
        DashboardConfig::default().port
    );
    println!(
        "dummy node {NODE_ID} running {} swactor actors",
        names.len()
    );

    loop {
        let _ = runtime.runtime.send_to(pulse, PulseTick { seq });
        if seq % 2 == 0 {
            let _ = runtime.runtime.send_to(
                queued_sink,
                WorkItem {
                    seq,
                    route: u32::MAX,
                    hops_left: 0,
                },
            );
        }
        seq = seq.wrapping_add(1);

        if ticks_until_publish == 0 {
            publish_runtime_snapshot(
                &dashboard,
                &stream,
                &mut position,
                &runtime.runtime,
                &stats_hook,
                &names,
            );
            ticks_until_publish = (PUBLISH_INTERVAL.as_millis() / PULSE_INTERVAL.as_millis()) as u8;
        }
        ticks_until_publish = ticks_until_publish.saturating_sub(1);

        thread::sleep(PULSE_INTERVAL);
    }
}

fn publish_runtime_snapshot(
    dashboard: &DashboardHandle,
    stream: &StreamId,
    position: &mut u64,
    runtime: &Runtime,
    stats_hook: &DashboardStatsHook,
    names: &HashMap<ActorAddress, String>,
) {
    let stats = runtime.stats();
    let actor_details = stats_hook.snapshot(&stats.actors, names);
    let actors: Vec<Value> = stats
        .actors
        .iter()
        .map(|(address, worker_id)| json!([address.to_string(), worker_id]))
        .collect();
    let workers = serde_json::to_value(&stats.workers).expect("serialize worker stats");
    let actor_details = serde_json::to_value(actor_details).expect("serialize actor stats");
    let tick_timings = serde_json::to_value(&stats.tick_timings).expect("serialize tick timings");
    let total_mailbox_depth: usize = stats
        .workers
        .iter()
        .map(|worker| worker.mailbox_depth)
        .sum();

    publish_json(
        dashboard,
        stream,
        position,
        RUNTIME_STATS,
        json!({
            "num_workers": stats.num_workers,
            "uptime_ms": stats.uptime_ms,
            "actors_live": stats.actors.len(),
            "mailbox_depth": total_mailbox_depth,
            "actors": actors,
            "workers": workers,
            "actor_details": actor_details,
            "tick_timings": tick_timings,
        }),
    );

    publish_json(
        dashboard,
        stream,
        position,
        RUNTIME_WORKERS,
        json!({ "workers": stats.workers }),
    );

    publish_json(
        dashboard,
        stream,
        position,
        RUNTIME_ACTORS,
        json!({ "actors": actor_details }),
    );
}

fn publish_json(
    dashboard: &DashboardHandle,
    stream: &StreamId,
    position: &mut u64,
    channel: &str,
    value: Value,
) {
    let payload = serde_json::to_vec(&value).expect("serialize dashboard frame");
    let frame = Frame::new(ChannelId::new(channel), Position(*position), payload);
    dashboard.ingest(stream, &frame);
    *position = position.wrapping_add(1);
}

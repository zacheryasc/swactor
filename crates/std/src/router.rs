use std::marker::PhantomData;
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Ctx, Down, Message};
use swactor::Error;

use crate::supervisor::ActiveChild;
use crate::CtxMonitoring;

/// Strategy for distributing messages across pool workers.
#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    /// Sequential round-robin distribution.
    RoundRobin,
    /// Random worker selection.
    Random,
    /// Send to all workers (message is cloned to each).
    Broadcast,
}

/// A router actor that manages a pool of identical workers and distributes
/// incoming messages across them according to a [`RoutingStrategy`].
///
/// Workers are spawned during `on_start`, monitored for failures, and
/// automatically replaced to maintain the target pool size. Meltdown
/// protection stops the router when total restarts exceed `max_restarts`.
///
/// # Example
///
/// ```ignore
/// let router = Router::new(
///     RoutingStrategy::RoundRobin,
///     5,
///     |ctx| ctx.spawn(MyWorker::new()),
///     10,
/// );
/// let router_addr = rt.spawn(router)?;
/// rt.send_to(router_addr, WorkerMessage::DoWork(42))?;
/// ```
pub struct Router<M: Message> {
    strategy: RoutingStrategy,
    pool_size: usize,
    factory: Arc<dyn Fn(&Ctx) -> Result<ActorAddress, Error> + Send + Sync>,
    workers: Vec<Option<ActiveChild>>,
    rr_index: usize,
    total_restarts: u32,
    max_restarts: u32,
    _marker: PhantomData<M>,
}

impl<M: Message> Router<M> {
    pub fn new(
        strategy: RoutingStrategy,
        pool_size: usize,
        factory: impl Fn(&Ctx) -> Result<ActorAddress, Error> + Send + Sync + 'static,
        max_restarts: u32,
    ) -> Self {
        Self {
            strategy,
            pool_size,
            factory: Arc::new(factory),
            workers: (0..pool_size).map(|_| None).collect(),
            rr_index: 0,
            total_restarts: 0,
            max_restarts,
            _marker: PhantomData,
        }
    }

    fn start_worker(&mut self, ctx: &Ctx, idx: usize) -> Result<(), Error> {
        let addr = (self.factory)(ctx)?;
        let mref = ctx.monitor(addr);
        self.workers[idx] = Some(ActiveChild {
            addr,
            _monitor_ref: mref,
        });
        Ok(())
    }

    fn find_worker_idx(&self, addr: ActorAddress) -> Option<usize> {
        self.workers
            .iter()
            .position(|w| w.as_ref().map_or(false, |ac| ac.addr == addr))
    }

    fn live_workers(&self) -> Vec<ActorAddress> {
        self.workers
            .iter()
            .filter_map(|w| w.as_ref().map(|ac| ac.addr))
            .collect()
    }

    fn select_one(&mut self) -> Option<ActorAddress> {
        let live = self.live_workers();
        if live.is_empty() {
            return None;
        }
        match self.strategy {
            RoutingStrategy::RoundRobin => {
                let idx = self.rr_index % live.len();
                self.rr_index = self.rr_index.wrapping_add(1);
                Some(live[idx])
            }
            RoutingStrategy::Random => {
                #[cfg(feature = "getrandom")]
                {
                    let mut buf = [0u8; 8];
                    getrandom::getrandom(&mut buf).expect("getrandom failed");
                    let r = u64::from_ne_bytes(buf) as usize;
                    Some(live[r % live.len()])
                }
                #[cfg(not(feature = "getrandom"))]
                {
                    // Fallback to round-robin when getrandom is unavailable (wasm)
                    let idx = self.rr_index % live.len();
                    self.rr_index = self.rr_index.wrapping_add(1);
                    Some(live[idx])
                }
            }
            RoutingStrategy::Broadcast => None, // handled separately
        }
    }
}

impl<M: Message> ActorInterface for Router<M> {
    type Incoming = M;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: M) {
        match self.strategy {
            RoutingStrategy::Broadcast => {
                let live = self.live_workers();
                for addr in live {
                    let _ = ctx.send(addr, msg.clone());
                }
            }
            _ => {
                if let Some(addr) = self.select_one() {
                    let _ = ctx.send(addr, msg);
                }
            }
        }
    }

    fn on_start(&mut self, ctx: &Ctx) {
        for idx in 0..self.pool_size {
            if let Err(e) = self.start_worker(ctx, idx) {
                eprintln!("swactor: router failed to start worker {idx}: {e}");
            }
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        for child in self.workers.iter().flatten() {
            let _ = ctx.stop_actor(child.addr);
        }
    }

    fn handle_down(&mut self, ctx: &Ctx, down: Down) {
        let Some(idx) = self.find_worker_idx(down.addr) else {
            return;
        };
        self.workers[idx] = None;

        self.total_restarts += 1;
        if self.total_restarts > self.max_restarts {
            eprintln!(
                "swactor: router reached max restarts ({}), shutting down",
                self.max_restarts
            );
            ctx.stop_self();
            return;
        }

        if let Err(e) = self.start_worker(ctx, idx) {
            eprintln!("swactor: router failed to restart worker {idx}: {e}");
        }
    }
}

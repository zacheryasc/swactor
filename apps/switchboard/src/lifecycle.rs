use std::time::Duration;

use swactor::actor::{ActorInterface, Ctx};
use swactor::runtime::ExternalSender;
use swactor_engine::{ActorCompletion, ActorTimer, EngineHandle};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
pub struct PeerObservation {
    pub connected: bool,
    pub joining: bool,
}

#[derive(Clone)]
pub enum NodeIn {
    Changed,
    Retry,
    Telemetry,
    Peers(Vec<PeerObservation>),
    Stopped(Result<(), String>),
}

pub enum Effect {
    Observe,
    Join(usize),
    Telemetry,
}

pub struct Lifecycle {
    clock: EngineHandle,
    sender: ExternalSender,
    effects: UnboundedSender<Effect>,
    completion: ActorCompletion<Result<(), String>>,
    timer: Option<ActorTimer>,
    telemetry_timer: Option<ActorTimer>,
    observing: bool,
    stopped: bool,
}

impl Lifecycle {
    pub fn new(
        clock: EngineHandle,
        sender: ExternalSender,
        effects: UnboundedSender<Effect>,
        completion: ActorCompletion<Result<(), String>>,
    ) -> Self {
        Self {
            clock, sender, effects, completion, timer: None, telemetry_timer: None,
            observing: false, stopped: false,
        }
    }

    fn observe(&mut self) {
        if !self.observing {
            self.observing = self.effects.send(Effect::Observe).is_ok();
        }
    }

    fn emit_telemetry(&mut self, ctx: &Ctx<'_>) {
        let _ = self.effects.send(Effect::Telemetry);
        self.telemetry_timer = Some(self.clock.send_after(
            Duration::from_secs(1), self.sender.clone(), ctx.self_addr(), NodeIn::Telemetry,
        ));
    }

    fn cancel_timers(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.cancel();
        }
        if let Some(timer) = self.telemetry_timer.take() {
            timer.cancel();
        }
    }
}

impl ActorInterface for Lifecycle {
    type Incoming = NodeIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.observe();
        self.emit_telemetry(ctx);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: NodeIn) {
        if self.stopped {
            return;
        }
        match message {
            NodeIn::Changed if self.timer.is_none() => self.observe(),
            NodeIn::Changed => {},
            NodeIn::Retry => {
                self.timer.take();
                self.observe();
            }
            NodeIn::Telemetry => {
                self.telemetry_timer.take();
                self.emit_telemetry(ctx);
            }
            NodeIn::Peers(peers) => {
                self.observing = false;
                let mut absent = false;
                for (index, peer) in peers.iter().enumerate() {
                    if !peer.connected {
                        absent = true;
                        if !peer.joining {
                            let _ = self.effects.send(Effect::Join(index));
                        }
                    }
                }
                // Finite transport attempts are retried for as long as a configured
                // peer is absent. Healthy clusters own no periodic retry timer.
                if absent {
                    self.timer = Some(self.clock.send_after(
                        Duration::from_secs(1), self.sender.clone(), ctx.self_addr(), NodeIn::Retry,
                    ));
                }
            }
            NodeIn::Stopped(result) => {
                self.stopped = true;
                self.cancel_timers();
                let _ = self.completion.complete(result);
            }
        }
    }

    fn on_stop(&mut self, _ctx: &Ctx<'_>) {
        self.cancel_timers();
        let _ = self.completion.complete(Err("node lifecycle stopped".to_owned()));
    }
}

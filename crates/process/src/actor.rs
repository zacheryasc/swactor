use std::sync::{Arc, OnceLock};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::action::ProcessAction;
use crate::event::ProcessEvent;
use crate::message::{ProcessCommand, ProcessNotification};
use crate::session::ProcessSession;
use crate::types::{ProcessDriver, ProcessWaker};

/// Actor wrapper around a `ProcessSession` and its driver.
///
/// Generic over `D: ProcessDriver` so that tests can use `MockDriver` or
/// `TestDriver` while production uses `LocalDriver`.
pub struct ProcessActor<D: ProcessDriver> {
    session: ProcessSession,
    driver: D,
    self_addr: Option<ActorAddress>,
    /// Actions from `ProcessSession::new()`, executed in `on_start`.
    deferred_actions: Option<Vec<ProcessAction>>,
    /// Shared slot for the waker — filled after the actor address is known.
    pub waker_slot: Arc<OnceLock<ProcessWaker>>,
}

impl<D: ProcessDriver> ProcessActor<D> {
    pub fn new(
        session: ProcessSession,
        driver: D,
        initial_actions: Vec<ProcessAction>,
        waker_slot: Arc<OnceLock<ProcessWaker>>,
    ) -> Self {
        Self {
            session,
            driver,
            self_addr: None,
            deferred_actions: Some(initial_actions),
            waker_slot,
        }
    }

    /// Drain events from the driver, apply each to the session, and dispatch
    /// all resulting actions.
    fn drain_and_dispatch(&mut self, ctx: &Ctx) {
        let events = self.driver.poll();
        for event in events {
            let actions = self.session.apply(event);
            self.dispatch_actions(ctx, actions);
        }
    }

    /// Execute actions produced by the session state machine.
    fn dispatch_actions(&mut self, ctx: &Ctx, actions: Vec<ProcessAction>) {
        let self_addr = self.self_addr.expect("self_addr not set");
        for action in actions {
            match action {
                // Driver commands — forward to the driver
                ProcessAction::SpawnProcess { .. }
                | ProcessAction::WriteStdin { .. }
                | ProcessAction::SendSignal { .. }
                | ProcessAction::ResizePty { .. }
                | ProcessAction::CloseStdin
                | ProcessAction::ScheduleKillTimeout { .. } => {
                    self.driver.execute(action);
                }

                // Subscriber notifications — send to each subscriber
                ProcessAction::NotifyStarted { subscribers } => {
                    let notif = ProcessNotification::Started { process: self_addr };
                    for sub in subscribers {
                        let _ = ctx.send(sub, notif.clone());
                    }
                }
                ProcessAction::NotifyOutput {
                    subscribers,
                    data,
                    stream,
                } => {
                    let notif = ProcessNotification::Output {
                        process: self_addr,
                        data,
                        stream,
                    };
                    for sub in subscribers {
                        let _ = ctx.send(sub, notif.clone());
                    }
                }
                ProcessAction::NotifyExited {
                    subscribers,
                    status,
                } => {
                    let notif = ProcessNotification::Exited {
                        process: self_addr,
                        status,
                    };
                    for sub in subscribers {
                        let _ = ctx.send(sub, notif.clone());
                    }
                }
                ProcessAction::NotifyError {
                    subscribers,
                    error,
                } => {
                    let notif = ProcessNotification::Error {
                        process: self_addr,
                        error,
                    };
                    for sub in subscribers {
                        let _ = ctx.send(sub, notif.clone());
                    }
                }

                // Lifecycle
                ProcessAction::SelfTerminate => {
                    ctx.stop_self();
                }
            }
        }
    }

    /// Map a `ProcessCommand` to the corresponding `ProcessEvent`.
    fn command_to_event(cmd: ProcessCommand) -> Option<ProcessEvent> {
        match cmd {
            ProcessCommand::WriteStdin { data } => Some(ProcessEvent::WriteStdin { data }),
            ProcessCommand::SendSignal { signal } => Some(ProcessEvent::SendSignal { signal }),
            ProcessCommand::ResizePty { size } => Some(ProcessEvent::ResizePty { size }),
            ProcessCommand::CloseStdin => Some(ProcessEvent::CloseStdin),
            ProcessCommand::Close => Some(ProcessEvent::CloseRequested),
            ProcessCommand::Subscribe { address } => Some(ProcessEvent::Subscribe { address }),
            ProcessCommand::Unsubscribe { address } => {
                Some(ProcessEvent::Unsubscribe { address })
            }
            ProcessCommand::PollTick => None, // handled by drain
        }
    }
}

impl<D: ProcessDriver + 'static> ActorInterface for ProcessActor<D> {
    type Incoming = ProcessCommand;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.self_addr = Some(ctx.self_addr());
        if let Some(actions) = self.deferred_actions.take() {
            self.dispatch_actions(ctx, actions);
        }
    }

    fn handle(&mut self, ctx: &Ctx, msg: ProcessCommand) {
        // Process the incoming command first — this ensures Subscribe
        // registers before drain dispatches notifications, and keeps
        // user commands (Close, WriteStdin) responsive.
        if let Some(event) = Self::command_to_event(msg) {
            let actions = self.session.apply(event);
            self.dispatch_actions(ctx, actions);
        }

        // Then drain pending I/O events from background threads.
        self.drain_and_dispatch(ctx);
    }
}

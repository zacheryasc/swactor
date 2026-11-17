use futures_util::future::{AbortHandle, Abortable};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::ActorTimer;

use super::*;

#[derive(Clone)]
struct DeadlineElapsed;

enum Completion {
    Cancel(AbortHandle),
    Wake(ActorAddress),
}

/// Owns one deadline decision, whether cancellation of an operation or a paced
/// retry wake. The adapter only awaits the resulting observation; it never
/// drives timers or progresses actor state.
struct DeadlineActor {
    engine: EngineHandle,
    sender: ExternalSender,
    deadline: Instant,
    completion: Completion,
    timer: Option<ActorTimer>,
}

impl ActorInterface for DeadlineActor {
    type Incoming = DeadlineElapsed;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.timer = Some(
            self.engine.send_after(
                self.deadline
                    .saturating_duration_since(self.engine.now().to_instant()),
                self.sender.clone(),
                ctx.self_addr(),
                DeadlineElapsed,
            ),
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, _: DeadlineElapsed) {
        match &self.completion {
            Completion::Cancel(operation) => operation.abort(),
            Completion::Wake(reply) => {
                let _ = ctx.send(*reply, ());
            }
        }
        ctx.stop_self();
    }
}

impl Drop for DeadlineActor {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.cancel();
        }
    }
}

struct DeadlineOwner<'a> {
    runtime: &'a Runtime,
    actor: ActorAddress,
}

impl Drop for DeadlineOwner<'_> {
    fn drop(&mut self) {
        let _ = self.runtime.stop_actor(self.actor);
    }
}

fn start<'a>(
    runtime: &'a Runtime,
    engine: &EngineHandle,
    deadline: Instant,
    completion: Completion,
) -> Result<DeadlineOwner<'a>> {
    let actor = runtime
        .spawn(DeadlineActor {
            engine: engine.clone(),
            sender: runtime.create_sender(),
            deadline,
            completion,
            timer: None,
        })
        .map_err(|error| NativeError::protocol(format!("start native deadline owner: {error}")))?;
    Ok(DeadlineOwner { runtime, actor })
}

pub(super) async fn bounded<T>(
    budget: Budget<'_>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    budget.remaining()?;
    let (abort, registration) = AbortHandle::new_pair();
    let _owner = start(
        budget.runtime,
        budget.engine,
        budget.deadline,
        Completion::Cancel(abort),
    )?;
    Abortable::new(operation, registration)
        .await
        .map_err(|_| NativeError::deadline_exhausted())?
}

pub(super) async fn pace(budget: Budget<'_>, delay: Duration) -> Result<()> {
    let wake = budget
        .runtime
        .new_inbox::<()>()
        .map_err(|error| NativeError::protocol(format!("create native pacing inbox: {error}")))?;
    let deadline = budget.engine.now().to_instant() + delay.min(budget.remaining()?);
    let _owner = start(
        budget.runtime,
        budget.engine,
        deadline,
        Completion::Wake(*wake.addr()),
    )?;
    wake.recv().await;
    budget.remaining()?;
    Ok(())
}

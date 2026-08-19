use std::time::Duration;

use swactor::runtime::Runtime;
use swactor_engine::SteppingBackend;

pub(crate) fn drive_steps(backend: &SteppingBackend, count: usize) {
    for _ in 0..count {
        backend.step();
    }
}

pub(crate) fn advance_and_drive(backend: &SteppingBackend, duration: Duration, count: usize) {
    backend.advance_time(duration);
    drive_steps(backend, count);
}

pub(crate) fn actor_census(runtime: &Runtime) -> String {
    let stats = runtime.stats();
    if stats.actor_details.is_empty() {
        return format!("actors={:?}, workers={:?}", stats.actors, stats.workers);
    }

    stats
        .actor_details
        .iter()
        .map(|actor| {
            format!(
                "address={} name={:?} worker={} mailbox={} last={:?} processed={} poisoned={}",
                actor.address,
                actor.name,
                actor.worker_id,
                actor.mailbox_depth,
                actor.last_msg_type,
                actor.messages_processed,
                actor.poisoned,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn assert_no_poison(runtime: &Runtime) {
    assert_no_poison_with_context(runtime, "");
}

pub(crate) fn assert_no_poison_with_context(runtime: &Runtime, context: &str) {
    let stats = runtime.stats();
    let poisoned = stats
        .actor_details
        .iter()
        .filter(|actor| actor.poisoned)
        .collect::<Vec<_>>();
    let panics = stats
        .workers
        .iter()
        .map(|worker| worker.panics)
        .sum::<u64>();
    assert!(
        poisoned.is_empty() && panics == 0,
        "actor poison/panic detected: poisoned={poisoned:?}, worker_panics={panics}\n{context}\n{}",
        actor_census(runtime),
    );
}

pub(crate) fn assert_actor_delta_at_most(runtime: &Runtime, baseline: usize, limit: usize) {
    let current = runtime.stats().actors.len();
    assert!(
        current <= baseline.saturating_add(limit),
        "actor count grew from {baseline} to {current}, limit={limit}\n{}",
        actor_census(runtime),
    );
}

pub(crate) fn assert_mailboxes_drained(runtime: &Runtime) {
    let stats = runtime.stats();
    let worker_depth = stats
        .workers
        .iter()
        .map(|worker| worker.mailbox_depth)
        .sum::<usize>();
    let actor_depth = stats
        .actor_details
        .iter()
        .map(|actor| actor.mailbox_depth)
        .sum::<usize>();
    assert_eq!(
        worker_depth + actor_depth,
        0,
        "mailboxes did not drain\n{}",
        actor_census(runtime),
    );
}

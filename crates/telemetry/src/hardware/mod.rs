pub mod cpu;
pub mod gpu;
pub mod memory;
pub mod net;
pub mod pressure;
pub mod storage;

use std::time::Duration;

use swactor_engine::EngineHandle;

/// Run a stateful blocking sampler on the engine without creating an actor.
///
/// Sampling never overlaps: the next interval is armed only after the previous
/// blocking sample has returned and `observed` has consumed its result.
pub fn spawn_blocking_sampler<State, Sample, Started, Observed>(
    engine: EngineHandle,
    period: Duration,
    state: State,
    sample: fn(State, u64) -> (State, Sample),
    started: Started,
    mut observed: Observed,
) where
    State: Send + 'static,
    Sample: Send + 'static,
    Started: FnOnce() + Send + 'static,
    Observed: FnMut(u64, Sample) + Send + 'static,
{
    let task_engine = engine.clone();
    engine.spawn(async move {
        started();
        let blocking_work = task_engine.blocking_work_sender();
        let mut interval = task_engine.interval(period);
        let mut state = state;
        let mut seq = 0_u64;

        loop {
            (&mut interval).await;
            let current_state = state;
            let (sample_tx, sample_rx) = futures_channel::oneshot::channel();
            let work = Box::new(move || {
                let _ = sample_tx.send(sample(current_state, seq));
            });
            if blocking_work.submit(work).is_err() {
                return;
            }
            let Ok((next_state, result)) = sample_rx.await else {
                return;
            };
            state = next_state;
            observed(seq, result);
            seq = seq.saturating_add(1);
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, TokioBackend, TokioConfig};

    use super::spawn_blocking_sampler;

    #[test]
    fn blocking_sampler_runs_sequentially_with_monotonic_sequences() {
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
        )
        .expect("engine");
        let (observed_tx, observed_rx) = mpsc::channel();

        spawn_blocking_sampler(
            engine.handle(),
            Duration::from_millis(1),
            0_u64,
            |state, seq| (state + 1, (seq, state)),
            || {},
            move |seq, result| {
                observed_tx
                    .send((seq, result))
                    .expect("observation receiver")
            },
        );

        assert_eq!(
            observed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first sample"),
            (0, (0, 0)),
        );
        assert_eq!(
            observed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("second sample"),
            (1, (1, 1)),
        );
    }
}

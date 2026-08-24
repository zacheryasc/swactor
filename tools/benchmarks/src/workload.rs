/// Engine-neutral throughput associated with one workload execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkUnits {
    Operations(u64),
    Frames(u64),
    Bytes(u64),
}

impl WorkUnits {
    pub fn amount(self) -> u64 {
        match self {
            Self::Operations(amount) | Self::Frames(amount) | Self::Bytes(amount) => amount,
        }
    }
}

/// How independently each measured execution must be prepared.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetupPolicy {
    #[default]
    Batched,
    PerExecution,
}

/// A benchmark workload independent of its timing and reporting engine.
pub trait Workload {
    type State;
    type Output;

    fn name(&self) -> String;
    fn setup(&self) -> Self::State;
    fn execute(&self, state: &mut Self::State) -> Self::Output;
    fn verify(&self, state: &Self::State, output: &Self::Output);
    fn units(&self) -> WorkUnits;

    fn setup_policy(&self) -> SetupPolicy {
        SetupPolicy::Batched
    }
}

/// Exercise correctness before an engine begins timing a workload.
pub fn validate<W: Workload>(workload: &W) {
    assert!(
        workload.units().amount() > 0,
        "workload units must be nonzero"
    );
    let mut state = workload.setup();
    let output = workload.execute(&mut state);
    workload.verify(&state, &output);
}

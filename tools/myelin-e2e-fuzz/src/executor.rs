//! Language-independent campaign execution boundary.
use std::fmt;

use serde::Serialize;

use crate::budget::Budget;
use crate::harness::ClusterHarness;
use crate::ir::{BehaviorCase, CaseObservation};
use crate::oracle::FailureSignature;

#[derive(Clone, Debug, Serialize)]
pub struct ExecutionFailure {
    pub detail: String,
    pub signature: Option<FailureSignature>,
    pub attempt_id: Option<u64>,
}

impl fmt::Display for ExecutionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ExecutionFailure {}

pub trait CaseExecutor {
    /// Execute, verify, and retract the case's resources before returning.
    fn execute(
        &mut self,
        case: &BehaviorCase,
        budget: &Budget,
    ) -> Result<CaseObservation, ExecutionFailure>;
}

impl CaseExecutor for ClusterHarness {
    fn execute(
        &mut self,
        case: &BehaviorCase,
        budget: &Budget,
    ) -> Result<CaseObservation, ExecutionFailure> {
        let previous = self.execution_budget().clone();
        let duration =
            budget
                .remaining("admit case executor")
                .map_err(|detail| ExecutionFailure {
                    detail,
                    signature: None,
                    attempt_id: None,
                })?;
        let fixture_remaining =
            previous
                .remaining("admit fixture execution")
                .map_err(|detail| ExecutionFailure {
                    detail,
                    signature: None,
                    attempt_id: None,
                })?;
        self.set_execution_budget(budget.child(duration.min(fixture_remaining)));
        let result = self
            .run_case(case)
            .and_then(|observation| {
                self.assert_healthy()?;
                Ok(observation)
            })
            .map_err(|detail| ExecutionFailure {
                detail,
                signature: self.last_failure_signature().cloned(),
                attempt_id: self.last_attempt_id(),
            });
        self.set_execution_budget(previous);
        result
    }
}

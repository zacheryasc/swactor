# Weight Lifecycle Contract

This document defines the behavioral contract for stage-local weight work.
Weights are persistent run state for a stage and must be usable before the
stage reports `StageReady`.

## Assignment

- The stage receives its weight source from `ProvisionStage`.
- The stage receives exactly one assigned layer range.
- The stage validates the assigned layer range against the run plan.
- The stage does not load layers outside its assigned range as graph-visible
  ownership.

## Loading

- A stage may download a whole GGUF and load only its range.
- A stage may download physical shards containing its range.
- A stage may use a cached artifact that already exists on the node.
- The physical loading mechanism is implementation-defined.
- The system-visible outcome is `WeightsReady` or `StageFault`.

## WeightsReady

- `WeightsReady` requires assigned artifact bytes to be locally available or
  cached.
- `WeightsReady` requires the assigned layer range to be validated.
- `WeightsReady` requires the worker to have loaded or bound the range needed
  for execution.
- `WeightsReady` happens before `StageReady`.

## Failure Behavior

- Download failure faults the stage.
- Parse failure faults the stage.
- Device allocation failure faults the stage.
- Binding failure faults the stage.
- Invalid layer range faults the stage.

## Test Direction

Tests should treat weight loading as a stage-local black box with observable
events. Successful tests should assert `WeightsReady` before `StageReady`.
Failure tests should inject download, parse, allocation, and bind failures and
assert `StageFault` with no `StageReady`.

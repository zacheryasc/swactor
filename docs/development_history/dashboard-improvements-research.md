# Dashboard Improvements — Research Phase

## Summary
Researched 10 comparable monitoring/dashboard systems to inform swactor's dashboard improvement plan.

## Systems Analyzed
- **Actor runtimes**: Erlang Observer (GUI/CLI/Web), Akka Insights, Ray Dashboard, Orleans Dashboard
- **Async/runtime tools**: tokio-console, Lunatic
- **Message/infrastructure**: RabbitMQ Management, Consul UI, Nomad UI
- **Web frameworks**: Phoenix LiveDashboard

## Key Findings
1. **Time-series history** is table-stakes — every system provides it
2. **Actor detail drill-down** is universal (Observer has 6-tab process info, Orleans has grain state inspection)
3. **Search/filter** exists in every system
4. **Warning/anomaly detection** (tokio-console's lint system) is a high-value differentiator
5. **Topology visualization** (Consul golden metrics, Observer supervision tree) is rare but powerful

## Implementation Plan
8 feature stages defined (see `CLAUDE/notes/feature-stages/`):
1. Time-Series History Infrastructure
2. Actor Detail Drill-Down
3. Search and Filter
4. Per-Worker Utilization Visualization
5. Warning/Anomaly Detection
6. Actor-to-Actor Message Flow Topology
7. Per-Actor Logging
8. Per-Message-Type Breakdown

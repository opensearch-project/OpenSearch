# analytics-framework

Shared library containing the SPI interfaces and core types for the analytics engine. All plugins depend on this library — it defines the contracts but contains no implementation logic.

## SPI Interfaces

- **`QueryPlanExecutorPlugin`** — Factory for creating a `QueryPlanExecutor` from discovered back-end plugins.
- **`AnalyticsBackEndPlugin`** — Extension point for native execution engines (DataFusion, Lucene, etc.). Exposes engine name, bridge, and capabilities.
- **`AnalyticsFrontEndPlugin`** — Marker interface for query language front-ends (PPL, SQL). Discovered by the hub for lifecycle tracking.
- **`SchemaProvider`** — Functional interface that builds a Calcite `SchemaPlus` from cluster state.

## Core Types

- **`QueryPlanExecutor`** — Executes a Calcite `RelNode` plan fragment and returns result rows.
- **`EngineBridge<T>`** — JNI/native boundary for engine-specific plan conversion and execution (e.g., Substrait → Arrow batches).
- **`EngineCapabilities`** — Declares supported operators and functions. Used by the push-down planner to decide what gets absorbed into engine-executed boundary nodes vs. what stays in Calcite's in-process execution.

## Dependencies

Calcite and Arrow — no dependency on the OpenSearch server module.

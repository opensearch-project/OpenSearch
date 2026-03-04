# analytics-backend-datafusion

DataFusion native execution engine plugin. Implements `AnalyticsBackEndPlugin` to provide a back-end that can execute query plan fragments via JNI.

## What it does

Exposes a `DataFusionBridge` (`EngineBridge<byte[]>`) that converts Calcite `RelNode` fragments into a serialized plan format and executes them through a native Rust/DataFusion library. Currently a stub.

## How it fits in

Declares `extendedPlugins = ['analytics-engine']` so the hub discovers it as an `AnalyticsBackEndPlugin`. The hub passes all discovered back-ends to the `QueryPlanExecutorPlugin` during executor creation. The executor will eventually use the bridge and capabilities to route plan fragments to the appropriate engine.

## Key classes

- **`DataFusionPlugin`** — The `AnalyticsBackEndPlugin` SPI implementation. Reports `name() = "datafusion"`.
- **`DataFusionBridge`** — The `EngineBridge<byte[]>` implementation for native execution.
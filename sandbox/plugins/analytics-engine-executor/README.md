# analytics-engine-executor

Default `QueryPlanExecutorPlugin` implementation. Provides the `QueryPlanExecutor` that the hub binds into Guice.

## What it does

Implements `QueryPlanExecutorPlugin.createExecutor(backEnds)` — receives the list of discovered back-end plugins and returns a `DefaultPlanExecutor`. Currently a stub that logs the received `RelNode` fragment and returns empty results. The real implementation will select a back-end based on planner rules and engine capabilities, then delegate execution.

## How it fits in

Declares `extendedPlugins = ['analytics-engine']` so the hub discovers it during `loadExtensions`. The hub calls `createExecutor`, gets back a `QueryPlanExecutor`, and binds it into Guice. Front-end plugins never interact with this plugin directly — they inject `QueryPlanExecutor`.

## Key classes

- **`AnalyticsExecutorPlugin`** — The `QueryPlanExecutorPlugin` SPI implementation.
- **`DefaultPlanExecutor`** — The `QueryPlanExecutor` implementation (stub).

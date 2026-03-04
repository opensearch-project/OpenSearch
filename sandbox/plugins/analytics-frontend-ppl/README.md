# analytics-frontend-ppl

PPL query language front-end plugin. Implements `AnalyticsFrontEndPlugin` and `ActionPlugin` to provide a REST endpoint for PPL.

## What it does

1. Registers `POST /_plugins/_analytics_engine/ppl` via `RestUnifiedPPLAction`.
2. `UnifiedQueryService` orchestrates the pipeline: PPL text → Calcite `RelNode` → push-down optimization → compile → execute → planExecutor -> response.

## Query pipeline

- **PPL parsing**: Uses `unified-query-ppl` (sql/api) to parse PPL text into a Calcite logical plan.
- **Push-down optimization**: `PushDownPlanner` uses a HepPlanner with absorb rules (`AbsorbFilterRule`, `AbsorbProjectRule`, `AbsorbAggregateRule`, `AbsorbSortRule`) to push supported operators into `OpenSearchBoundaryTableScan` nodes. Unsupported operators stay as Calcite logical nodes and execute in-process via Janino.
- **Compilation**: `OpenSearchQueryCompiler` rebuilds the plan in a fresh `VolcanoPlanner` and produces a `PreparedStatement`.
- **Execution**: JDBC execution returns a `ResultSet`, which is converted to `UnifiedPPLResponse(columns, rows)`.

## How it fits in

Declares `extendedPlugins = ['analytics-engine']` so the hub discovers it as an `AnalyticsFrontEndPlugin`.

## Key classes

- **`PPLFrontEndPlugin`** — Plugin entry point. Registers REST handler and transport action.
- **`TransportUnifiedPPLAction`** — Transport action. Injects `QueryPlanExecutor` + `SchemaProvider`, delegates to `UnifiedQueryService`.
- **`UnifiedQueryService`** — Pipeline orchestrator.
- **`PushDownPlanner`** — Calcite HepPlanner-based optimizer with boundary node absorption.
- **`OpenSearchBoundaryTableScan`** — `EnumerableRel` boundary node that holds an absorbed logical fragment and delegates to `PlanExecutor` at execution time.
- **`OpenSearchQueryCompiler`** — Compiles mixed plans into executable `PreparedStatement`s.

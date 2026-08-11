# dsl-query-executor

A front-end sandbox plugin to the analytics engine that intercepts `_search` requests, converts DSL queries into Calcite RelNode logical plans, and executes them through the analytics engine's query pipeline.

## Supported Query Types

- **Term** — equality filter
- **Terms** — multi-value equality filter (uses query Filter with SEARCH and EQUALS)
- **Match All** — matches all documents

## Architecture

```
_search request
  → SearchActionFilter (intercepts SearchAction)
    → TransportDslExecuteAction (resolves index, orchestrates pipeline)
      → SearchSourceConverter (DSL → Calcite RelNode)
      → DslQueryPlanExecutor (delegates to analytics engine)
      → SearchResponseBuilder (builds SearchResponse)
```

## Dependencies

- `analytics-engine` — provides `QueryPlanExecutor` and `EngineContext` via Guice (declared as `extendedPlugins`)
- `analytics-framework` — provides Calcite and shared SPI interfaces

## Supported Queries

| DSL Query | Calcite Representation |
|-----------|------------------------|
| `term` | `=($field, value)` — equality filter |
| `match_all` | Skipped (boolean literal `TRUE`) |
| `exists` | `IS NOT NULL($field)` — field existence check (boost not supported) |

## Running locally

```bash
./gradlew run -PinstalledPlugins="['analytics-engine','dsl-query-executor']"
```

## Testing

```bash
# Unit tests
./gradlew :sandbox:plugins:dsl-query-executor:test

# Integration tests
./gradlew :sandbox:plugins:dsl-query-executor:internalClusterTest
```

## Known Divergences (regexp)

| # | Divergence | Detail |
|---|---|---|
| 1 | `search.allow_expensive_queries=false` not honoured | `StringFieldType.regexpQuery` does consult the setting, but the Lucene backend injects a hardcoded always-true `allowExpensiveQueries` supplier so the check can never fail in engine mode. Known family-wide gap, not regexp-specific. |
| 2 | Page pruning lost | A delegated predicate yields an all-true bitmap at the pruning stage; the previous translatable form was prunable. Correctness preserved by residual re-evaluation. |
| 3 | Non-scoring path — `boost` rejected | The columnar path is filter-only; `boost` is rejected with `ConversionException`. Delegated regexp always uses `constant_score` rewrite unless explicitly overridden via the `rewrite` parameter. |
| 4 | Field types not storable in engine mode | `wildcard`, `constant_keyword`, `version` and `flat_object` support regexp in vanilla, but indexes using them cannot be created in optimized engine mode — storage-layer limitation shared by every query front-end. |
| 5 | Multi-valued keyword fields rejected in engine mode | Parquet rejects arrays: `Cannot accept multiple values for field: [tags] of type: [keyword]`. Engine-mode storage limitation shared by every query type, not regexp-specific. Observed in E2E run. |

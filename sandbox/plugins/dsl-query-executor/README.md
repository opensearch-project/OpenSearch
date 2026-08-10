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
| 1 | `search.allow_expensive_queries=false` not honoured | The Lucene backend hardcodes an always-true supplier. Vanilla refuses regexp when this setting is false. Pre-existing property of the delegation layer. |
| 2 | Page pruning lost | A delegated predicate yields an all-true bitmap at the pruning stage; the previous translatable form was prunable. Correctness preserved by residual re-evaluation. |
| 3 | Non-scoring path — `boost` rejected | The columnar path is filter-only; `boost` is rejected with `ConversionException`. Delegated regexp always uses `constant_score` rewrite unless explicitly overridden via the `rewrite` parameter. |
| 4 | Keyword normalizer gap | Keywords with custom normalizers apply `indexedValueForSearch` transformation at Lucene query time. Delegation preserves this automatically (Lucene handles it). |
| 5 | Field types not storable in engine mode | `wildcard`, `constant_keyword`, `version` and `flat_object` support regexp in vanilla, but indexes using them cannot be created in optimized engine mode — storage-layer limitation shared by every query front-end. |

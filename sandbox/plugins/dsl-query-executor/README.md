# dsl-query-executor

A front-end sandbox plugin to the analytics engine that intercepts `_search` requests, converts DSL queries into Calcite RelNode logical plans, and executes them through the analytics engine's query pipeline.

## Architecture

```
_search request
  → SearchActionFilter (intercepts SearchAction)
    → TransportDslExecuteAction (resolves index, orchestrates pipeline)
      → SearchSourceConverter (DSL → Calcite RelNode)
      → FilterConverter (converts query + search_after to LogicalFilter)
      → DslQueryPlanExecutor (delegates to analytics engine)
      → SearchResponseBuilder (builds SearchResponse)
```

## Supported Features

- **Query DSL**: `term`, `match_all` (via QueryTranslators)
- **Pagination**: `search_after` with multi-field sorting (converted to filter conditions via FilterConverter)
- **Sorting**: Field sorts with `from`/`size`
- **Aggregations**: Metric and bucket aggregations

## Key Components

### FilterConverter

Converts DSL `query` clauses and `search_after` pagination into Calcite `LogicalFilter` nodes:

- Translates OpenSearch queries to RexNode conditions using QueryRegistry
- Converts `search_after` values into equivalent filter predicates based on sort order
- Supports multi-field sorting with proper precedence handling
- Validates that `search_after` values match sort field count

## Dependencies

- `analytics-engine` — provides `QueryPlanExecutor` and `EngineContext` via Guice (declared as `extendedPlugins`)
- `analytics-framework` — provides Calcite and shared SPI interfaces

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

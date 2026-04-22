# dsl-query-executor

A front-end sandbox plugin to the analytics engine that intercepts `_search` requests, converts DSL queries into Calcite RelNode logical plans, and executes them through the analytics engine's query pipeline.

## Architecture

```
_search request
  → SearchActionFilter (intercepts SearchAction)
    → TransportDslExecuteAction (resolves index, orchestrates pipeline)
      → SearchSourceConverter (DSL → Calcite RelNode)
      → DslQueryPlanExecutor (delegates to analytics engine)
      → SearchResponseBuilder (builds SearchResponse)
```

## Supported Features

### Query Types
- **Term Query** - Exact term matching
- **Match All Query** - Match all documents
- **Range Query** - Numeric and date range queries with full date math support

### Range Query Features
- **Operators**: `gte`, `gt`, `lte`, `lt`
- **Date Format**: Custom date formats (e.g., `dd/MM/yyyy`)
- **Timezone**: Timezone handling (defaults to UTC)
- **Date Math**: Expressions like `now-7d`, `now/d`, `now-1M/M`
- **Rounding**: Automatic end-of-day rounding for upper bounds without explicit `/`
- **Relation**: INTERSECTS relation support
- **Millisecond Precision**: TIMESTAMP(3) for accurate date comparisons

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
./gradlew :sandbox:plugins:dsl-query-executor:test -Dsandbox.enabled=true

# Integration tests
./gradlew :sandbox:plugins:dsl-query-executor:internalClusterTest -Dsandbox.enabled=true
```

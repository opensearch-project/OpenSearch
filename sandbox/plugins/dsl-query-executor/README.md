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

### Bool Query
Converts to Calcite logical expressions with full support for all clauses and `minimum_should_match`.

**Clauses:**
- `must` - Required clauses (AND logic)
- `should` - Optional clauses (OR logic)
- `must_not` - Exclusion clauses (NOT logic)
- `filter` - Filtering clauses (AND logic, no scoring)

**minimum_should_match formats:**
- Non-negative integer: `"2"` - exactly 2 clauses must match
- Negative integer: `"-1"` - total minus 1 must match
- Non-negative percentage: `"70%"` - 70% of clauses (rounded down)
- Negative percentage: `"-30%"` - can miss 30% of clauses
- Single combination: `"2<75%"` - if total ≤ 2 match all, else 75%
- Multiple combinations: `"3<-1 5<50%"` - threshold-based rules

**Example:**
```json
{
  "bool": {
    "must": [
      {"term": {"status": "active"}}
    ],
    "should": [
      {"term": {"priority": "high"}},
      {"term": {"priority": "medium"}},
      {"term": {"priority": "low"}}
    ],
    "must_not": [
      {"term": {"deleted": "true"}}
    ],
    "minimum_should_match": "2"
  }
}
```

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

# Specific test class
./gradlew :sandbox:plugins:dsl-query-executor:test --tests "BoolQueryTranslatorTests" -Dsandbox.enabled=true
```

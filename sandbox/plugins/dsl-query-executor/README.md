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

## Supported Queries

| DSL Query | Calcite Representation |
|-----------|------------------------|
| `term` | `=($field, value)` — equality filter |
| `terms` | `SEARCH($field, Sarg[v1, v2, ...])` — multi-value equality |
| `match_all` | Skipped (boolean literal `TRUE`) |
| `exists` | `IS NOT NULL($field)` — field existence check (boost not supported) |
| `prefix` | `LIKE 'value%' ESCAPE '\'` — prefix match with escaped SQL metacharacters |
| `wildcard` | `LIKE 'pattern' ESCAPE '\'` — `*`→`%`, `?`→`_`, with escape handling |

### Prefix Query

Converts to `LIKE 'prefix%' ESCAPE '\'`. Supports `case_insensitive` (wraps in `LOWER()`).
SQL metacharacters (`%`, `_`, `\`) in the prefix value are escaped.

```json
{"prefix": {"name": "lap"}}           → name LIKE 'lap%'
{"prefix": {"name": {"value": "LAP", "case_insensitive": true}}} → LOWER(name) LIKE 'lap%'
```

### Wildcard Query

Converts `*`→`%` and `?`→`_` with backslash-escape semantics matching WildcardQueryBuilder.
Supports `case_insensitive`.

```json
{"wildcard": {"name": "lap*"}}        → name LIKE 'lap%'
{"wildcard": {"name": "l?ptop"}}      → name LIKE 'l_ptop'
{"wildcard": {"name": "*book*"}}      → name LIKE '%book%'
```

Unsupported parameters for both: `boost`, `rewrite` (throw ConversionException).

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

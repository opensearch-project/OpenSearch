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

### Term Query
Converts to Calcite equality expressions.
```json
{"term": {"status": "active"}}
```

### Match All Query
Converts to boolean true literal.
```json
{"match_all": {}}
```

### Prefix Query
Converts to Calcite LIKE expressions with wildcard suffix.

**Supported parameters:**
- `value` - The prefix string
- `case_insensitive` - Case-insensitive matching (default: false)

**Unsupported parameters (throw ConversionException):**
- `boost` - Query boosting not supported
- `rewrite` - Rewrite method not supported

**Examples:**
```json
{"prefix": {"name": "lap"}}
// Converts to: name LIKE 'lap%'

{"prefix": {"name": {"value": "LAP", "case_insensitive": true}}}
// Converts to: LOWER(name) LIKE 'lap%'
```

**Special character escaping:**
- `%` → `\%` (SQL wildcard for any characters)
- `_` → `\_` (SQL wildcard for single character)
- `\` → `\\` (escape character)

### Wildcard Query
Converts to Calcite LIKE expressions with wildcard pattern translation.

**Wildcard characters:**
- `*` - Matches any character sequence (converts to SQL `%`)
- `?` - Matches any single character (converts to SQL `_`)

**Supported parameters:**
- `value` - The wildcard pattern
- `case_insensitive` - Case-insensitive matching (default: false)

**Unsupported parameters (throw ConversionException):**
- `boost` - Query boosting not supported
- `rewrite` - Rewrite method not supported

**Examples:**
```json
{"wildcard": {"name": "lap*"}}
// Converts to: name LIKE 'lap%'

{"wildcard": {"name": "l?ptop"}}
// Converts to: name LIKE 'l_ptop'

{"wildcard": {"name": {"value": "*BOOK*", "case_insensitive": true}}}
// Converts to: LOWER(name) LIKE '%book%'
```

**Special character escaping:**
- SQL special chars (`%`, `_`, `\`) are escaped before wildcard conversion
- `*` → `%` (after escaping)
- `?` → `_` (after escaping)

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

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

## Supported Features

### Query Types
- **Term Query** - Exact term matching
- **Match All Query** - Match all documents
- **Range Query** - Numeric and date range queries with full date math support
- **Bool Query** - Compound query with `must`, `should`, `must_not`, `filter` and `minimum_should_match`
- **Prefix Query** - Prefix matching via Calcite LIKE with wildcard suffix
- **Wildcard Query** - Wildcard pattern matching via Calcite LIKE

### Range Query Features
- **Operators**: `gte`, `gt`, `lte`, `lt`
- **Date Format**: Custom date formats (e.g., `dd/MM/yyyy`)
- **Timezone**: Timezone handling (defaults to UTC)
- **Date Math**: Expressions like `now-7d`, `now/d`, `now-1M/M`
- **Rounding**: Automatic end-of-day rounding for upper bounds without explicit `/`
- **Relation**: INTERSECTS relation support
- **Millisecond Precision**: TIMESTAMP(3) for accurate date comparisons

### Bool Query
Converts to Calcite logical expressions with support for all clauses. `minimum_should_match` is
supported for 0, 1, and values at or above the should-clause count (optional, OR, and AND
respectively); intermediate values (greater than 1 and below the should-clause count) are
unsupported on this path.

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
./gradlew :sandbox:plugins:dsl-query-executor:test -Dsandbox.enabled=true

# Integration tests
./gradlew :sandbox:plugins:dsl-query-executor:internalClusterTest -Dsandbox.enabled=true

# Specific test class
./gradlew :sandbox:plugins:dsl-query-executor:test --tests "BoolQueryTranslatorTests" -Dsandbox.enabled=true
```

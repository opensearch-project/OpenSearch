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

### Calcite Representation

| DSL Query | Calcite Representation |
|-----------|------------------------|
| `term` | `=($field, value)` — equality filter |
| `terms` | `SEARCH($field, Sarg[v1, v2, ...])` — multi-value equality |
| `match_all` | Skipped (boolean literal `TRUE`) |
| `exists` | `IS NOT NULL($field)` — field existence check (boost not supported) |
| `prefix` | Delegated `PREFIX_QUERY` RexCall — Lucene `PrefixQueryBuilder` on data node |
| `wildcard` | Delegated `WILDCARD_QUERY_DSL` RexCall — Lucene `WildcardQueryBuilder` on data node |

### Prefix Query

Emits a delegated `PREFIX_QUERY` RexCall (Category.FULL_TEXT). The prefix value is passed
verbatim to `PrefixQuerySerializer`, which builds a real `PrefixQueryBuilder` on the data node.
Lucene performs the matching against the term dictionary, inheriting normalizer handling and the
`index_prefixes` O(1) fast path.

```json
{"prefix": {"name": "lap"}}           → PREFIX_QUERY(MAP('field',name), MAP('query','lap'))
{"prefix": {"name": {"value": "LAP", "case_insensitive": true}}}
  → PREFIX_QUERY(MAP('field',name), MAP('query','LAP'), MAP('case_insensitive','true'))
```

### Wildcard Query

Emits a delegated `WILDCARD_QUERY_DSL` RexCall (Category.FULL_TEXT). The Lucene wildcard
pattern (`*`, `?`, `\` escapes) is passed verbatim — no SQL conversion occurs. A dedicated
serializer is used because the existing `WildcardQuerySerializer` expects SQL-form patterns
and would reinterpret a literal `%` in customer data as a wildcard.

```json
{"wildcard": {"name": "lap*"}}        → WILDCARD_QUERY_DSL(MAP('field',name), MAP('query','lap*'))
{"wildcard": {"name": "l?ptop"}}      → WILDCARD_QUERY_DSL(MAP('field',name), MAP('query','l?ptop'))
```

Supported parameters: `value` (verbatim), `case_insensitive` (delegated to builder; ASCII-only folding),
`rewrite` (passed through to builder, validated on data node). Rejected: `boost` (throws ConversionException —
scores are not surfaced). Rejected: `_name` (throws ConversionException — matched_queries not surfaced).

### Known Divergences (prefix / wildcard)

| # | Divergence | Detail |
|---|---|---|
| 1 | `search.allow_expensive_queries=false` not honoured | The Lucene backend hardcodes an always-true supplier (LuceneAnalyticsBackendPlugin:284). Vanilla refuses prefix/wildcard when this setting is false. Pre-existing property of the delegation layer. |
| 2 | Page pruning lost | A delegated predicate yields an all-true bitmap at the pruning stage (page_pruner.rs:779-782); the previous LIKE form was prunable (page_pruner.rs:24). Correctness preserved by residual re-evaluation (single_collector.rs:604-611). Follow-up: schema enrichment would enable a prunable fast path. |
| 3 | Field types not storable in this engine mode | `wildcard`, `constant_keyword`, `version` and `flat_object` support prefix/wildcard in vanilla, but indexes using them cannot be created in optimized engine mode at all — the Parquet primary format (`CoreDataFieldPlugin`) and the Lucene secondary format (`LuceneFieldFactoryRegistry`) register no writers for them, so index creation fails with `MapperParsingException` (see `CompositeFieldCapabilityIT`). Storage-layer limitation shared by every query front-end via `OpenSearchSchemaBuilder`, not a prefix/wildcard behaviour difference. |

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

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
./gradlew :sandbox:plugins:dsl-query-executor:test

# Integration tests
./gradlew :sandbox:plugins:dsl-query-executor:internalClusterTest
```

# DSL Query Executor Plugin — Architecture & Flow Diagram

> Architecture for the `dsl-query-executor` plugin on the `feature/datafusion` branch.
> This replaces the `DslLogicalPlanPlugin` / `DslConverterPlugin` approach with an
> action-filter intercept pattern and direct converter invocation.

## Overall Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          OpenSearch Server                               │
│                                                                          │
│  _search request (SearchAction)                                          │
│         │                                                                │
│         ▼                                                                │
│  SearchActionFilter (ActionFilter, order=MIN_VALUE)                      │
│         │  intercepts SearchAction.NAME                                  │
│         │  dispatches via NodeClient                                     │
│         ▼                                                                │
│  TransportDslExecuteAction.doExecute()   ← DslExecuteAction.INSTANCE    │
│         │                                                                │
│         ├─► resolveToSingleIndex(request)                                │
│         │     └─► aliases/wildcards → single concrete index              │
│         │                                                                │
│         ├─► SearchSourceConverter(engineContext.getSchema())              │
│         │         │                                                      │
│         │         ├─► catalogReader.getTable(indexName)                   │
│         │         │                                                      │
│         │         ├─► Shared base: TableScan → Filter                    │
│         │         │                                                      │
│         │         ├─► HITS path:                                         │
│         │         │     Project → Sort                                   │
│         │         │                                                      │
│         │         └─► AGGS path:                                         │
│         │               AggregationTreeWalker.walk()                     │
│         │                 → List<AggregationMetadata>                    │
│         │               AggregateConverter.convert() (per metadata)      │
│         │                                                                │
│         ├─► QueryPlanExecutor.execute(plans)  ← RelNode → rows          │
│         │     └─► analytics-engine execution                             │
│         │                                                                │
│         └─► SearchResponseBuilder.build(results, tookInMillis)           │
│               └─► results → SearchResponse                              │
│                                                                          │
│  Dependencies:                                                           │
│    analytics-engine  — QueryPlanExecutor, EngineContext (via Guice)       │
│    analytics-framework — Calcite, shared SPI interfaces                  │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## Class Hierarchy

### Plugin Layer
```
DslQueryExecutorPlugin (extends Plugin, implements ActionPlugin)
  ├── SearchActionFilter          — intercepts _search, dispatches to DslExecuteAction
  ├── TransportDslExecuteAction   — orchestrates DSL → QueryPlans → SearchResponse
  │     (Guice-injected: EngineContext, QueryPlanExecutor, ClusterService,
  │      IndexNameExpressionResolver)
  └── DslExecuteAction            — action registration (INSTANCE singleton)
```

### Converter Layer
```
SearchSourceConverter (replaces DslLogicalPlanService)
  ├── SchemaPlus                  — from EngineContext (analytics engine), NOT IndexMappingClient
  ├── RelDataTypeFactory          — Calcite type factory, created internally
  ├── HepPlanner                  — Calcite heuristic planner
  ├── RelOptCluster               — Calcite planning cluster
  ├── CalciteCatalogReader        — resolves table metadata from schema
  ├── FilterConverter             — DSL query → LogicalFilter
  ├── ProjectConverter            — _source fields → LogicalProject
  ├── SortConverter               — sort + pagination → LogicalSort
  ├── AggregateConverter          — AggregationMetadata → LogicalAggregate (standalone)
  └── AggregationTreeWalker       — walks DSL agg tree → List<AggregationMetadata>
```

### Shared Context
```
ConversionContext (simplified)
  ├── SearchSourceBuilder         — original DSL source
  ├── RelOptCluster               — Calcite planning cluster
  └── RelOptTable                 — resolved table reference
  Provides: getSearchSource(), getCluster(), getTable(), getRowType(), getRexBuilder()
  Removed:  indexName, indexSchema, capabilities, mutable AggregationMetadata
```

### Converter Classes
```
AbstractDslConverter (abstract)
  convert(RelNode input, ConversionContext ctx)
    └── isApplicable(ctx) → doConvert(input, ctx)
  No PipelinePhase or ordering — converters called directly in sequence

FilterConverter (extends AbstractDslConverter)
  └── skips MatchAllQueryBuilder; uses QueryRegistry → RexNode

ProjectConverter (extends AbstractDslConverter)
  └── _source field selection → LogicalProject

SortConverter (extends AbstractDslConverter)
  └── sort clauses + offset/fetch → LogicalSort

AggregateConverter (standalone — NOT AbstractDslConverter)
  convert(RelNode input, AggregationMetadata metadata)
  └── LogicalAggregate.create(input, groupByBitSet, null, aggregateCalls)
```

### Query Translation Layer
```
QueryRegistry (extends HandlerRegistry<QueryBuilder, QueryTranslator>)
  └── Map<Class<QueryBuilder>, QueryTranslator>

QueryRegistryFactory.create():
  ├── TermQueryTranslator          → EQUALS($field, literal)
  └── MatchAllQueryTranslator      → boolean TRUE
```

### Aggregation Translation Layer
```
AggregationRegistry (extends HandlerRegistry<AggregationBuilder, AggregationType>)
  └── Map<Class<AggregationBuilder>, AggregationType>

AggregationRegistryFactory.create():
  ├── AvgMetricTranslator          → AVG()
  ├── SumMetricTranslator          → SUM()
  ├── MinMetricTranslator          → MIN()
  ├── MaxMetricTranslator          → MAX()
  └── TermsBucketTranslator        → GROUP BY field
```

### Aggregation Tree Walker
```
AggregationTreeWalker.walk(aggs, rowType, typeFactory)
  └── walkRecursive(aggs, currentGroupings, granularities)
        ├── BucketShape → accumulate groupings, recurse into sub-aggs
        └── MetricTranslator → add AggregateCall to builder

Output: List<AggregationMetadata>
  Each contains: ImmutableBitSet (GROUP BY), List<AggregateCall>
```

### Execution & Response Layer
```
DslQueryPlanExecutor
  └── wraps analytics-engine QueryPlanExecutor<RelNode, Iterable<Object[]>>
      iterates QueryPlans, executes each RelNode, collects ExecutionResults
      logs plan via relNode.explain() at INFO level

SearchResponseBuilder (simplified)
  └── static build(List<ExecutionResult> results, long tookInMillis)
      currently returns empty SearchHits (TODO: populate from results)
```

## Detailed Flow: TransportDslExecuteAction.doExecute()

```
1. SearchActionFilter.apply(action, request, listener, chain)
   │  action == SearchAction.NAME?
   │    yes → dispatch to DslExecuteAction via NodeClient
   │    no  → chain.proceed(action, request, listener)
   │
   2. TransportDslExecuteAction.doExecute(task, request, listener)
   │   │
   │   2a. resolveToSingleIndex(request)
   │   │     → uses IndexNameExpressionResolver
   │   │     → aliases/wildcards → single concrete index name
   │   │
   │   2b. new SearchSourceConverter(engineContext.getSchema())
   │   │     → receives SchemaPlus from analytics engine (via Guice)
   │   │     → creates Calcite infrastructure internally:
   │   │         RelDataTypeFactory, HepPlanner, RelOptCluster, CalciteCatalogReader
   │   │     → creates converters: FilterConverter, ProjectConverter,
   │   │         SortConverter, AggregateConverter
   │   │     → creates AggregationTreeWalker with AggregationRegistryFactory.create()
   │   │
   │   2c. converter.convert(request.source(), indexName)
   │   │     │
   │   │     2c-i.  catalogReader.getTable(List.of(indexName))
   │   │     │        → resolve table from analytics engine schema
   │   │     │
   │   │     2c-ii. Create ConversionContext(searchSource, cluster, table)
   │   │     │
   │   │     2c-iii. Shared base:
   │   │     │        LogicalTableScan.create(cluster, table)
   │   │     │          → filterConverter.convert(base, ctx)
   │   │     │        Result: scan → filter (shared by both paths)
   │   │     │
   │   │     2c-iv. [HITS path]
   │   │     │        projectConverter.convert(filtered, ctx)
   │   │     │          → sortConverter.convert(projected, ctx)
   │   │     │        Result: scan → filter → project → sort
   │   │     │
   │   │     2c-v.  [AGGS path]
   │   │              treeWalker.walk(aggs, rowType, typeFactory)
   │   │                → List<AggregationMetadata>
   │   │              For each metadata:
   │   │                aggregateConverter.convert(filtered, metadata)
   │   │              Result: scan → filter → aggregate (per granularity)
   │   │     │
   │   │     2c-vi. Return QueryPlans (HITS plan + N AGGREGATION plans)
   │   │
   │   2d. planExecutor.execute(plans)
   │   │     → analytics-engine QueryPlanExecutor<RelNode, Iterable<Object[]>>
   │   │     → iterates plans, executes each RelNode
   │   │     → collects List<ExecutionResult>
   │   │
   │   2e. SearchResponseBuilder.build(results, tookInMillis)
   │         → assembles SearchResponse from execution results
   │         → currently returns empty SearchHits (TODO)
```

## Key Differences from DslLogicalPlanPlugin Architecture

| Aspect | Old (DslLogicalPlanPlugin) | New (DslQueryExecutorPlugin) |
|--------|---------------------------|------------------------------|
| Entry point | `DslConverterPlugin.convertDsl()` called by server | `SearchActionFilter` intercepts `_search` at action layer |
| Schema source | `IndexMappingClient` fetches mappings from cluster | `EngineContext.getSchema()` from analytics engine (Guice) |
| Converter orchestration | `ConversionPipeline` auto-sorts by `PipelinePhase` order | Converters called directly in sequence — no pipeline |
| Scan + Filter | Each pipeline starts with its own Scan → Filter | Shared base: single Scan → Filter, then branch for hits vs aggs |
| AggregateConverter | Extends `AbstractDslConverter`, uses `ConversionContext` | Standalone class, takes `(RelNode, AggregationMetadata)` directly |
| ConversionContext | Holds indexName, indexSchema, capabilities, mutable agg metadata | Simplified: only SearchSourceBuilder, cluster, table |
| Registry creation | Inline in `DslLogicalPlanService` constructor | Factory classes: `QueryRegistryFactory`, `AggregationRegistryFactory` |
| Execution | `DefaultQueryPlanExecutor` → `RelNodeExecutor` (placeholder) | `DslQueryPlanExecutor` wraps analytics-engine `QueryPlanExecutor` |
| Dependencies | Self-contained (uses OpenSearch Client for schema) | Depends on `analytics-engine` and `analytics-framework` (extendedPlugins) |

## How to Add New Query Translators

### Adding a new term-level query (e.g., `terms` → IN operator)

1. Create `TermsQueryTranslator.java` in `org.opensearch.dsl.query`:
```java
public class TermsQueryTranslator implements QueryTranslator {
    public Class<? extends QueryBuilder> getQueryType() {
        return TermsQueryBuilder.class;
    }
    public RexNode convert(QueryBuilder query, ConversionContext ctx) {
        // Build IN($field, val1, val2, ...) using SqlStdOperatorTable.IN
    }
}
```

2. Register in `QueryRegistryFactory.create()`:
```java
public static QueryRegistry create() {
    QueryRegistry registry = new QueryRegistry();
    registry.register(new TermQueryTranslator());
    registry.register(new MatchAllQueryTranslator());
    registry.register(new TermsQueryTranslator());  // ← add here
    return registry;
}
```

### Adding a new single-value metric aggregation (e.g., `value_count`)

1. Create `ValueCountMetricTranslator.java` in `org.opensearch.dsl.aggregation.metric`:
```java
public class ValueCountMetricTranslator extends AbstractMetricTranslator<ValueCountAggregationBuilder> {
    public Class<ValueCountAggregationBuilder> getAggregationType() { ... }
    protected SqlAggFunction getAggFunction() { return SqlStdOperatorTable.COUNT; }
    protected String getFieldName(ValueCountAggregationBuilder agg) { return agg.field(); }
    public InternalAggregation toInternalAggregation(String name, Object value) {
        return new InternalValueCount(name, value == null ? 0 : ((Number)value).longValue(), Map.of());
    }
}
```

2. Register in `AggregationRegistryFactory.create()`:
```java
public static AggregationRegistry create() {
    AggregationRegistry registry = new AggregationRegistry();
    registry.register(new AvgMetricTranslator());
    registry.register(new SumMetricTranslator());
    // ...existing registrations...
    registry.register(new ValueCountMetricTranslator());  // ← add here
    return registry;
}
```

### Adding a new bucket aggregation (e.g., `histogram`)

1. Create `HistogramBucketShape.java` implementing `BucketShape<HistogramAggregationBuilder>`:
```java
public class HistogramBucketShape implements BucketShape<HistogramAggregationBuilder> {
    public GroupingInfo getGrouping(HistogramAggregationBuilder agg) {
        // Return ExpressionGrouping with FLOOR(field/interval)*interval
    }
    public BucketOrder getOrder(...) { ... }
    public Collection<AggregationBuilder> getSubAggregations(...) { ... }
    public InternalAggregation toBucketAggregation(..., List<BucketEntry> buckets) {
        // Build InternalHistogram from bucket entries
    }
}
```

2. Register in `AggregationRegistryFactory.create()`.

## Key Design Patterns

1. **Action Filter intercept** — `SearchActionFilter` sits at `Integer.MIN_VALUE` order, intercepting all `_search` requests before any other filter. Non-search actions pass through unchanged. This replaces the `DslConverterPlugin` interface that required server-side integration.

2. **Direct converter sequencing** — Instead of a `ConversionPipeline` that auto-sorts by phase order, converters are called explicitly in a fixed sequence within `SearchSourceConverter.convert()`. This makes the flow easier to follow and removes the indirection of phase ordering.

3. **Shared Scan→Filter base** — Both hits and aggregation paths share a single `LogicalTableScan → LogicalFilter` base. The old architecture created separate scan/filter nodes per pipeline. This avoids redundant work and makes the branching point explicit.

4. **Schema from analytics engine** — The schema comes from `EngineContext.getSchema()` (injected via Guice from the `analytics-engine` plugin) rather than being fetched from cluster mappings via `IndexMappingClient`. This decouples the DSL converter from cluster state and leverages the engine's type system.

5. **Registry factory pattern** — `QueryRegistryFactory.create()` and `AggregationRegistryFactory.create()` encapsulate registry construction. Adding a new translator is: implement the interface + add one line to the factory method.

6. **Standalone AggregateConverter** — Unlike other converters that extend `AbstractDslConverter` and receive `ConversionContext`, `AggregateConverter` takes `(RelNode, AggregationMetadata)` directly. This reflects that aggregation conversion depends on walker output, not on the shared conversion context.

7. **Granularity decomposition** — `AggregationTreeWalker` still produces one `AggregationMetadata` per unique GROUP BY key set. The converter loops over them, creating one `LogicalAggregate` per granularity from the shared filtered base.

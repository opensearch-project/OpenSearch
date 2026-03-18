# Lucene Backend Plugin — Design

## Scope

This document covers the design of the Lucene Backend Plugin (`analytics-backend-lucene`) for the analytics query engine. In scope:

- Plugin registration and SPI integration with the analytics engine
- Planner-side predicate identification, validation, and conversion (RexNode → QueryBuilder → byte[])
- Executor-side shard initialization, Lucene query execution, and BitSet result packaging
- Segment-level and batch-level execution aligned with DataFusion's Parquet processing
- Extensible `PredicateHandler` registry for adding new predicate types
- Weight caching, resource lifecycle management, and fail-fast validation
- Custom `LUCENE_DELEGATED` operator for in-place plan annotation (design only, not yet implemented)

Out of scope (upstream dependencies):

- `DefaultPlanExecutor` predicate splitting and lifecycle orchestration
- `LUCENE_DELEGATED` custom SqlOperator implementation
- BitSet handoff mechanism to DataFusion
- Multi-shard coordination and fan-out from coordinator
- Wiring `ClusterService`/`IndicesService` into the executor

## Overview

DataFusion handles columnar scans, numeric filtering, and aggregations over Parquet DocValues. Lucene handles text-search predicates (EQUALS, LIKE on keyword fields) against inverted indices. The coordinator identifies Lucene-compatible predicates and serializes them. The data node executes them against local shards and returns a BitSet for DataFusion to intersect with its scan results.

---

## Planner vs Executor

The Lucene Backend Plugin participates in two distinct phases with different roles:

- **Planner phase** (coordinator): identifies Lucene-eligible predicates, converts RexNode → QueryBuilder → serialized bytes. No shard context. Pure logical transformation. This is `convertFragment`.
- **Executor phase** (data node): receives serialized bytes, initializes shard resources, executes Lucene queries, returns BitSet. Requires IndexShard, Engine.Searcher, QueryShardContext. This is `initialize` / `execute` / `close`.

Both phases use `LuceneEngineBridge` but through different methods:

| Phase    | Role                                          | Bridge Method       | Runs On     | Needs Shard Context |
|----------|-----------------------------------------------|---------------------|-------------|---------------------|
| Planner  | Identify eligible predicates                  | `canHandle()`       | Coordinator | No (uses cluster state + Calcite types) |
| Planner  | Convert RexNode → QueryBuilder → byte[]       | `convertFragment()` | Coordinator | No                  |
| Executor | Acquire shard resources (searcher, QSC)       | `initialize()`      | Data Node   | Yes                 |
| Executor | Deserialize → Lucene Query → search → BitSet  | `execute()`         | Data Node   | Yes (cached)        |
| Executor | Release searcher, allocator                    | `close()`           | Data Node   | Yes (cleanup)       |

### Planner Validation: Fail Fast

The planner runs on the coordinator which has access to `ClusterState` → `IndexMetadata` → `MappingMetadata`. This means the planner can validate at planning time — before any bytes are sent to data nodes:

- **Field existence**: does the field exist in the index mapping?
- **Field type**: is it `keyword` or `text` (has Lucene inverted index) vs `integer`/`long`/`date` (no inverted index for text search)?
- **Index existence**: does the target index exist at all?

Currently `canHandle` checks the Calcite `SqlTypeName` (VARCHAR vs INTEGER) as a proxy. This catches obvious mismatches (numeric field routed to Lucene) but can't distinguish `keyword` from `text` from `ip` since all map to VARCHAR.

For full fail-fast validation, the planner should check the actual OpenSearch field type from cluster state before routing to LBP. The recommended approach:

```
Planner validation chain (coordinator):
  1. Planner checks ClusterState → MappingMetadata → field type
     → "verb" is "keyword" → has inverted index → Lucene-eligible
     → "status" is "integer" → no inverted index → DataFusion only
  2. PredicateHandlerRegistry.canHandle(call, rowType)
     → secondary check: operand shape, Calcite type
  3. luceneBridge.convertFragment(...)
     → conversion only happens after both checks pass
```

This ensures errors like "field doesn't exist" or "field is numeric, not text" are caught at planning time on the coordinator, not at execution time on the data node.

**Current state of upstream access:** `AnalyticsPlugin` has `ClusterService` and passes it to `DefaultEngineContext`. However, `DefaultPlanExecutor` does not currently receive `ClusterService` or `EngineContext` — it only gets `List<AnalyticsSearchBackendPlugin>`. For the planner to do mapping-level validation, `DefaultPlanExecutor` needs `ClusterService` (or `MappingMetadata`) wired into it. This is an upstream change in `analytics-engine`.

---

## Example: Plan Transformation and Execution

### Query

```sql
SELECT verb, status FROM logs WHERE verb = 'GET' AND status > 200 AND path LIKE '/api%'
```

### Step 1: Calcite Logical Plan (before planner traversal)

```
LogicalProject(verb=[$0], status=[$2])
  LogicalFilter(condition=[AND(
    =($0, 'GET'),           ← RexCall(EQUALS, [RexInputRef#0, RexLiteral('GET')])
    >($2, 200),             ← RexCall(GREATER_THAN, [RexInputRef#2, RexLiteral(200)])
    LIKE($1, '/api%')       ← RexCall(LIKE, [RexInputRef#1, RexLiteral('/api%')])
  )])
    LogicalTableScan(table=[[logs]])

Field types from schema (via OpenSearchSchemaBuilder):
  $0: verb   → VARCHAR (keyword in OS)
  $1: path   → VARCHAR (keyword in OS)
  $2: status → INTEGER (integer in OS)
```

### Step 2: Planner Traversal (coordinator)

The planner walks each RexNode child of the AND condition. For each, it checks the cluster state mapping first, then `canHandle`:

```
RexCall(EQUALS, verb, 'GET'):
  ClusterState → MappingMetadata → verb is "keyword" → has inverted index ✓
  PredicateHandlerRegistry.canHandle(call, rowType)?
    → EqualsPredicateHandler: SqlKind=EQUALS ✓, operands=FieldRef+Literal ✓, field type=VARCHAR ✓
    → YES → Lucene

RexCall(GREATER_THAN, status, 200):
  ClusterState → MappingMetadata → status is "integer" → no inverted index ✗
  → NO → DataFusion (skips canHandle entirely)

RexCall(LIKE, path, '/api%'):
  ClusterState → MappingMetadata → path is "keyword" → has inverted index ✓
  PredicateHandlerRegistry.canHandle(call, rowType)?
    → LikePredicateHandler: SqlKind=LIKE ✓, operands=FieldRef+Literal ✓, field type=VARCHAR ✓
    → YES → Lucene
```

### Step 3: Annotated Plan (after planner traversal)

The planner replaces Lucene-eligible RexNodes in-place with a custom `LUCENE_DELEGATED` operator. This keeps the plan self-contained — no side-channel fragment lists. The custom operator carries the original operands (for explain/debugging) plus the serialized `byte[]` payload.

```
LogicalProject(verb=[$0], status=[$2])
  LogicalFilter(condition=[AND(
    LUCENE_DELEGATED($0, 'GET', byte[]{TermQueryBuilder("verb","GET")}),
    >($2, 200),
    LUCENE_DELEGATED($1, '/api%', byte[]{PrefixQueryBuilder("path","/api")})
  )])
    LogicalTableScan(table=[[logs]])
```

This is still a valid Calcite plan — `LUCENE_DELEGATED` is a custom `SqlOperator` registered in the framework. The plan can be serialized, transported, and explained like any Calcite plan. The `byte[]` payload is the output of `luceneBridge.convertFragment`.

Why in-place replacement over a side-channel:
- Plan is self-contained — no separate `delegated_fragments` list to keep in sync
- Boolean structure preserved — `AND(LUCENE_DELEGATED, DF_predicate)` keeps correlation
- Calcite plan serialization/transport works as-is
- Plan explain/toString shows exactly what was delegated and where
- Executor walks the RexNode tree naturally — `LUCENE_DELEGATED` → execute in Lucene, regular operator → DataFusion

Note: `LUCENE_DELEGATED` is an upstream concept — it would live in `analytics-framework` or `analytics-engine`, not in LBP. LBP's role stays the same: `convertFragment` produces the `byte[]` payload, `execute` consumes it.

### Step 4: Data Node Execution

```
Executor receives the plan with LUCENE_DELEGATED operators in-place.

1. Initialize Lucene bridge:
   luceneBridge.initialize(shardCtx)
     → acquires Engine.Searcher (cached)
     → creates QueryShardContext (cached)

2. DataFusion processes Parquet DocValues segment-by-segment.
   For each segment (segOrd), Parquet is decompressed in batches (startDocId → endDocId).
   For each batch, the executor calls Lucene for the matching doc range:

   Segment 0 (maxDoc=5000):
     Batch 0: startDocId=0, endDocId=1024
       luceneBridge.executeForSegment(termQB_bytes, segOrd=0, start=0, end=1024)
         → BitSet for docs 0-1023 in segment 0
       luceneBridge.executeForSegment(prefixQB_bytes, segOrd=0, start=0, end=1024)
         → BitSet for docs 0-1023 in segment 0
       Intersect → pass to DataFusion for status > 200 on this batch

     Batch 1: startDocId=1024, endDocId=2048
       luceneBridge.executeForSegment(termQB_bytes, segOrd=0, start=1024, end=2048)
       ...

   Segment 1 (maxDoc=3000):
     Batch 0: startDocId=0, endDocId=1024
       luceneBridge.executeForSegment(termQB_bytes, segOrd=1, start=0, end=1024)
       ...

3. Helper methods for DataFusion to drive iteration:
   luceneBridge.getSegmentCount() → number of segments
   luceneBridge.getSegmentMaxDoc(segOrd) → maxDoc for batch boundary calculation

4. Close:
   luceneBridge.close()
     → releases Engine.Searcher, Arrow allocator
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  COORDINATOR NODE — PLANNER PHASE                                       │
│                                                                         │
│  Front-end (PPL/SQL) → Calcite RelNode tree                            │
│    │                                                                    │
│    ▼                                                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Planner / Delegation Decider                                      │  │
│  │                                                                   │  │
│  │  Walk RexNode condition tree                                      │  │
│  │    │                                                              │  │
│  │    ├── For each predicate:                                        │  │
│  │    │     1. Check ClusterState → MappingMetadata → OS field type  │  │
│  │    │     2. Check PredicateHandlerRegistry.canHandle(call, rowType, mapperService)
│  │    │        → validates SqlKind + operand shape + SqlTypeName     │  │
│  │    │                                                              │  │
│  │    ├── Text field + supported operator → Lucene-eligible          │  │
│  │    └── Non-text field or unsupported operator → DataFusion        │  │
│  │                                                                   │  │
│  │  luceneBridge.convertFragment(LogicalFilter wrapping EQUALS)      │  │
│  │    │                                                              │  │
│  │    ▼                                                              │  │
│  │  ┌─────────────────────────────────────────────────────────────┐  │  │
│  │  │ LuceneEngineBridge.convertFragment (PLANNER)                 │  │  │
│  │  │                                                             │  │  │
│  │  │  RexToQueryBuilderConverter(rowType, mapperService)          │  │  │
│  │  │    → PredicateHandlerRegistry.getHandler(EQUALS)            │  │  │
│  │  │    → handler.canHandle(call, rowType, mapperService)        │  │  │
│  │  │    → handler.convert(call, rowType, mapperService)          │  │  │
│  │  │    → QueryBuilder                                          │  │  │
│  │  │                                                             │  │  │
│  │  │  QueryBuilderSerializer.serialize()                         │  │  │
│  │  │    → byte[] (NamedWriteable format)                         │  │  │
│  │  │                                                             │  │  │
│  │  │  MapperService available on coordinator via                  │  │  │
│  │  │  IndicesService → IndexService → mapperService().            │  │  │
│  │  │  No IndexShard or Engine.Searcher needed.                   │  │  │
│  │  └─────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  Planner output: single Calcite plan with LUCENE_DELEGATED custom       │
│  RexNodes replacing Lucene-eligible predicates → sent to data node      │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ Modified Calcite Plan
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA NODE — EXECUTOR PHASE                                             │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ DefaultPlanExecutor (analytics-engine)                            │  │
│  │                                                                   │  │
│  │  shardCtx = new DefaultShardExecutionContext(indexShard, idxSvc)  │  │
│  │    │                                                              │  │
│  │    ▼                                                              │  │
│  │  luceneBridge.initialize(shardCtx)                                │  │
│  │  ┌─────────────────────────────────────────────────────────────┐  │  │
│  │  │ LuceneEngineBridge.initialize (EXECUTOR)                    │  │  │
│  │  │                                                             │  │  │
│  │  │  engineSearcher = indexShard.acquireSearcher(...)            │  │  │
│  │  │  queryShardContext = indexService.newQueryShardContext(...)  │  │  │
│  │  │    (both cached for lifetime of shard execution)            │  │  │
│  │  │    (on partial failure: searcher closed before re-throw)    │  │  │
│  │  └─────────────────────────────────────────────────────────────┘  │  │
│  │    │                                                              │  │
│  │    ▼                                                              │  │
│  │  Walk plan RexNode tree, for each LUCENE_DELEGATED node:          │  │
│  │  luceneBridge.execute(byte[])  ← full shard, or:                 │  │
│  │  luceneBridge.executeForSegment(byte[], segOrd, start, end)      │  │
│  │  ┌─────────────────────────────────────────────────────────────┐  │  │
│  │  │ LuceneEngineBridge.execute / executeForSegment              │  │  │
│  │  │                                                             │  │  │
│  │  │  getOrCreateWeight(bytes): (cached per unique fragment)     │  │  │
│  │  │    first call: deserialize → toQuery → rewrite → Weight     │  │  │
│  │  │    subsequent: returns cached Weight                        │  │  │
│  │  │                                                             │  │  │
│  │  │  execute():          all segments → global FixedBitSet      │  │  │
│  │  │  executeForSegment(): single segment + doc range            │  │  │
│  │  │    → scorer.advance(startDocId)                             │  │  │
│  │  │    → iterate until endDocId → scoped FixedBitSet            │  │  │
│  │  │                                                             │  │  │
│  │  │  FixedBitSet → Arrow BitVector → VectorSchemaRoot           │  │  │
│  │  └─────────────────────────────────────────────────────────────┘  │  │
│  │    │                                                              │  │
│  │    ▼                                                              │  │
│  │  Intersect Lucene BitSets, pass to DataFusion with remaining      │  │
│  │  predicates for Parquet scan                                      │  │
│  │    │                                                              │  │
│  │    ▼                                                              │  │
│  │  luceneBridge.close()                                             │  │
│  │  ┌─────────────────────────────────────────────────────────────┐  │  │
│  │  │ LuceneEngineBridge.close (EXECUTOR — releases resources)    │  │  │
│  │  │  engineSearcher.close(), allocator.close()                  │  │  │
│  │  └─────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌────────────────┐  ┌──────────────────────┐                          │
│  │ Lucene Indices  │  │ Parquet DocValues    │                          │
│  │ (inverted idx)  │  │ (columnar storage)   │                          │
│  └────────────────┘  └──────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Components

### Framework (`analytics-framework`) — shared contracts, no server dependency

| Interface                                          | Role                                                                                |
|----------------------------------------------------|-------------------------------------------------------------------------------------|
| `SearchExecEngine<T, V>`                           | `prepare(ctx)` → `execute(ctx)` → `close()`. Shard-level execution engine.          |
| `ExecutionContext`                                  | Carries reader, task, and table name through execution lifecycle.                    |
| `EngineResultStream` / `EngineResultBatch`         | Result stream of record batches returned by engine execution.                        |
| `EngineBridge<Fragment,Stream,LogicalPlan>`         | Planner-side: `convertFragment(plan)`. Executor-side: `initialize`/`execute`/`close`.|
| `ShardExecutionContext`                             | Opaque shard context marker. Backends downcast to concrete type.                     |
| `AnalyticsSearchBackendPlugin`                      | SPI: extends `SearchExecEngineProvider`. `name()`, `createSearchExecEngine(ctx)`. |

### Analytics Engine (`analytics-engine`) — orchestrator

| Component                      | Role                                                                                    |
|--------------------------------|-----------------------------------------------------------------------------------------|
| `AnalyticsPlugin`              | Discovers backends via SPI, aggregates operator tables, creates executor                |
| `DefaultPlanExecutor`          | Receives RelNode + context. Splits predicates, delegates to backends. (Currently stub.) |
| `DefaultShardExecutionContext` | Wraps `IndexShard` + `IndexService`. Provides `acquireSearcher()` and `createQueryShardContext()`. |

### Lucene Backend (`analytics-backend-lucene`)

The plugin has two SPI entry points and is organized into three layers: plugin registration, query execution infrastructure, and predicate conversion.

#### SPI Entry Points

The plugin registers through two separate SPI interfaces serving different upstream consumers:

| Entry Point                  | SPI Interface                    | Purpose                                                    |
|------------------------------|----------------------------------|------------------------------------------------------------|
| `LuceneSearchEnginePlugin`   | `SearchBackEndPlugin`            | Reader management. Creates `LuceneReaderManager` for `DirectoryReader` lifecycle per `CatalogSnapshot`. Used by the shard engine for index reader acquisition. |
| `LuceneBackendPlugin`        | `AnalyticsSearchBackendPlugin`   | Analytics query execution. Creates `LuceneSearchExecEngine` via `createSearchExecEngine(ctx)`. Used by the analytics engine for predicate delegation. |

#### Query Execution Infrastructure (data node)

These components handle shard-level Lucene query execution. `LuceneSearchExecEngine` orchestrates through `LuceneEngineBridge`, which delegates segment-level scoring to `LuceneIndexFilterProvider`.

| Component                    | Owner    | Role                                                                                     |
|------------------------------|----------|------------------------------------------------------------------------------------------|
| `LuceneSearchExecEngine`    | Ours     | Single entry point for analytics search. Implements `SearchExecEngine`. Orchestrates bridge lifecycle. |
| `LuceneEngineBridge`        | Ours     | Core bridge. Planner: `convertFragment` (RexNode → byte[]). Executor: `initialize`/`execute`/`executeForSegment`/`close`. Delegates scoring to `LuceneIndexFilterProvider`. Packages results as Arrow BitVector. |
| `LuceneIndexFilterProvider`  | Upstream | Per-segment scoring: Weight → Scorer → `DocIdSetIterator` → `long[]` bitset. Manages collector lifecycle. Our bridge delegates here. |
| `LuceneIndexFilterContext`   | Upstream | Per-(query, reader) context. Caches `Weight`, exposes segment count/maxDoc. |
| `LuceneSearchEnginePlugin`   | Upstream | `SearchBackEndPlugin` SPI entry point. Creates `LuceneReaderManager` for `DirectoryReader` lifecycle. |
| `LuceneReaderManager`       | Upstream | Manages `DirectoryReader` per `CatalogSnapshot`. Used by shard engine for reader acquisition. |

#### Predicate Conversion (coordinator + data node)

These components handle RexNode → QueryBuilder conversion and serialization. All ours — upstream doesn't have Calcite integration.

| Component                    | Role                                                                                     |
|------------------------------|------------------------------------------------------------------------------------------|
| `PredicateHandler`           | Extension point interface: SqlKind + SqlOperator + `canHandle` + `convert` + serialization entries. |
| `PredicateHandlerRegistry`   | Single registration point for all handlers. Drives converter and serializer.             |
| `EqualsPredicateHandler`     | `field = 'val'` → `TermQueryBuilder`. Validates VARCHAR/CHAR field type via `canHandle`. |
| `LikePredicateHandler`       | `field LIKE 'pat%'` → `PrefixQueryBuilder` / `WildcardQueryBuilder`. Validates VARCHAR/CHAR. |
| `RexToQueryBuilderConverter` | Dispatches `RexCall` to handler via registry. Validates via `canHandle` before `convert`. Accepts optional `MapperService` for field type validation. |
| `QueryBuilderSerializer`     | Round-trip `QueryBuilder` ↔ `byte[]` via `NamedWriteableRegistry`. Registry entries derived from handlers. This is the transport format between coordinator and data node. |

### DataFusion Backend (`analytics-backend-datafusion`)

| Component          | Role                                                                                     |
|--------------------|------------------------------------------------------------------------------------------|
| `DataFusionBridge` | `EngineBridge<byte[], Long, RelNode>`. Substrait → native Rust. Ignores `initialize`/`close`. |

---

## Upstream Dependencies (What LBP Needs)

| From                           | What                                                        | Phase    | Used In                                |
|--------------------------------|-------------------------------------------------------------|----------|----------------------------------------|
| `ClusterState` (via planner)   | `IndexMetadata` → `MappingMetadata` → OS field types        | Planner  | Fail-fast validation (field exists, is keyword/text) |
| `IndicesService` (via planner) | `IndexService` → `MapperService` → `MappedFieldType`        | Planner  | Field type validation in `canHandle`/`convert` |
| Planner                       | Predicate splitting + `canHandle` checks                    | Planner  | Before `convertFragment()`             |
| `DefaultShardExecutionContext` | `IndexShard.acquireSearcher()` → `Engine.Searcher`          | Executor | `initialize()`                         |
| `DefaultShardExecutionContext` | `IndexService.newQueryShardContext()` → `QueryShardContext`  | Executor | `initialize()`                         |
| `QueryShardContext`            | `fieldMapper(name)` → `MappedFieldType`                     | Executor | `execute()` via `toQuery()`            |
| `DefaultPlanExecutor`          | Lifecycle orchestration: `initialize` → N × `execute` → `close` | Executor | Per shard                          |

## Downstream Output (What LBP Provides)

| To                    | What                  | Format                                                                           |
|-----------------------|-----------------------|----------------------------------------------------------------------------------|
| DataFusion / Executor | Matching doc IDs      | `Iterator<VectorSchemaRoot>` with `doc_ids` BitVector. Bit i=1 if doc i matched. |
| Planner               | Predicate eligibility | `PredicateHandlerRegistry.canHandle(call, rowType, mapperService)` → boolean     |

---

## Extensibility: Adding New Predicate Types

Each `PredicateHandler` bundles: what SQL operator it handles, how to convert RexCall → QueryBuilder, and what serialization entries are needed. Adding a new predicate = one new class + one registry line.

```
PredicateHandlerRegistry
  │
  ├── RexToQueryBuilderConverter: registry.getHandler(kind).convert(call, rowType, mapperService)
  └── QueryBuilderSerializer:     new NamedWriteableRegistry(registry.allNamedWriteableEntries())
```

Example — adding range queries:

1. Create `RangePredicateHandler implements PredicateHandler`
2. Add to `PredicateHandlerRegistry.HANDLERS`
3. Done. No other files change.

Current handlers:

| Handler                  | SQL                 | QueryBuilder                                  |
|--------------------------|---------------------|-----------------------------------------------|
| `EqualsPredicateHandler` | `field = 'val'`     | `TermQueryBuilder`                            |
| `LikePredicateHandler`   | `field LIKE 'pat%'` | `PrefixQueryBuilder` / `WildcardQueryBuilder` |

---

## Supported Predicates

The plugin currently supports text-search predicates on keyword/text fields. Each predicate shape maps to a specific Lucene QueryBuilder type. New shapes are added by implementing `PredicateHandler` — see the Extensibility section.

| SQL Shape                    | Field Type       | Lucene QueryBuilder      | Status      |
|------------------------------|------------------|--------------------------|-------------|
| `field = 'value'`           | keyword, text    | `TermQueryBuilder`       | Implemented |
| `field LIKE 'prefix%'`      | keyword, text    | `PrefixQueryBuilder`     | Implemented |
| `field LIKE '%pattern%'`    | keyword, text    | `WildcardQueryBuilder`   | Implemented |
| `field LIKE '_attern'`      | keyword, text    | `WildcardQueryBuilder`   | Implemented |
| `A AND B` (both Lucene)     | keyword, text    | `BoolQueryBuilder(must)` | Planned     |
| `A OR B` (both Lucene)      | keyword, text    | `BoolQueryBuilder(should)` | Planned   |
| `NOT A`                     | keyword, text    | `BoolQueryBuilder(mustNot)` | Planned  |
| `field > value` (range)     | keyword, numeric | `RangeQueryBuilder`      | Planned     |
| `field RLIKE 'pattern'`     | keyword, text    | `RegexpQueryBuilder`     | Planned     |
| `field IN ('a','b','c')`    | keyword, text    | `TermsQueryBuilder`      | Planned     |

Planned predicates will be added incrementally as new `PredicateHandler` implementations. The `PredicateHandlerRegistry` pattern ensures each addition is a single-file change with no modifications to existing components.

---

## Boolean Expression Delegation

For compound RexNode predicates mixing Lucene and DataFusion expressions like `(L1 AND D1) OR (L2 AND D2)`:

- **Option A**: Push entire boolean to Lucene if all leaves are Lucene-compatible. Produces a single `BoolQueryBuilder`, single BitSet. Most efficient when applicable.
- **Option B**: Execute per branch — `Lucene(L1) ∩ DF(D1)` ∪ `Lucene(L2) ∩ DF(D2)`. Correct for all cases including OR-of-ANDs, uses multiple `execute()` calls (supported by cached searcher).

For simple AND (`L1 AND D1`), splitting into separate Lucene/DF groups and intersecting is correct and sufficient. For OR-of-ANDs, per-branch execution (Option B) is needed to preserve the AND correlation between Lucene and DataFusion predicates within each branch. The cached searcher design (acquire once, execute N times) supports this naturally.

---

## Relationship to Core Search Path

The plugin is a thin bridge over core — it reuses, not duplicates:

| Plugin Owns (new code)             | Reuses from Core (no duplication)                      |
|------------------------------------|--------------------------------------------------------|
| RexNode → QueryBuilder conversion  | QueryBuilder hierarchy (TermQB, WildcardQB, etc.)     |
| PredicateHandler registry          | `QueryBuilder.toQuery(QueryShardContext)`              |
| BitSet → Arrow BitVector packaging | `Engine.Searcher` / `IndexSearcher`                    |
| EngineBridge lifecycle             | `QueryShardContext`, `MapperService`, `NamedWriteable` |

### Migration Path

```
Phase 1 (now):  Text predicates (EQUALS, LIKE). Minimal overlap with core.
Phase 2 (next): Range, regex, bool. Each = new PredicateHandler. No structural changes.
Phase 3:        Aggregations. Lucene collectors via EngineBridge. Parallels AggregationPhase.
Phase 4:        Full query execution. Plugin = primary Lucene path for analytics.
                SearchService remains for REST _search backward compat.
```

### Future Evolution Areas

| Area          | Current              | Future                                   |
|---------------|----------------------|------------------------------------------|
| Result format | BitVector only       | Full Arrow RecordBatch with field values  |
| Aggregations  | Not supported        | Collector-based execution                 |
| Scoring       | `COMPLETE_NO_SCORES` | Relevance scores when needed              |
| Caching       | No query cache       | Participate in `IndicesQueryCache`         |

All additive — current design doesn't preclude any of them.

---

## Design Constraints

- **Single-threaded per bridge instance.** `engineSearcher` and `queryShardContext` are mutable fields with no synchronization. One bridge per shard, single-threaded access.
- **`convertFragment` expects `LogicalFilter`.** The planner must wrap Lucene-eligible RexNode predicates in a `LogicalFilter` before calling `convertFragment`.
- **`canHandle` gates `convert`.** Handlers validate field types via Calcite `SqlTypeName` (VARCHAR/CHAR only) and accept `MapperService` (nullable) for future OpenSearch-level validation. Always check `canHandle` before `convert`.
- **Weight is cached per query fragment.** `toQuery` → `rewrite` → `createWeight` runs once per unique `byte[]` fragment. Subsequent `executeForSegment` calls for the same query reuse the cached `Weight`. Cache is cleared on `close()`.
- **Lucene predicates must have literal values known at planning time.** `convertFragment` runs on the coordinator and converts `field = 'literal'` into a serialized `QueryBuilder`. If the comparison value comes from a subquery or join (e.g., `verb = (SELECT ...)` or `logs.verb = users.name`), it's not available at planning time and cannot be delegated to Lucene. Late-binding values would require `convertFragment` to run at execution time on the data node after the dependent value is resolved — a pattern the current design does not support. For now, only predicates with constant literals are Lucene-eligible.
- **`LUCENE_DELEGATED` operators are resolved by the executor, not DataFusion.** The executor walks the plan, executes all `LUCENE_DELEGATED` nodes via `luceneBridge.execute`, and replaces them with BitSet results before passing the plan to DataFusion. DataFusion never sees the custom operator — it only receives a pre-computed BitSet filter alongside its own predicates.

---

## Open Items

1. **DefaultPlanExecutor** — stub. Needs predicate splitting and lifecycle orchestration.
2. **BitSet handoff to DataFusion** — mechanism for passing BitVector for intersection TBD.
3. **Multi-shard coordination** — current design is per-shard.
4. **BoolQueryBuilder** — AND/OR/NOT handler not yet implemented.

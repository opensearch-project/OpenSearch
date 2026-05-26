# [Analytics Engine] M2 — Hash-shuffle joins + theta-join coord-centric fallback

> **Status:** Draft / planning. Branch: `feature/scaffold_mpp_stype_join_m2`.
> Companion to `MPP-JOIN-M1-PR.md` (broadcast) and `HOW-JOIN-WORKS.md` (architectural narrative).

## Summary

This branch ships M2: distributed equi-joins via hash-shuffle, plus restoration of theta (non-equi) joins via the coordinator-centric `NestedLoopJoinExec` path that PR #21639 inadvertently removed. Both are split-rule additions to the planner architecture established by PR #21639 — no changes to the trait def or split-rule contract.

**Two commits, one branch:**

1. **Hash-shuffle equi-join end-to-end** — the bulk of the work: split rule emitting HASH alternatives, RelNode for the shuffle exchange, partition transport wiring, DataFusion-side `RepartitionExec` integration, advisor selection logic, IT.
2. **Theta-join coord-centric fallback** — small: a sibling split rule that emits `COORDINATOR+SINGLETON` alternatives for non-equi joins (without the `JoinInfo.isEqui()` gate). DataFusion's `NestedLoopJoinExec` is already on the runtime side from M0. Removes the `expectThrows` assertion in `BroadcastJoinIT.testThetaJoinFailsAtPlanningTimeUnderPRDesign` and replaces it with a row-multiset parity test.

**Scope decisions made before drafting:**

- Hash-shuffle only — no broadcast-fallback for theta joins (rare; cartesian shape; broadcast wouldn't help).
- DataFusion-native partitioning via `RepartitionExec(Hash, N)` — no Java-side row hashing.
- Advisor-driven strategy selection (mirrors M1 `JoinStrategyAdvisor`) — not pure CBO. Reuses `analytics.mpp.enabled`, `JoinStrategyMetrics`, and IT counter assertions.
- Partition count: cluster setting `analytics.mpp.shuffle_partitions` with engine-provided default. The DataFusion backend defaults to **probe-side data-node count**; future Lucene backend can default to 1 (no shuffle). Mirrors RFC 4812's `mpp_parallelism` × `defaultEngineParallelism()` design.
- Theta locale: coord-centric only. M2 doesn't broadcast-then-nested-loop (that's a future optimization that would require teaching `JoinStrategyAdvisor` and `BroadcastInjectionHandler` to handle non-equi predicates).

## Existing scaffolding (do not duplicate)

PR #21639 + M0 + M1 already left these in the tree, gated behind "M2 follow-up" comments:

| File | Status | M2 work |
|---|---|---|
| `planner/rel/OpenSearchShuffleExchange.java` | Scaffolded (`SingleRel`, hashKeys + partitionCount + viableBackends) | Wire into trait conversion path; cost gate |
| `spi/ShuffleProducerInstructionNode.java` | Scaffolded (hashKeyChannels, partitionCount, targetWorkerNodeIds, side tag) | Used by producer fragments after hash-shuffle rewrite |
| `spi/ShuffleScanInstructionNode.java` | Scaffolded (namedInputId, partitionIndex, expectedSenders) | Used by consumer fragments to register the shuffled input |
| `exec/action/AnalyticsShuffleDataAction.java` (+ Request/Response/Transport) | Scaffolded transport types | Wire from producer handler to ShuffleBufferManager |
| `exec/shuffle/ShuffleBufferManager.java` | Scaffolded — keyed by `(queryId, stageId, partitionIndex)` | Drive consumer-side completion; expose Arrow stream to the engine |
| `exec/shuffle/ShuffleSenderRetry.java` | Scaffolded — exponential backoff | Caller for the producer side |
| `be/datafusion/ShuffleProducerHandler.java` | Stub | Wrap fragment output with `RepartitionExec(Hash, N)`; one stream per partition; send via `AnalyticsShuffleDataAction` |
| `be/datafusion/ShuffleScanHandler.java` | Stub | Register a per-partition consumer stream as a `NamedScan` on the worker session |

What's **not** scaffolded and needs to be added in M2:

- `OpenSearchHashJoinSplitRule` — the split rule emitting HASH alternatives (deliberately not present; the old `OpenSearchHashJoinRule` was removed during the PR #21639 rebase because it didn't fit the split-rule architecture).
- `ExchangeInfo.partitionCount` — reverted to 2-field shape during the rebase; restore as the third field (M0's M1 had it).
- Cluster setting `analytics.mpp.shuffle_partitions` and the engine SPI hook `defaultShuffleParallelism()`.
- Coord-centric fallback split rule for theta joins.

## Commit 1 — Hash-shuffle equi-join end-to-end

### Planner layer

#### `OpenSearchHashJoinSplitRule` (new)

Sibling of `OpenSearchJoinSplitRule`, fires on the same `OpenSearchJoin` operand. Emits a third alternative:

- Both inputs demand `HASH+localized(joinKeys, partitionCount)` instead of `COORDINATOR+SINGLETON`.
- Volcano's trait converter (`OpenSearchDistributionTraitDef.convert(...)`) inserts a per-side `OpenSearchShuffleExchange(hashKeys, partitionCount)` to satisfy the demand.
- `OpenSearchJoin.computeSelfCost` cost-gates the alternative: legal only when both inputs deliver matching HASH locality on the same key set + partition count.

Critical: the rule must be **stable under repeated firing**. The earlier `OpenSearchHashJoinRule` (M0) caused a Volcano memo explosion because trait-conversion inserted ERs that re-fired the rule on the wrapper. The split-rule design avoids this — rules fire once on the marked `OpenSearchJoin` and `convert(...)` insertion is opaque to the rule.

```java
// Sketch — full implementation will follow OpenSearchJoinSplitRule's structure
public class OpenSearchHashJoinSplitRule extends RelOptRule {
    onMatch(call) {
        OpenSearchJoin join = call.rel(0);
        if (!join.analyzeCondition().isEqui()) return;  // hash needs equi
        if (!mppEnabled(call)) return;                   // gated by master MPP kill switch

        int partitionCount = resolvePartitionCount(call);  // from settings + engine default
        List<Integer> leftKeys  = join.analyzeCondition().leftKeys;
        List<Integer> rightKeys = join.analyzeCondition().rightKeys;

        OpenSearchDistribution leftHash  = distTraitDef.hash(leftKeys,  partitionCount);
        OpenSearchDistribution rightHash = distTraitDef.hash(rightKeys, partitionCount);

        RelNode hashJoin = join.copy(
            join.getTraitSet().replace(distTraitDef.executionLocalized()),  // post-shuffle, locale=worker
            convert(join.getLeft(),  leftHash),
            convert(join.getRight(), rightHash),
            join.getCondition(), join.getJoinType()
        );
        // Coord must still gather shuffled-and-joined output
        call.transformTo(convert(hashJoin, distTraitDef.coordSingleton()));
    }
}
```

#### `OpenSearchDistribution.hash(...)` and `executionLocalized()` (extend trait def)

Currently `OpenSearchDistributionTraitDef` defines `coordSingleton()`, `shardSingleton()`, `executionShardLocalized()`. Add:

- `hash(List<Integer> keys, int partitionCount)` → `HASH+EXECUTION` distribution.
- `executionLocalized()` → "runs on a worker, no specific shard" — used as the post-hash-join locale before final gather.
- Extend `Locality` enum with `WORKER` (the post-shuffle execution locale).

The trait's `satisfies(...)` rules:

| current trait | required trait | satisfies? |
|---|---|---|
| HASH(keys, N) | HASH(keys, N) | yes |
| HASH(keys, N) | COORDINATOR+SINGLETON | no (needs gather) |
| HASH(k1, N) | HASH(k1, M) where M ≠ N | no |
| HASH(k1+k2, N) | HASH(k1, N) | yes (finer satisfies coarser) |
| SHARD+SINGLETON | HASH(keys, N) | no (needs shuffle) |

The "finer satisfies coarser" rule (last row) matters for downstream operators that only need partial-key co-location.

#### `ExchangeInfo.partitionCount` (restore)

Currently `ExchangeInfo` is `(distributionType, partitionKeyIndices)`. M0 had `partitionCount` as a third field; the rebase reverted to 2-field for shape-test compat. Restore it. ~11 `*PlanShapeTests` will need their toString fixtures updated; this is mechanical.

`OpenSearchShuffleExchange` carries this through to its `getExchangeInfo()` so `DAGBuilder.cutAtExchange` can read it.

#### `OpenSearchJoin.computeSelfCost` (extend cost gate)

Currently returns `makeInfiniteCost()` for non-SINGLETON inputs. Extend so HASH inputs are legal **iff** both inputs report HASH on the same keys + same partition count, else infinite. This is what makes Volcano accept the new alternative without combinatorially exploring incompatible HASH+SINGLETON pairs.

### DAG construction

`DAGBuilder` already cuts at `OpenSearchExchangeReducer` via `cutAtExchange`. Generalize to also cut at `OpenSearchShuffleExchange`. The cut node's `ExchangeInfo` (now with `partitionCount`) is attached to the resulting child stage so downstream knows the partition shape.

Each shuffle cut produces a child stage tagged `Stage.StageRole.SHUFFLE_PRODUCER`. The parent stage carries `SHUFFLE_CONSUMER`. The `Stage.StageRole` enum already has `BROADCAST_BUILD` / `BROADCAST_PROBE` from M1; add the two SHUFFLE_* values alongside.

### Strategy selection (advisor-driven)

#### `JoinStrategy.HASH_SHUFFLE` (existing enum value, currently falls through)

Today's `DefaultPlanExecutor` returns `COORDINATOR_CENTRIC` when the advisor picks HASH_SHUFFLE. M2 makes it dispatch through a new `HashShuffleDispatch` (sibling of `BroadcastDispatch`).

#### `JoinStrategyAdvisor.adviseAndTag` (extend)

Pre-merge logic:

```
if shard_gate AND row_gate AND join_type_permits:
    pick BROADCAST
else:
    fall through to COORDINATOR_CENTRIC
```

M2 logic:

```
if shard_gate AND row_gate AND join_type_permits:
    pick BROADCAST
else if equi_join AND both_sides_above_broadcast_row_threshold:
    pick HASH_SHUFFLE
    tag stages SHUFFLE_PRODUCER (left), SHUFFLE_PRODUCER (right), SHUFFLE_CONSUMER (root child)
else:
    COORDINATOR_CENTRIC
```

Gating logic for the new branch:

- Both sides' `rowCount > broadcast_max_rows` — i.e. broadcast was rejected.
- `JoinInfo.isEqui()` — required.
- Outer-join correctness: hash-shuffle works for INNER, LEFT, RIGHT, FULL natively (DataFusion `HashJoinExec` handles all four). SEMI / ANTI: also fine.

If row counts are unknown (`StatisticsCollector` returned 0), refuse fail-safe → COORDINATOR_CENTRIC.

There is no separate `shuffle_enabled` kill switch — `mpp.enabled` is the single MPP master gate. With `mpp.enabled=true` for an equi-join, the advisor picks BROADCAST when broadcast-eligible, otherwise HASH_SHUFFLE. With `mpp.enabled=false`, every join routes coord-centric.

### Partition count resolution

Resolution order (highest priority first), implemented as a single helper in `JoinStrategyAdvisor`:

1. **Per-query setting** — if a future query-level override is added (deferred). Not in M2.
2. **Cluster setting** `analytics.mpp.shuffle_partitions` — if explicitly set by an operator, use it.
3. **Engine default** — call `backend.defaultShuffleParallelism(clusterState)` on the join's chosen backend. The DataFusion backend returns the count of data nodes that hold any shard of the probe-side index (inclusive of replica selection — same set the scheduler would target). Lucene backend returns 1.

This puts the right knob in three places: deployment-wide (cluster setting), per-engine (SPI default), and per-query (future).

#### `AnalyticsSearchBackendPlugin.defaultShuffleParallelism(ClusterState)` (new SPI method)

Default implementation: 1 (i.e. opt-out). Backends that support shuffle override.

### DAG rewrite

Sibling of `BroadcastDAGRewriter`. Given an advisor-tagged DAG with a HASH_SHUFFLE join, produce a fresh DAG of shape:

```
root Reducer (gather)
  └── join Stage (SHUFFLE_CONSUMER)
        ├── consumer Join(NamedScan("shuffle-<leftStageId>-p"), NamedScan("shuffle-<rightStageId>-p"))
        ├── leftProducer Stage (SHUFFLE_PRODUCER, side="left", hashKeys=joinLeftKeys)
        │     └── scan(left side)
        └── rightProducer Stage (SHUFFLE_PRODUCER, side="right", hashKeys=joinRightKeys)
              └── scan(right side)
```

Each producer stage's plan alternative gets a `ShuffleProducerInstructionNode` appended (similar to how `BroadcastInjectionInstructionNode` is appended for M1). Each consumer stage gets one `ShuffleScanInstructionNode` per side (so two instructions appended per consumer fragment).

Consumer stage runs once per partition — N consumer tasks, one per data node.

### Dispatch orchestration

#### `HashShuffleDispatch` (new — sibling of `BroadcastDispatch`)

Three-pass conceptually, but only **one explicit handoff** because producers and consumers run concurrently (unlike M1 where build must complete before probe starts).

```
1. Resolve partitionCount, allocate ShuffleBufferManager keyed by (queryId, stageId)
   for the consumer stage. Buffers expect (partitionCount × numProducerSides) sender
   completion signals before declaring each partition complete.

2. Dispatch all three stages (left producer, right producer, consumer) to QueryScheduler.execute
   simultaneously. The walker honors the DAG dependency order:
   - Producer stages run on data nodes that hold their respective input shards.
   - Consumer tasks run on the partition-owning nodes (resolved by the shuffle-target
     resolver), each one waiting on its ShuffleBuffer for both 'left' and 'right' partitions
     to complete before HashJoinExec executes.

3. Coord ExchangeReducer gathers joined rows from N consumer partitions.
```

The "concurrent dispatch" is what differentiates this from `BroadcastDispatch.run`'s pass-1-then-pass-2. The scheduler's existing dependency graph (each consumer leaf depends on the matching producer leaves transitively) makes this work without new orchestration logic — we just reuse `QueryScheduler.execute` on the rewritten DAG.

Cancellation: if any producer fails, the walker cancels all stages (existing behavior). Consumer's `ShuffleBuffer` notices missing senders via a separate timeout (`analytics.mpp.shuffle_recv_timeout`, default 60s) and fails the partition.

### DataFusion backend

#### `ShuffleProducerHandler.apply(...)` (fill in stub)

Driven by `ShuffleProducerInstructionNode`. Steps:

1. Take the existing prepared plan from `commonContext` (the producer fragment's already-converted Substrait plan).
2. Wrap its top operator with `RepartitionExec(Hash, partitionCount)` using the hash key channels from the instruction. DataFusion exposes `RepartitionExec::try_new(input, Partitioning::Hash(exprs, n))`.
3. Open one output stream per partition. For each output partition `i`, pump batches via `AnalyticsShuffleDataAction` to `targetWorkerNodeIds[i]`, with metadata `(queryId, targetStageId, partitionIndex=i, side)`.
4. On stream end, send a final `isLast=true` request so the consumer's `ShuffleBuffer` knows the producer is done.

Use `ShuffleSenderRetry` for transient transport failures (already scaffolded with exponential backoff).

#### `ShuffleScanHandler.apply(...)` (fill in stub)

Driven by `ShuffleScanInstructionNode`. Steps:

1. Call `ShuffleBufferManager.openConsumerStream(queryId, stageId, partitionIndex, side)` — returns a `Stream<RecordBatch>` that becomes ready when all `expectedSenders` have signaled `isLast`.
2. Register that stream as a `NamedScan(namedInputId)` on the worker's `SessionContextHandle` via a new FFM entry point `df_register_partition_stream_on_session_context`.
3. The consumer fragment's Substrait plan references both `NamedScan("shuffle-<leftStageId>-<i>")` and `NamedScan("shuffle-<rightStageId>-<i>")`, which DataFusion resolves to the registered streams. `HashJoinExec` runs on the resolved input.

#### Native FFM addition

New Rust entry point in `rust/src/api.rs`:

```rust
pub unsafe fn register_partition_stream_on_session_context(
    handle_ptr: i64,
    input_id: &str,
    schema_ipc: &[u8],
    partition_stream_ptr: i64,  // opaque token, looked up in a partition-stream registry
) -> Result<(), DataFusionError>
```

Mirrors the M1 pattern (`register_memtable_on_session_context`) — same `SessionContextHandle` type, same `align_buffers()` requirement on imports. The difference is the input is a *streaming* table, not a static memtable: `StreamingTable::try_new` instead of `MemTable::try_new`. Schema arrives as IPC bytes; rows arrive on the channel.

#### `ExchangeSinkProvider.createShuffleProducerSink(...)` (extend SPI)

Default UOE; DataFusion overrides. Returns an `ExchangeSink` that buffers per-partition Arrow batches and ships via the producer handler.

### Settings & metrics

New cluster settings (extend `AnalyticsSettings`):

| Setting | Type | Default | Dynamic | Description |
|---|---|---|---|---|
| `analytics.mpp.shuffle_partitions` | int | (engine default) | yes | Number of hash-shuffle partitions. When unset, the backend's `defaultShuffleParallelism` decides. |
| `analytics.mpp.shuffle_recv_timeout` | TimeValue | 60s | yes | Per-partition receive timeout on consumer side. |

Extend `JoinStrategyMetrics` with a `HASH_SHUFFLE` counter; expose at the existing `GET /_analytics/_strategies` endpoint.

### Tests

#### Unit
- `OpenSearchHashJoinSplitRuleTests` — emits HASH alternative when both inputs support it; falls through when not equi; respects cost gate.
- `OpenSearchDistributionTraitDefHashTests` — satisfies / converts for HASH ↔ HASH, HASH → SINGLETON, SHARD → HASH.
- `JoinStrategyAdvisorTests` — extend with `testHashShuffleSelectedWhenBroadcastIneligible`.
- `JoinStrategySelectorTests` — new branch for HASH_SHUFFLE eligibility (equi + both-sides-large).
- `HashShuffleDAGRewriterTests` — rewritten DAG shape: 3 stages (2 producers + 1 consumer); consumer plan has `Join(NamedScan, NamedScan)`.
- `HashShuffleDispatchTests` — concurrent dispatch (no pass-1/pass-2 sequencing); cancellation cascade; consumer waits for both sides.
- `ShuffleProducerHandlerTests` — `RepartitionExec` insertion, per-partition stream open, retry on transient failure.
- `ShuffleScanHandlerTests` — `NamedScan` registration, partition stream readiness.
- `ExchangeInfoSerializationTests` — partitionCount round-trip.

#### Planner shape tests
- Update ~11 fixtures broken by `ExchangeInfo.partitionCount` restoration. Mechanical: regenerate `toString`.

#### Integration
- `HashShuffleJoinIT` — sibling of `BroadcastJoinIT`, 2-node cluster, both indices ≥ broadcast_max_rows so HASH_SHUFFLE fires:
  - `testInnerEquiJoin_largeFact_largeDim_hashShuffleMatchesBaseline`
  - `testLeftOuterJoin_largeFact_largeDim_hashShuffleMatchesBaseline`
  - `testMppDisabled_keepsAllMppCountersAtZero` (mpp.enabled=false routes coord-centric)
  - `testHashShufflePartitionCountSetting` (override → assert partition count from `_analytics/_strategies` or task explain)
- Counter assertion: `HASH_SHUFFLE` counter advances when MPP on for large×large; `0` when MPP off.

## Commit 2 — Theta-join coord-centric fallback

### Planner layer

#### `OpenSearchThetaJoinSplitRule` (new)

Sibling split rule, fires on `OpenSearchJoin` operand **without** the `JoinInfo.isEqui()` guard. Emits a single COORDINATOR+SINGLETON alternative — same shape `OpenSearchJoinSplitRule` produces today for non-co-located equi-joins.

The rule's discriminator: only fires when `!joinInfo.isEqui()`. Equi-joins continue through `OpenSearchJoinSplitRule` as today.

```java
// Sketch
public class OpenSearchThetaJoinSplitRule extends RelOptRule {
    onMatch(call) {
        OpenSearchJoin join = call.rel(0);
        if (join.analyzeCondition().isEqui()) return;  // equi handled elsewhere
        // Demand SINGLETON on each input — Volcano inserts an ER per side.
        // The join itself is COORDINATOR-localized; NestedLoopJoinExec runs there.
        OpenSearchDistribution coord = distTraitDef.coordSingleton();
        RelNode coordJoin = join.copy(
            join.getTraitSet().replace(coord),
            convert(join.getLeft(),  coord),
            convert(join.getRight(), coord),
            join.getCondition(), join.getJoinType()
        );
        call.transformTo(coordJoin);
    }
}
```

The cost gate (`OpenSearchJoin.computeSelfCost`) already accepts SINGLETON+SINGLETON inputs — no changes needed there.

#### Remove `JoinInfo.isEqui()` rejection in `OpenSearchJoinRule`

Today's HEP marker rule rejects non-equi joins, leaving a raw `LogicalJoin` that crashes Volcano's trait converter. M2 removes this rejection — the marker just produces an `OpenSearchJoin` regardless of equi/non-equi. Discrimination then happens at the split rule level (equi vs theta).

### DataFusion backend

No changes. `NestedLoopJoinExec` is part of DataFusion's standard physical planning — when isthmus-converted Substrait reaches the engine with a non-equi join condition, DataFusion picks `NestedLoopJoinExec` automatically.

The M0 path was working before PR #21639. The fix is purely planner-side: stop rejecting at marker rule, add the theta split rule.

### Tests

#### Unit
- `OpenSearchThetaJoinSplitRuleTests` — emits COORDINATOR alternative for non-equi; doesn't fire on equi.
- `OpenSearchJoinRuleTests` — extend: now accepts non-equi joins (was rejecting).

#### Integration

Replace the `expectThrows` assertion in `BroadcastJoinIT.testThetaJoinFailsAtPlanningTimeUnderPRDesign` with row-multiset parity. Rename the test to `testThetaJoinRoutesToCoordinatorCentric`. New assertions:

- Rows match a baseline computed in-test (cartesian product filtered by predicate).
- `BROADCAST` counter does NOT advance.
- `COORDINATOR_CENTRIC` counter advances.
- A larger sanity test with `F.id < D.id` confirms PR #21639's earlier-removed M0 behavior is back.

## Out of scope for M2

- **Co-routing (SHARD-local) hash join** — the case where both Parquet tables are already hash-bucketed on the join key. M3 work; requires `SOURCE_ROUTING_HASH` distribution, Parquet bucketing metadata, and a separate split-rule alternative emitting the shard-local fast-path.
- **Skew handling** — partition-count is not the right knob for skew; M3+ work via salting / broadcast-the-skewed-side / AQE-style adaptive splitting.
- **Broadcast-then-nested-loop for theta joins with one small side** — possible but rare workload and architecturally orthogonal. Defer.
- **Hash-shuffle for aggregates** — analytics-engine already does partial/final agg split for COORDINATOR locality. Extending to HASH-localized aggregates is a separate "MppPhysicalAggregateRule" follow-up modeled after RFC 4812's design.
- **Per-query parallelism override** — defer to a future PR; the cluster setting + engine default is enough for M2.

## Why not adopt datafusion-ballista?

Ballista is the closest existing project to "distributed DataFusion." We considered it as an alternative to building our own M2 hash-shuffle path. **Conclusion: wrong shape for our project.** Recording the reasoning here so this isn't relitigated later.

The architectural mismatch isn't about features — Ballista has most of the features we want (hash shuffle, broadcast joins, retry, AQE-style coalesce). It's that **Ballista is an alternative deployment model, not a library**:

| Concern | Ballista | Our setup |
|---|---|---|
| Process model | Standalone scheduler + executor binaries (or embedded Tokio runtime on a localhost gRPC port) | OpenSearch JVMs that already exist; `TransportService` + `TaskManager` + SEARCH threadpool already running |
| Wire protocol | gRPC (control) + Arrow Flight (shuffle reads) | OpenSearch `TransportService` (already deployed cluster-wide; certs / firewalls / monitoring already configured) |
| Shuffle storage | Default: Arrow IPC files written to local disk in `work_dir`, pulled via Flight RPC (`shuffle_writer.rs:69-146`, `shuffle_reader.rs:96-150`) | In-memory `ShuffleBufferManager` keyed by `(queryId, stageId, partitionIndex)`; transport-streamed via `AnalyticsShuffleDataAction` |
| Memory | Per-executor `MemoryPool` + `mimalloc`. No unified pressure signal across cluster | Per-query Arrow `ArrowAllocatorService` child of cluster-wide root; integrates with OpenSearch circuit breakers |
| Task model | Scheduler launches via gRPC `LaunchTask`; executors poll via `PollWork` with backoff | `AnalyticsQueryTask` registered with `TaskManager`; visible in `_tasks` API; respects `cancel_after_time_interval` |
| Plan input | DataFusion physical plan in, distributed-stage protobuf out (`DistributedPlanner::plan_query_stages`, `planner.rs:98-270`) | Calcite logical plan → marked → Volcano → DAGBuilder cuts at exchanges → per-stage Substrait via isthmus |

Adopting Ballista would mean running a **second control plane** (its scheduler), **second thread pool** (its Tokio runtime), **second memory allocator** (mimalloc), and **second task manager** inside every OpenSearch JVM, alongside the ones OpenSearch already has. Cancellation, timeouts, observability, circuit breakers, auth — all would either break or need rebuilding to match the existing OpenSearch contracts.

**The "planner-only" reuse is also weaker than it sounds.** Ballista's `DistributedPlanner` is a 170-line walk over a *DataFusion physical plan* tree, looking for `RepartitionExec` / `CoalescePartitionsExec` and wrapping pipeline-breakers with `ShuffleWriterExec`. We never produce a DataFusion physical plan in the planner — that's the data node's job after isthmus conversion. To "use the planner" we'd have to:

1. Convert our Calcite plan into a DataFusion physical plan (we don't, by design).
2. Run Ballista's planner on it.
3. Map its `ShuffleWriterExec` / `UnresolvedShuffleExec` outputs back into our `Stage` / `StagePlan` / `InstructionNode` model.

That's strictly more work than the ~200 LOC `OpenSearchHashJoinSplitRule` planned for M2 commit 2 — which does the job at the right layer (Calcite Volcano), in the language our planner already speaks.

## Concepts to steal from Ballista (M3+ enhancement candidates)

That said, **Ballista has solved several adjacent problems well**, and once M2's basic functionality is in place these are the ideas worth bringing over. None of them require adopting Ballista's runtime — they're algorithmic / heuristic ideas portable to our architecture.

### M3.1 — Adaptive shuffle-partition coalescing (AQE-style)

**Source:** Ballista's `BALLISTA_COALESCE_SHUFFLE_PARTITIONS` rule (`config.rs:89-102`). Same idea Spark calls Adaptive Query Execution.

**Problem:** Our M2 picks partition count at plan time (= probe-side data-node count). Real shuffle output is often skewed — one partition gets 10× the data of average. The query is bottlenecked on that one partition.

**Steal:** After the shuffle writer reports actual per-partition byte sizes, post-coalesce small adjacent partitions into a single consumer task. Reduces tail latency.

**Where it plugs in:** Between producer-stage SUCCEEDED and consumer-stage start. Requires per-partition size reporting from the producer (our `ShuffleProducerHandler` already knows this; we just need to surface it). Then a coordinator-side coalesce pass before consumer dispatch.

**Cost:** Modest — adds one round of stats reporting + one rewrite pass.

### M3.2 — Sort-based shuffle with batch consolidation

**Source:** Ballista's `SortShuffleWriterExec` (default-on, `BALLISTA_SHUFFLE_SORT_BASED_ENABLED`, 256 MB/task limit). Consolidates many small per-partition outputs into fewer sorted files with an index.

**Problem:** Hash-shuffle naturally fragments — N output partitions × M producer tasks = N×M small streams. Each stream has transport overhead. With N=10 and M=20, 200 streams for one join.

**Steal:** Even though we shuffle in-memory (not to disk), the **batch-consolidation** idea is portable: producer accumulates multiple Arrow batches per partition before sending one transport request. Reduces wire round-trips dramatically.

**Where it plugs in:** Inside `ShuffleProducerHandler.apply(...)` — instead of one `AnalyticsShuffleDataAction` request per `RecordBatch`, accumulate up to a configurable byte threshold (e.g. 4 MB) before flushing.

**Cost:** Small — a batch-buffer per output partition. Tradeoff is added latency for first batch (mitigatable with a max-time threshold).

### M3.3 — Stage-level retry

**Source:** Ballista's stage state machine (`UnResolved → Resolved → Successful | Failed`, `query_stage_scheduler.rs:220-250`). On stage failure, cancel and retry the stage rather than failing the whole query.

**Problem:** Today our orchestrator (`PlanWalker` + `QueryScheduler`) fails the entire query on first stage failure. For long-running joins this is a steep cost — a transient transport blip on one shuffle producer kills a query that's 80% done.

**Steal:** Per-stage retry budget (e.g. 2 retries with exponential backoff) before giving up. Builds on the existing `Stage.StageRole` tagging — only retry-safe stages (idempotent producers, deterministic consumers) qualify.

**Where it plugs in:** `PlanWalker.start()` failure path. Currently calls `cancelAll` and propagates failure to the terminal listener. Add a "this stage is retryable" check, increment retry count, re-dispatch the stage, propagate failure only when retries exhausted.

**Cost:** Moderate — needs a retry registry per query, careful interaction with cancellation (a retried stage shouldn't be cancelled by the previous attempt's stale callback).

### M3.4 — Byte-based broadcast threshold

**Source:** Ballista's `BALLISTA_BROADCAST_JOIN_THRESHOLD_BYTES` (`config.rs:86`). Threshold gate is in bytes, not rows.

**Problem:** Our M1 advisor uses `BROADCAST_MAX_ROWS` (default 1M rows). A 100K-row dim with 10 KB strings per row (1 GB) currently passes the row gate. A 1M-row dim with int-only columns (8 MB) also passes. They're treated identically; the first one will OOM the probe-side memtable.

**Note:** We already have `BROADCAST_MAX_BYTES` (default 32 MB) as a *runtime* cap at the capture sink. It catches the 1 GB case — but only after running the full build stage. The cost has already been paid.

**Steal:** Add a *pre-flight* byte-size gate to `JoinStrategyAdvisor`. Compute estimated byte size from `IndicesStats` `store.size_in_bytes` × selectivity estimate (or just primaries' total store size, conservative). Refuse broadcast when estimated bytes > `BROADCAST_MAX_BYTES_PREFLIGHT` (default 32 MB), pushing the query directly to HASH_SHUFFLE without running the wasted build.

**Where it plugs in:** `StatisticsCollector` already fetches `IndicesStats`; extend to also pull `primaries.store.size_in_bytes`. `JoinStrategySelector.fitsBroadcastGates()` adds a third gate.

**Cost:** Small — one extra IndicesStats field, one extra gate. Backwards-compatible (treat unknown as fail-safe = refuse broadcast, same convention as `rowCount=0`).

### M3.5 — Disk-spill for shuffle buffers

**Source:** Ballista's sort-shuffle spill (referenced in `execution_engine.rs:26`).

**Problem:** Our M2 ships in-memory shuffle (each `ShuffleBufferManager` partition holds Arrow batches in heap until consumer reads them). Large joins on a node with hot shards can OOM the buffer manager before the consumer drains it.

**Steal:** When a `ShuffleBuffer` exceeds a configurable byte threshold (`analytics.mpp.shuffle_buffer_max_bytes`, default 256 MB), spill oldest batches to disk (Arrow IPC files in node's data directory). Consumer transparently reads spilled batches via the same iterator.

**Where it plugs in:** `ShuffleBufferManager` — internal change. Consumer-facing iterator stays the same.

**Cost:** Moderate — spill encoder/decoder + cleanup on query terminal. Disk space accounting belongs in OpenSearch's existing breaker hierarchy.

### Summary table — when to land each

| Item | Relative size | When | Trigger |
|---|---|---|---|
| M3.1 — AQE coalesce | Medium | Post-M2 | First production skew complaint |
| M3.2 — Batch consolidation | Small | M2.x (could land in M2 if simple) | Profile shows transport overhead |
| M3.3 — Stage retry | Medium-Large | M3 | First production query lost to transient failure |
| M3.4 — Byte broadcast threshold | Small | M2.x | Soon — addresses a real M1 gap |
| M3.5 — Disk spill | Medium | M3 | First OOM in `ShuffleBufferManager` |

M3.4 (byte-based broadcast threshold) is the lowest-cost / highest-payoff item; could even land in M2 if scoped tightly. The rest are post-MVP.

## End-to-end flow (post-M2)

```
Calcite logical Join
  │
  ▼  OpenSearchJoinRule (HEP; mark only — accepts equi AND non-equi now)
OpenSearchJoin   (trait: COORDINATOR+SINGLETON)
  │
  ▼  Volcano CBO: three split rules can fire
     ├── OpenSearchJoinSplitRule:        equi → COORDINATOR or SHARD-co-location
     ├── OpenSearchHashJoinSplitRule:    equi → HASH(joinKeys, N)               [M2 NEW]
     └── OpenSearchThetaJoinSplitRule:   non-equi → COORDINATOR                 [M2 NEW]
  │
  ▼  DAGBuilder cuts at OpenSearchExchangeReducer AND OpenSearchShuffleExchange
QueryDAG with per-stage fragments
  │
  ▼  JoinStrategyAdvisor.adviseAndTag(dag)
     ├── BROADCAST eligible      → BROADCAST                                    [M1, unchanged]
     ├── HASH_SHUFFLE eligible   → HASH_SHUFFLE  (tags SHUFFLE_PRODUCER/CONSUMER) [M2 NEW]
     └── else                    → COORDINATOR_CENTRIC                          [M0/theta]
  │
  ▼  DefaultPlanExecutor dispatch
     ├── COORDINATOR_CENTRIC  → QueryScheduler.execute(dag)                     [M0]
     ├── BROADCAST            → BroadcastDispatch (pass 1: build → probe)        [M1]
     └── HASH_SHUFFLE         → HashShuffleDispatch                              [M2 NEW]
                                rewrite DAG → 3-stage shape (2 producers + 1 consumer)
                                concurrent dispatch via QueryScheduler.execute
                                producer fragments wrap RepartitionExec(Hash, N) on top
                                consumer fragments register two ShuffleScans as NamedScans
                                DataFusion runs HashJoinExec on the worker
                                joined rows stream to coord ExchangeReducer
```

## Test plan (when implementation ships)

- [ ] `:sandbox:plugins:analytics-engine:test` — full unit suite green
- [ ] `:sandbox:plugins:analytics-backend-datafusion:test` — full backend suite green
- [ ] `:sandbox:libs:analytics-framework:test` — SPI tests green
- [ ] `:sandbox:qa:analytics-engine-rest:integTest --tests "*HashShuffleJoinIT"` — IT green
- [ ] `:sandbox:qa:analytics-engine-rest:integTest --tests "*BroadcastJoinIT"` — M1 IT still green (regression)
- [ ] `:sandbox:qa:analytics-engine-coordinator:internalClusterTest` — coordinator IT suite still green

## Sequencing

The two commits land together (one PR) but can be developed sequentially:

1. **First** — theta-join restoration (small, ~200 LOC). This unblocks the regression IT and validates the split-rule architecture works for a second non-broadcast strategy. Changes:
   - 1 new split rule
   - `OpenSearchJoinRule.matches()` accepts non-equi
   - Update `BroadcastJoinIT.testThetaJoinFailsAtPlanningTimeUnderPRDesign` → `testThetaJoinRoutesToCoordinatorCentric`

2. **Second** — hash-shuffle (~2000+ LOC). The bulk of M2. Order within:
   1. Settings + SPI extension (`defaultShuffleParallelism`, cluster settings).
   2. Planner: trait extensions, split rule, restored `ExchangeInfo.partitionCount`, cost gate.
   3. DAG / strategy: rewriter, advisor, dispatch.
   4. Backend: producer + consumer handlers, Rust FFM entry point.
   5. IT: end-to-end on 2-node parquet cluster.

Commit 1 is the "small follow-up" we said we'd bundle; it stays small. Commit 2 is the bulk and warrants a chain of internal commits within the branch (likely 4–6 logical commits) before squash on the PR.

## Open questions for review

1. **Per-stage shuffle parallelism override.** Should `analytics.mpp.shuffle_partitions` be a single setting or per-fragment (e.g., a join hint)? The latter is more flexible but defers the design of a hint mechanism. M2 ships single-setting; revisit if production data shows a need.
2. **Streaming-vs-buffered shuffle.** RFC 4812 uses streaming (each batch flows through). Spark uses spillable buffers per partition. M2 ships streaming via `AnalyticsShuffleDataAction` (one transport request per batch); the consumer's `ShuffleBuffer` accumulates in memory. If memory pressure becomes an issue, add disk spill in M3.
3. **Shuffle-recv timeout interaction with task cancel.** A consumer waiting on a partition that never completes should fail fast. The 60s default is a backstop; cancellation via `AnalyticsQueryTask.setOnCancelCallback` is the primary path. Need to verify the buffer wakes up on cancel.
4. **Schema mismatch between producer batches.** DataFusion's `RepartitionExec` preserves input schema across all output partitions, but if the producer's prepared plan has a `Projection` upstream that the partitioning expressions reference by *name*, isthmus's column-index resolution needs to be schema-stable. M0 / M1 didn't exercise this. Likely a non-issue but warrants a planner-shape test with a join-on-projected-column case.

## References

- `MPP-JOIN-M0-PR.md` — coord-centric baseline
- `MPP-JOIN-M1-PR.md` — broadcast (the architectural template for M2's shape)
- `HOW-JOIN-WORKS.md` — narrative reference; will gain an updated "Stage 7" section once M2 lands
- `analytics-comparison/65-mpp-joins-design.md` — original forward-looking design
- RFC 4812 (`/workplace/ltjin/vec/RFC_4812/`) — partition-count + parallelism design we're mirroring
  - `07-physical-optimizer.md` — `MppPhysicalAggregateRule` shape (mirror for join)
  - `13-task-scheduling.md` — Stage→Task fan-out
  - `15-data-exchange.md` — exchange semantics
  - `20-settings-and-configuration.md` — `mpp_parallelism` setting
  - `23-distribution-and-partition.md` — distribution trait + partition info
- datafusion-ballista (`/workplace/ltjin/mustang/datafusion-ballista/`) — surveyed as an alternative; rejected as runtime, but several heuristics worth borrowing (see "Concepts to steal" section above)
  - `ballista/scheduler/src/planner.rs:98-270` — `DistributedPlanner::plan_query_stages`
  - `ballista/core/src/config.rs:85-102` — broadcast threshold, AQE coalesce knobs
  - `ballista/core/src/execution_plans/shuffle_writer.rs` / `shuffle_reader.rs` — shuffle wire path
  - `ballista/core/src/execution_plans/sort_shuffle.rs` — sort-based shuffle with spill

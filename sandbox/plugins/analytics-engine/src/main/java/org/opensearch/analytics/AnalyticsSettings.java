/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Locale;

/**
 * Engine-level cluster settings for analytics-engine. Backend-specific settings live
 * alongside their backend (e.g. {@code DatafusionSettings}); this class is for settings
 * that gate engine-side behavior independent of the chosen backend.
 *
 * @opensearch.internal
 */
public final class AnalyticsSettings {

    private AnalyticsSettings() {}

    /**
     * Master switch for MPP (multi-pass parallel) join/aggregate dispatch.
     *
     * <p>Defaults to {@code false}: production runs the coordinator-centric (M0) path until an
     * operator opts into MPP. When {@code true}, the engine routes joins/aggregates through the
     * strategy advisor and may pick BROADCAST or HASH_SHUFFLE for eligible queries; when
     * {@code false}, everything falls back to coordinator-centric regardless of advisor decision —
     * which also makes this an incident-response kill switch and the A/B baseline. QA test clusters
     * enable it explicitly (see {@code qa/analytics-engine-rest/build.gradle}) so the MPP path is
     * exercised in CI.
     */
    public static final Setting<Boolean> MPP_ENABLED = Setting.boolSetting(
        "analytics.mpp.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Runtime cap: the build-side IPC payload assembled by the coordinator-side capture sink may
     * not exceed this many bytes. When the build stage's accumulated Arrow IPC exceeds this
     * threshold during pass 1, the capture sink raises {@code BroadcastSizeExceededException}.
     *
     * <p>This same value drives the pre-flight planning gate in {@code OpenSearchBroadcastJoinSplitRule}
     * (a build whose estimated bytes exceed the cap never gets a broadcast alternative), and the
     * runtime cap stays the safety net for builds whose size CBO under-estimated (filter/semijoin
     * selectivity). On a runtime overflow the coordinator re-plans without broadcast (falling back
     * to hash-shuffle / coordinator-centric) rather than failing — see {@code DefaultPlanExecutor}.
     *
     * <p>Default 64 MiB: broadcast is for small dimension builds, and 64 MiB stays well within the
     * 256 MiB default {@code analytics.coordinator.buffer_limit} (≈1/4) even after the build is
     * captured + replicated. Operators broadcasting larger builds can raise this; very large builds
     * should route through hash-shuffle instead.
     */
    public static final Setting<ByteSizeValue> BROADCAST_MAX_BYTES = Setting.byteSizeSetting(
        "analytics.mpp.broadcast.max_bytes",
        new ByteSizeValue(64L * 1024 * 1024),
        new ByteSizeValue(0L),
        new ByteSizeValue(Long.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Number of hash-shuffle output partitions. When set to a positive value, every
     * HASH_SHUFFLE query uses this exact partition count regardless of cluster shape. When
     * unset (or non-positive), the partition count is resolved per-query via
     * {@code AnalyticsSearchBackendPlugin.defaultShuffleParallelism(ClusterState)} — backends
     * that participate in MPP shuffle (DataFusion today) return the count of probe-side data
     * nodes; backends that don't (Lucene today) return 1 and so opt out of the strategy
     * entirely (the split rule refuses to fire when partitionCount ≤ 1).
     *
     * <p>Default {@code -1} means "use engine default." Operators rarely need to override.
     */
    public static final Setting<Integer> MPP_SHUFFLE_PARTITIONS = Setting.intSetting(
        "analytics.mpp.shuffle.partitions",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-partition receive timeout for hash-shuffle consumers. Each consumer task blocks on
     * its {@code ShuffleBuffer} until both producer sides signal {@code isLast}; if no senders
     * complete within this timeout, the partition fails and the query terminates. The timeout
     * is a backstop against stuck producers (cancelled queries cascade through the walker
     * faster than this); 60s is conservative enough that healthy queries never hit it.
     */
    public static final Setting<TimeValue> MPP_SHUFFLE_RECV_TIMEOUT = Setting.timeSetting(
        "analytics.mpp.shuffle.recv_timeout",
        TimeValue.timeValueSeconds(60L),
        TimeValue.timeValueSeconds(1L),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Node-level on-heap hash-shuffle budget, as a PERCENT of the JVM max heap ({@code -Xmx}).
     *
     * <p>The shuffle consumer is buffer-all-then-drain: a worker blocks on
     * {@code ShuffleBuffer.awaitReady} until both producer sides finish, then drains the accumulated
     * Arrow-IPC {@code byte[]} chunks. Those chunks live ON the JVM heap, and a node's live shuffle
     * bytes are the SUM across every buffer it holds (all queries/stages/partitions). Without a bound
     * a large shuffle accumulates its whole input on-heap and OOMs the node (observed: 7.4 GB of
     * {@code byte[]} on an 8 GB heap for TPC-H q17 at sf=10). A PER-BUFFER cap can't bound the sum
     * (N partitions each under the cap still OOM in aggregate), so the budget is per-NODE.
     *
     * <p>{@code ShuffleBufferManager} admits a chunk only if the node total stays under
     * {@code percent% × maxHeap}; over-budget admissions are rejected for retry (room frees when
     * other queries finish) UNLESS a single query's own footprint exceeds the budget, which fails
     * fast and non-retryably with {@code ShuffleBufferExceededException} (waiting can't help — the
     * query can't fit even on an idle node). A percent (not an absolute byte value) auto-scales to
     * node heap size.
     *
     * <p>Default 80(%): generous enough that legitimately-fitting shuffles run while leaving heap for
     * the rest of query execution; bounds runaway buffers well before OOM. Operator remediation on a
     * fast failure: raise this (toward but below 100), give the node more heap, narrow the query, or
     * set {@code analytics.mpp.enabled=false}. Set to {@code 0} to disable the budget (pre-fix
     * behavior — NOT recommended; risks node OOM).
     */
    public static final Setting<Integer> MPP_SHUFFLE_NODE_BUDGET_PERCENT = Setting.intSetting(
        "analytics.mpp.shuffle.node_budget_percent",
        80,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Size floor for the general post-CBO distribution-enforcement pass ({@code DistributionEnforcementPass},
     * the only MPP scheduler): a join/aggregate is distributed onto a worker tier only when its larger scan
     * subtree exceeds this many rows (or a deeper operator already distributed — the cascade continues upward
     * regardless). Below the floor the operator stays coordinator-centric, matching CBO's cheap choice for
     * small joins — distribution adds shuffle overhead that only pays off at scale.
     *
     * <p>Default {@code 1_000_000}: well below any TPC-H fact table that needs distributing (partsupp 8M,
     * lineitem 60M) and well above trivial joins that gather cheaply. Exposed as a setting so the floor is
     * tunable per workload AND so integration tests on small datasets can lower it to exercise the
     * distributed path (the JVM tests use {@code minRows=1}; the cluster ITs set this to a small value).
     */
    public static final Setting<Long> MPP_DISTRIBUTE_MIN_ROWS = Setting.longSetting(
        "analytics.mpp.distribute.min_rows",
        1_000_000L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cost-based join reordering for multi-way joins. When {@code true}, a plan with 3+ joins is
     * collapsed into an n-ary {@code MultiJoin} and re-ordered by Calcite's bushy-join heuristic
     * ({@code JOIN_TO_MULTI_JOIN} + {@code MULTI_JOIN_OPTIMIZE_BUSHY}) using the per-index row counts
     * seeded by {@code IndexRowCountFetcher} — driving the smaller/more-selective joins first so the
     * fat fact-table intermediate is not carried through the whole tree (and not shuffled at every
     * tier). Runs pre-marking on the plain {@code Logical*} tree, in its own HEP phase, alongside the
     * column-prune + filter-pushdown pre-marking rewrites.
     *
     * <p>Gated to 3+ join plans (2-way plans have only one order) and to all-equi-join plans (a
     * cross-join — e.g. PPL {@code transpose} — is left untouched). Default {@code false}: join order
     * is taken as-written until the reorder is benchmarked, mirroring how prune/compress shipped
     * default-off first. The two reorder rules run as SEPARATE HEP instructions ({@code JOIN_TO_MULTI_JOIN}
     * to fixpoint, THEN the optimize rule) so they cannot invert each other into an infinite loop.
     */
    public static final Setting<Boolean> MPP_JOIN_REORDER = Setting.boolSetting(
        "analytics.mpp.join.reorder",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Build-side row threshold above which a hash-shuffle WORKER join uses a spillable sort-merge join
     * instead of the in-memory hash-join build. When a worker join's build-side (right input) estimated
     * scan rows exceed this value, the coordinator sets {@code prefer_hash_join=false} on that worker
     * stage, so DataFusion's physical planner emits a {@code SortMergeJoinExec} (which spills its buffered
     * batches to disk under memory pressure) rather than the {@code HashJoinExec} whose in-memory build
     * has no escape to disk and trips the native circuit breaker on large builds (TPC-H sf=10 q17/q18/q21).
     * This mirrors Spark's memory-safety rule: hash-join only when the build provably fits, else the
     * spillable join.
     *
     * <p>Below the threshold the worker keeps the (faster, no-sort) hash join. Only worker joins are
     * affected — shard-scan and coordinator-reduce sessions always prefer hash join. Default
     * {@code 20_000_000}: above the dimension builds that fit comfortably in memory (TPC-H supplier 100K,
     * part 2M, partsupp 8M at sf=10) and below the fact-table-scale builds that OOM. Set to
     * {@code Long.MAX_VALUE} to disable (always hash join — the pre-SMJ behavior) or {@code 0} to force
     * sort-merge on every worker join (A/B benchmarking).
     */
    public static final Setting<Long> MPP_WORKER_SORT_MERGE_JOIN_MIN_ROWS = Setting.longSetting(
        "analytics.mpp.worker.sort_merge_join_min_rows",
        20_000_000L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-strategy sub-toggle for distributed <em>aggregation</em> (the {@code HASH_SHUFFLE_AGG}
     * strategy): a decomposable {@code GROUP BY} over a distributed join is split PARTIAL (on the join's
     * worker tier, per-partition) + FINAL (gathered to the coordinator) by the general post-CBO pass
     * {@code DistributionEnforcementPass}, instead of gathering the whole join output and aggregating
     * serially on the coordinator.
     *
     * <p>Gated under {@link #MPP_ENABLED}: this only has effect when MPP is on. When {@code true}
     * (default — preserves current behavior), eligible aggregates over a distributed join run the
     * PARTIAL-on-worker / FINAL-on-coordinator split. When {@code false}, the enforcement pass does NOT
     * split the aggregate: it gathers the (possibly distributed) child and runs the SINGLE aggregate
     * coordinator-centric — exactly as if MPP were off, but scoped to aggregation only. Distributed JOINS
     * are unaffected (a join below the aggregate still runs on its worker tier). Useful as a targeted kill
     * switch when an agg-specific issue is seen, without disabling MPP joins.
     */
    public static final Setting<Boolean> MPP_SHUFFLE_AGGREGATE_ENABLED = Setting.boolSetting(
        "analytics.mpp.shuffle.aggregate.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Collapse co-partitioned join tiers into ONE worker tier ({@code false} = one tier per join, the
     * validated default).
     *
     * <p>When several joins in a tree partition on the SAME key, the lower join's output is already
     * hash-partitioned the way its parent needs it, so the inter-tier shuffle between them ships rows that
     * are already in the right place. With {@code true} the enforcement pass reuses such a co-partitioned
     * input in place and {@code GeneralShuffleDAGRewriter} promotes the whole collapsed sub-tree to a single
     * worker tier, saving one shuffle round-trip per collapsed level (a bushy 4-way join on one key goes
     * from 3 tiers to 1). This requires the N-ary shuffle transport ({@code ShuffleSlots}) — a collapsed
     * tier reads one slot per leaf, not two.
     *
     * <p>Default {@code false} because the saving is a TRADE, not free: collapsing removes a shuffle
     * round-trip but makes one worker run several joins, raising its peak memory. DataFusion's hash-join
     * build is non-spillable, so a collapsed tier can OOM where the tiered plan survived (the same hazard
     * behind {@code analytics.mpp.shuffle.sort_merge_join_min_rows}). Enable per-query and measure before
     * making it the default; the tiered shape stays the validated path.
     */
    public static final Setting<Boolean> MPP_COLLAPSE_COPARTITIONED_TIERS = Setting.boolSetting(
        "analytics.mpp.collapse_copartitioned_tiers",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Run a distributed aggregate's FINAL phase on a WORKER TIER, fed by a shuffle on the group keys,
     * instead of gathering every PARTIAL state to the coordinator.
     *
     * <p>Today a split aggregate is {@code FINAL( ER(SINGLETON)( PARTIAL(input) ) )}: every shard's partial
     * state crosses to the coordinator, which merges them all. For a HIGH-CARDINALITY grouping that gather is
     * the whole cost — it is what trips
     * {@code ReduceSizeExceededException: Coordinator-reduce buffer exceeded the per-query memory budget}
     * on the shared Arrow {@code POOL_QUERY} pool (TPC-H sf=10 q16-q21). With {@code true} the pass instead
     * emits {@code FINAL( SHUFFLE(hash groupKeys)( PARTIAL(input) ) )}: each partition receives every partial
     * for its groups, merges them locally, and the coordinator only CONCATENATES one row per group.
     *
     * <p>Correct because {@code PARTIAL} fronts its group keys to output positions {@code [0..groupCount)},
     * so hashing on those places every partial of a group in ONE partition — the per-partition merge is
     * therefore complete, and no cross-partition FINAL is needed. An EMPTY group set has no key to hash on
     * and always keeps the coordinator gather.
     *
     * <p>Default {@code false}: it trades the coordinator gather for a shuffle round-trip, which is a loss
     * when the grouping is low-cardinality (few partials, cheap to gather). Enable and measure per workload.
     */
    public static final Setting<Boolean> MPP_AGGREGATE_GROUP_KEY_SHUFFLE = Setting.boolSetting(
        "analytics.mpp.aggregate.group_key_shuffle",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Allow a JOIN to distribute even when one of its inputs is a GATHERED sub-stage, by letting the resulting
     * coordinator-reduce stage act as a hash-shuffle PRODUCER.
     *
     * <p>The pass's shippable-producer gate (step 3d) refuses to distribute such a join today: a gathered input
     * becomes a {@code ReduceStageExecution}, which historically could only emit to its parent sink, so
     * shuffling out of one left the consuming worker waiting on a producer that never fired
     * ({@code ShuffleScanHandler timed out for input-N}). With the reduce stage taught to resolve a producer
     * sink from its own instruction chain, that limit is lifted — and with it the reason every join above a
     * decorrelated subquery stays coordinator-centric (TPC-H q4 {@code exists}→SEMI, q22 {@code not exists}
     * →ANTI, q2/q15 scalar subqueries).
     *
     * <p>Default {@code false}: the failure mode of getting this wrong is a HANG rather than a wrong answer or
     * a clean error, so the coordinator-centric fallback stays the validated default until the distributed
     * shape is proven on a cluster for these shapes.
     */
    public static final Setting<Boolean> MPP_REDUCE_STAGE_SHUFFLE_PRODUCER = Setting.boolSetting(
        "analytics.mpp.reduce_stage_shuffle_producer",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Compute a sub-plan that the query evaluates MORE THAN ONCE only once, feeding every consumer from that
     * one result.
     *
     * <p>This is a CORRECTNESS fix before it is an optimization. A query that inlines the same aggregate
     * subquery twice — TPC-H q15 joins {@code revenue0} and then filters
     * {@code where total_revenue = [ … max(total_revenue) ]} over the same {@code revenue0}, because the
     * spec's VIEW has no PPL equivalent — aggregates each copy independently. {@code SUM(double)} is not
     * associative, so the copies' partial sums merge in different orders, disagree in the last bits, and the
     * exact {@code =} matches nothing: q15 then returns 1 row or 0 rows at random (measured 11/20 correct
     * without this, 20/20 with it). Sharing one evaluation makes both consumers read identical rows, so the
     * comparison holds whatever order the sum ran in — and halves the work.
     *
     * <p><b>Not an MPP setting</b>, despite living alongside them historically: sharing is done by
     * {@code DAGBuilder} for every analytics query and is deliberately NOT gated on {@link #MPP_ENABLED} — the
     * wrong answer it prevents happens coordinator-centric too. In fact it applies MORE often with distribution
     * off, because a distributed plan can put the two references in different fragments, where sharing does not
     * currently reach.
     *
     * <p>Default {@code true}, and it is a KILL SWITCH rather than an opt-in feature flag: the same posture
     * Spark takes for the equivalent transform ({@code spark.sql.exchange.reuse}, internal, default true since
     * 2.0.0). {@code SharedSubplanReuse} keeps sharing narrow — only a COMPLETE aggregate subtree with no
     * shuffle/broadcast/late-materialization boundary — and {@code DAGBuilder} rebuilds without sub-plan reuse
     * when the consumer would not buffer the shared input. What no internal fallback can catch is a WRONG
     * digest match (two subtrees that normalize equal without being equivalent), which would be a silent wrong
     * answer; set this to {@code false} to revert that class of incident without a rollback.
     */
    public static final Setting<Boolean> SUBPLAN_REUSE_ENABLED = Setting.boolSetting(
        "analytics.planner.subplan_reuse.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Master switch for hash-shuffle disk spill. When {@code true}, a query whose per-query shuffle
     * footprint would exceed the on-heap budget spills its oldest buffered Arrow-IPC chunks to disk
     * (see {@code ShuffleBufferManager.spillOldest}) instead of failing fast with
     * {@code ShuffleBufferExceededException}. This lets multi-GB shuffle intermediates (TPC-H q5/q10
     * at sf=10) RUN: the per-query on-heap footprint is bounded by the budget, the rest lives on disk,
     * and the consumer drains spilled chunks back (in arrival order) followed by the in-memory tail —
     * preserving the proven buffer-all consumer contract.
     *
     * <p>When {@code false} (default), behavior is byte-identical to the pre-spill fail-fast path: a
     * per-query budget breach still throws {@code ShuffleBufferExceededException}. The node-budget
     * REJECT_RETRY (transient cross-query contention) path is unchanged either way.
     *
     * <p>Even with spill enabled the query still fails — re-messaged to name {@code spill.max_bytes} /
     * disk-full — once the disk ceiling {@link #MPP_SHUFFLE_SPILL_MAX_BYTES} is hit or a spill write
     * I/O error occurs. Gated under {@link #MPP_ENABLED}.
     */
    public static final Setting<Boolean> MPP_SHUFFLE_SPILL_ENABLED = Setting.boolSetting(
        "analytics.mpp.shuffle.spill.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Directory under which hash-shuffle spill files are written, one subdir per query
     * ({@code <directory>/<queryId>/}). Default empty {@code ""} resolves at wiring time to
     * {@code <path.data>/shuffle_spill} (the node's first data path), so operators rarely need to set
     * it. When set, it must be a writable absolute path on the data node. Only consulted when
     * {@link #MPP_SHUFFLE_SPILL_ENABLED} is {@code true}.
     */
    public static final Setting<String> MPP_SHUFFLE_SPILL_DIRECTORY = Setting.simpleString(
        "analytics.mpp.shuffle.spill.directory",
        "",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Hard disk ceiling for hash-shuffle spill, in bytes, across all of this node's spill files.
     * Exceeding it (or hitting a disk-full / write I/O error) is the new terminal failure when spill
     * is enabled — surfaced as a re-messaged {@code ShuffleBufferExceededException} naming
     * {@code spill.max_bytes} / disk rather than the on-heap budget. Default 50 GiB
     * ({@code 53687091200}). Only consulted when {@link #MPP_SHUFFLE_SPILL_ENABLED} is {@code true}.
     */
    public static final Setting<Long> MPP_SHUFFLE_SPILL_MAX_BYTES = Setting.longSetting(
        "analytics.mpp.shuffle.spill.max_bytes",
        53687091200L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cost-model parameter: how many probe-side data nodes the broadcast exchange estimates it
     * has to replicate to. The broadcast cost is roughly {@code buildSide.rows × probeNodes};
     * this number is what enters the formula. Default {@code -1} means "use the cluster's
     * data-node count at planning time" — the natural answer when the probe-side index spans
     * all data nodes. Operators can override to tune for selective routing or to nudge the
     * cost model toward favoring or disfavoring broadcast in their workload.
     */
    public static final Setting<Integer> MPP_BROADCAST_PROBE_ESTIMATE = Setting.intSetting(
        "analytics.mpp.broadcast.probe_estimate",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Pre-marking column pruning for the distributed path: drop columns no operator references
     * before the plan is cut into stages, so a hash-shuffle carries only the join keys plus the
     * downstream-referenced columns rather than the full join-output width. On wide fact-table joins
     * (TPC-H) this shrinks the shuffled payload several-fold — the single biggest driver of the
     * distributed-join latency, and it keeps more queries under the on-heap shuffle budget without
     * spilling. Scoped to plans whose joins are all equi-joins (a cross-join — e.g. what PPL
     * {@code transpose} lowers to — is left untouched). Default {@code true}; disable only to isolate a
     * suspected pruning issue.
     */
    /**
     * NOTE: currently INERT. Upstream #22301 (`RelFieldTrimmer` projection pushdown) superseded the gated
     * pre-marking trim this used to control, and its trimmer runs unconditionally — so toggling this no
     * longer changes planning. Retained so clusters that already set it still start.
     */
    public static final Setting<Boolean> MPP_SHUFFLE_PRUNE_COLUMNS = Setting.boolSetting(
        "analytics.mpp.shuffle.prune_columns",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Master toggle for hash-shuffle IPC compression. When {@code true}, each shuffle IPC chunk is
     * compressed (standard Arrow IPC compression) before it is buffered/shipped, shrinking the
     * on-heap buffered bytes at the cost of per-buffer compress/decompress CPU. Default {@code false}:
     * with {@link #MPP_SHUFFLE_PRUNE_COLUMNS} on (the default), the shuffle is already narrow, so
     * compression's CPU cost usually outweighs its remaining heap benefit — enable it only when a node
     * is memory-constrained enough to prefer heap headroom over latency. The reader auto-detects the
     * codec from the IPC metadata, so a mixed-setting (rolling-restart) cluster still decodes correctly.
     */
    public static final Setting<Boolean> MPP_SHUFFLE_COMPRESS = Setting.boolSetting(
        "analytics.mpp.shuffle.compress",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Codec used when {@link #MPP_SHUFFLE_COMPRESS} is on — {@code zstd} (default; native zstd-jni,
     * best ratio) or {@code lz4} (native lz4-java LZ4-frame). Only the writer consults this; the
     * reader auto-detects. Validated at settings-apply time so a typo is rejected up front rather than
     * silently falling back to zstd (the codec resolver still treats any unexpected value as zstd
     * defensively, but the validator prevents the confusing case).
     */
    public static final Setting<String> MPP_COMPRESSION_CODEC = Setting.simpleString("analytics.mpp.compression.codec", "zstd", value -> {
        String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        if (!v.equals("zstd") && !v.equals("lz4") && !v.equals("lz4_frame")) {
            throw new IllegalArgumentException("analytics.mpp.compression.codec must be one of [zstd, lz4], got [" + value + "]");
        }
    }, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * ZSTD compression level when the codec is {@code zstd}. Default {@code 1} (matching Spark's
     * shuffle codec default) — measured faster end-to-end than Arrow's default level 3 (cheaper
     * consumer decompress + less GC) at comparable heap relief. Ignored by the LZ4 codec. Bounded to
     * zstd's valid range [1, 22] at settings-apply time so an invalid level (e.g. 0 or 100) is rejected
     * on the {@code PUT} rather than crashing the first compressed shuffle write.
     */
    public static final Setting<Integer> MPP_COMPRESSION_ZSTD_LEVEL = Setting.intSetting(
        "analytics.mpp.compression.zstd.level",
        1,
        1,
        22,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** All engine-level settings registered by {@code AnalyticsPlugin.getSettings()}. */
    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        MPP_ENABLED,
        SUBPLAN_REUSE_ENABLED,
        BROADCAST_MAX_BYTES,
        MPP_SHUFFLE_PARTITIONS,
        MPP_SHUFFLE_RECV_TIMEOUT,
        MPP_SHUFFLE_NODE_BUDGET_PERCENT,
        MPP_SHUFFLE_AGGREGATE_ENABLED,
        MPP_COLLAPSE_COPARTITIONED_TIERS,
        MPP_AGGREGATE_GROUP_KEY_SHUFFLE,
        MPP_REDUCE_STAGE_SHUFFLE_PRODUCER,
        MPP_DISTRIBUTE_MIN_ROWS,
        MPP_JOIN_REORDER,
        MPP_WORKER_SORT_MERGE_JOIN_MIN_ROWS,
        MPP_SHUFFLE_SPILL_ENABLED,
        MPP_SHUFFLE_SPILL_DIRECTORY,
        MPP_SHUFFLE_SPILL_MAX_BYTES,
        MPP_BROADCAST_PROBE_ESTIMATE,
        MPP_SHUFFLE_PRUNE_COLUMNS,
        MPP_SHUFFLE_COMPRESS,
        MPP_COMPRESSION_CODEC,
        MPP_COMPRESSION_ZSTD_LEVEL
    );
}

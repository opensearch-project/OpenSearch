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
     * Per-strategy sub-toggle for <em>cascaded</em> hash-shuffle joins (multi-way joins where each
     * binary join level runs as its own worker tier, the inner worker producing a shuffle to the
     * outer worker). When {@code true} (default), a post-CBO RelNode rewrite
     * ({@code CascadeShufflePlanRewriter}) turns each {@code OpenSearchJoin}'s coordinator-gathered
     * inputs into hash-shuffle inputs so {@code DAGBuilder} cuts a cascaded shuffle DAG, and the
     * recursive {@code HashShuffleDispatch} lifts every join level into a worker. When {@code false},
     * only the bottom join shuffles and the outer join coordinator-reduces (pre-cascade behavior) —
     * a kill switch if a cascade-specific issue is seen. Gated under {@link #MPP_ENABLED}.
     */
    public static final Setting<Boolean> MPP_SHUFFLE_CASCADE_ENABLED = Setting.boolSetting(
        "analytics.mpp.shuffle.cascade.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-strategy sub-toggle for <em>distributed aggregation over a cascade join</em> (the q5/q10
     * shape: {@code Sort? → stats … by … → dimension-joins → bottom hash-shuffle join}). When
     * {@code true} (default, under {@link #MPP_ENABLED} + {@link #MPP_SHUFFLE_CASCADE_ENABLED}), a
     * post-CBO DAG rewrite ({@code DistributedAggOverJoinRewriter}) pushes a PARTIAL aggregate onto
     * the cascade top worker — moving the dimension joins onto the worker as BROADCAST inputs so the
     * worker runs the whole pre-aggregate pipeline per partition — and leaves the FINAL aggregate
     * (plus Sort) on the coordinator. This caps the coordinator gather at {@code N × #distinct-groups}
     * partial rows instead of the full {@code ~1.7M} post-join rows, so q5/q10 at sf=10 stop OOMing
     * the coordinator with {@code ReduceSizeExceededException}.
     *
     * <p>When {@code false}, the aggregate stays a SINGLE on the coordinator over the gathered join
     * output (pre-distributed-agg behavior) — a kill switch if a distributed-agg-specific issue is
     * seen, without disabling the cascade join lift itself. Only SUM/COUNT/MIN/MAX/AVG-class
     * decomposable aggregates over an all-INNER cascade are pushed; STATE_EXPANDING /
     * COUNT(DISTINCT) / percentile shapes stay coordinator-centric regardless (the shared
     * {@code OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit} gate).
     */
    public static final Setting<Boolean> MPP_SHUFFLE_AGGREGATE_OVER_JOIN_ENABLED = Setting.boolSetting(
        "analytics.mpp.shuffle.aggregate_over_join.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-strategy sub-toggle for hash-shuffle <em>aggregation</em> (the {@code HASH_SHUFFLE_AGG}
     * strategy emitted by {@code OpenSearchAggregateShuffleSplitRule}: PARTIAL on shards →
     * hash-shuffle on group keys → FINAL on data-node workers in parallel).
     *
     * <p>Gated under {@link #MPP_ENABLED}: this only has effect when MPP is on. When {@code true}
     * (default — preserves current behavior), eligible {@code GROUP BY} aggregates may run the
     * parallel worker-tier shuffle. When {@code false}, the shuffle-aggregate split rule stays out
     * and aggregates route through the coordinator-centric PARTIAL+gather+FINAL path instead —
     * exactly as if MPP were off, but scoped to aggregation only (joins still use MPP). Useful as a
     * targeted kill switch when an agg-shuffle-specific issue is seen, without disabling MPP joins.
     */
    public static final Setting<Boolean> MPP_SHUFFLE_AGGREGATE_ENABLED = Setting.boolSetting(
        "analytics.mpp.shuffle.aggregate.enabled",
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

    /** All engine-level settings registered by {@code AnalyticsPlugin.getSettings()}. */
    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        MPP_ENABLED,
        BROADCAST_MAX_BYTES,
        MPP_SHUFFLE_PARTITIONS,
        MPP_SHUFFLE_RECV_TIMEOUT,
        MPP_SHUFFLE_NODE_BUDGET_PERCENT,
        MPP_SHUFFLE_AGGREGATE_ENABLED,
        MPP_SHUFFLE_CASCADE_ENABLED,
        MPP_SHUFFLE_AGGREGATE_OVER_JOIN_ENABLED,
        MPP_SHUFFLE_SPILL_ENABLED,
        MPP_SHUFFLE_SPILL_DIRECTORY,
        MPP_SHUFFLE_SPILL_MAX_BYTES,
        MPP_BROADCAST_PROBE_ESTIMATE
    );
}

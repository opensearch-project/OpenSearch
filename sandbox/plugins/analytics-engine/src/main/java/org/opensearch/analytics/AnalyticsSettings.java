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
        MPP_SHUFFLE_AGGREGATE_ENABLED,
        MPP_SHUFFLE_CASCADE_ENABLED,
        MPP_BROADCAST_PROBE_ESTIMATE
    );
}

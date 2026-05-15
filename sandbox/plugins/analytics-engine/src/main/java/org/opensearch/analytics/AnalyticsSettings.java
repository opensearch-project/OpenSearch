/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.opensearch.common.settings.Setting;
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
     * Master kill switch for MPP (multi-pass parallel) join dispatch.
     *
     * <p>When {@code true} (default), the engine routes joins through the strategy advisor
     * and may pick BROADCAST or HASH_SHUFFLE for eligible queries. When {@code false}, every
     * join falls back to the coordinator-centric (M0) path regardless of advisor decision —
     * useful as an incident-response kill switch when an MPP-specific issue is observed in
     * production, or for A/B comparison against the baseline path.
     *
     * <p>HASH_SHUFFLE already falls through to coordinator-centric until M2 lands, so today
     * this setting effectively gates the BROADCAST path; it will gate HASH_SHUFFLE too once
     * that's wired end-to-end.
     */
    public static final Setting<Boolean> MPP_ENABLED = Setting.boolSetting(
        "analytics.mpp.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Pre-flight gate: a join is eligible for broadcast only when the chosen build side's
     * estimated row count (from {@code IndicesStats.primaries.docs.count}, fetched per query
     * by {@code StatisticsCollector}) is at most this threshold. When stats are missing
     * (row count is zero or negative — IndicesStats unavailable, or the index is genuinely
     * empty), broadcast is refused fail-safe.
     *
     * <p>Default is conservative — 1M rows on a 100-byte-per-row table is ~100MB of build-side
     * scan output, which is at the upper end of what fits comfortably in coordinator memory
     * for a single query. Operators with consistently narrow rows can safely raise this.
     */
    public static final Setting<Long> BROADCAST_MAX_ROWS = Setting.longSetting(
        "analytics.mpp.broadcast_max_rows",
        1_000_000L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Runtime cap: the build-side IPC payload assembled by the coordinator-side capture sink
     * may not exceed this many bytes. When the build stage's accumulated Arrow IPC exceeds
     * this threshold during pass 1, the broadcast dispatch fails the query — operators can
     * either raise the cap, narrow the query, or flip {@link #MPP_ENABLED} off.
     *
     * <p>The runtime cap exists because pre-flight row-count gates can't account for filter
     * selectivity, projection width, or stats staleness. It's the actual memory safety net.
     */
    public static final Setting<ByteSizeValue> BROADCAST_MAX_BYTES = Setting.byteSizeSetting(
        "analytics.mpp.broadcast_max_bytes",
        new ByteSizeValue(32L * 1024 * 1024),
        new ByteSizeValue(0L),
        new ByteSizeValue(Long.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** All engine-level settings registered by {@code AnalyticsPlugin.getSettings()}. */
    public static final List<Setting<?>> ALL_SETTINGS = List.of(MPP_ENABLED, BROADCAST_MAX_ROWS, BROADCAST_MAX_BYTES);
}

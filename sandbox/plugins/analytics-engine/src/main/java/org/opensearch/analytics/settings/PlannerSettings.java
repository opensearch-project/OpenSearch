/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

/**
 * Top-level, hot-reloadable view of the cluster settings the planner consults at planning time.
 * Bundles the otherwise-scattered planner knobs into one wrapper so the planner context carries a
 * single object instead of one field (plus update wiring) per setting:
 *
 * <ul>
 *   <li>{@code analytics.shard_bucket_oversampling_factor} — shard-bucket oversampling factor
 *       (volatile; updated live).</li>
 *   <li>{@code analytics.delegation.<backend>.blocked_predicates} — the per-backend delegation
 *       block-list ({@link DelegationBlockList}, which self-updates via its own affix consumer).</li>
 * </ul>
 *
 * <p>{@link #create} reads current values and self-registers the dynamic update consumers, so a
 * single instance built at executor construction stays current for the node's lifetime. Tests that
 * don't exercise cluster settings use {@link #defaults()} or {@link #of}.
 *
 * @opensearch.internal
 */
public final class PlannerSettings {

    private volatile double oversamplingFactor;
    private final DelegationBlockList delegationBlockList;

    private PlannerSettings(double oversamplingFactor, DelegationBlockList delegationBlockList) {
        this.oversamplingFactor = oversamplingFactor;
        this.delegationBlockList = delegationBlockList;
    }

    /**
     * Production factory: seeds from the current cluster settings and registers update consumers so
     * later changes apply live. The block-list ({@link DelegationBlockList#create}) self-registers its
     * own affix consumer + registry-derived validator; the oversampling factor is refreshed here.
     */
    public static PlannerSettings create(ClusterSettings clusterSettings, Settings initialSettings, CapabilityRegistry registry) {
        DelegationBlockList blockList = DelegationBlockList.create(clusterSettings, initialSettings, registry);
        PlannerSettings settings = new PlannerSettings(
            AnalyticsApproximationSettings.SHARD_BUCKET_OVERSAMPLING_FACTOR.get(initialSettings),
            blockList
        );
        clusterSettings.addSettingsUpdateConsumer(
            AnalyticsApproximationSettings.SHARD_BUCKET_OVERSAMPLING_FACTOR,
            v -> settings.oversamplingFactor = v
        );
        return settings;
    }

    /** Planner defaults for unit tests: no oversampling, nothing blocked. */
    public static PlannerSettings defaults() {
        return new PlannerSettings(0.0, DelegationBlockList.empty());
    }

    /** Explicit values for tests that exercise a specific setting. */
    public static PlannerSettings of(double oversamplingFactor, DelegationBlockList delegationBlockList) {
        return new PlannerSettings(oversamplingFactor, delegationBlockList);
    }

    public double getOversamplingFactor() {
        return oversamplingFactor;
    }

    /** Per-backend delegation block-list consulted at marking time. Never null (defaults to empty). */
    public DelegationBlockList getDelegationBlockList() {
        return delegationBlockList;
    }
}

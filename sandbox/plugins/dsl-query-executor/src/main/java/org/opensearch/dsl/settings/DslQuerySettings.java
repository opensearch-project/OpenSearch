/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * The cluster-level operator knob for the DSL sub-plan fan-out — its width — plus a live holder for its
 * value.
 */
public final class DslQuerySettings {

    /**
     * Maximum number of sub-plans a single DSL query may execute concurrently ("K_setting").
     * Default 1, which is byte-identical to sequential execution; the hard maximum is
     * {@code SubPlanParallelism.MAX_K_SETTING}, whose javadoc records why widths above 2 are not yet
     * known-good. Room above 2 exists so an operator on a larger instance can widen without a deploy;
     * the width actually used is still clamped by the terms derived from the host.
     *
     * <p>The bound is spelled as a literal rather than read from {@code SubPlanParallelism}: that class
     * is deliberately dependency-free and lives in the {@code executor} package, which already depends
     * on this one, so importing it here would close a package cycle. The two are pinned to each other by
     * {@code SubPlanParallelismTests#testTheSettingsBoundMatchesTheHardCeiling}.
     */
    public static final Setting<Integer> MAX_PARALLEL_SUB_PLANS = Setting.intSetting(
        "dsl.query.max_parallel_sub_plans",
        1,
        1,
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // MAX_PARALLEL_SUB_PLANS above is a WIDTH knob, not an off switch, and it is the rollback for the
    // concurrency added here: its default of 1 IS the sequential path, so the fan-out is opt-in and
    // putting it back to 1 restores the prior behaviour without a deploy.

    /**
     * Every setting this plugin registers — {@code DslQueryExecutorPlugin.getSettings()} returns this
     * list verbatim. A setting missing from here is rejected in
     * {@code opensearch.yml}, invisible to {@code _cluster/settings} (a PUT of it is a 400), and not
     * resolvable by key from another plugin's classloader.
     *
     * @return the SC-1 width descriptor, the only setting this plugin registers
     */
    public static List<Setting<?>> all() {
        return List.of(MAX_PARALLEL_SUB_PLANS);
    }

    // Volatile: the update consumer runs on the cluster-applier thread while readers are on SEARCH threads.
    private volatile int maxParallelSubPlans;

    /**
     * Reads the settings from the node settings and registers update consumers so later
     * {@code _cluster/settings} changes are visible to readers without a restart.
     *
     * @param clusterService supplies the node settings and the {@link ClusterSettings} registry
     */
    public DslQuerySettings(ClusterService clusterService) {
        this.maxParallelSubPlans = MAX_PARALLEL_SUB_PLANS.get(clusterService.getSettings());
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(MAX_PARALLEL_SUB_PLANS, v -> maxParallelSubPlans = v);
    }

    /**
     * Current value of {@code dsl.query.max_parallel_sub_plans} — the "K_setting" term of the
     * fan-out width. Read this per query rather than caching it at construction time, otherwise the
     * value freezes for the life of the node and {@code Property.Dynamic} means nothing.
     *
     * @return the configured maximum concurrent sub-plans, always in [1, 5]
     */
    public int maxParallelSubPlans() {
        return maxParallelSubPlans;
    }

}

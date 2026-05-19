/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.IndexService;
import org.opensearch.index.MergeSchedulerConfig;

import java.util.List;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.NODE_PROCESSORS_SETTING;
import static org.opensearch.index.MergeSchedulerConfig.DEFAULT_AUTO_THROTTLE;

/**
 * Configuration manager for merge scheduler settings.
 *
 * <h2>What does this do?</h2>
 * Controls {@code merges.scheduler.max_merge_count} and {@code merges.scheduler.max_thread_count}.
 * These determine how many merge operations can run simultaneously and how many threads handle merging.
 *
 * <h2>Three-Level Priority System</h2>
 * Settings are applied in this order (prioritized highest to lowest):
 * <ol>
 *   <li><b>Index Level</b> (highest priority) - {@link MergeSchedulerConfig#MAX_MERGE_COUNT_SETTING}
 *       and {@link MergeSchedulerConfig#MAX_THREAD_COUNT_SETTING}</li>
 *   <li><b>Cluster Level</b> (middle priority) - {@link #CLUSTER_MAX_MERGE_COUNT_SETTING}
 *       and {@link #CLUSTER_MAX_THREAD_COUNT_SETTING}</li>
 *   <li><b>Absolute Default</b> (fallback) - Calculated automatically based on your CPU cores </li>
 * </ol>
 *
 * <h2>The One Rule</h2>
 * Always: {@code MAX_THREAD_COUNT <= MAX_MERGE_COUNT}
 * <p>
 * You can't have more threads than allowed merges.
 *
 * <h2>Quick Reference Table</h2>
 * <table border="1">
 * <caption>Settings hierarchy and fallback behavior for merge scheduler configuration</caption>
 *   <tr>
 *     <th>Index Setting</th>
 *     <th>Cluster Setting</th>
 *     <th>Result</th>
 *   </tr>
 *   <tr>
 *     <td>Not set</td>
 *     <td>Not set</td>
 *     <td>Absolute defaults (CPU-based calculation)</td>
 *   </tr>
 *   <tr>
 *     <td>Not set</td>
 *     <td>Set</td>
 *     <td>Cluster defaults</td>
 *   </tr>
 *   <tr>
 *     <td>Only thread count</td>
 *     <td>Doesn't matter</td>
 *     <td>Thread count = your value, Merge count = your value + 5</td>
 *   </tr>
 *   <tr>
 *     <td>Only merge count</td>
 *     <td>Doesn't matter</td>
 *     <td>Merge count = your value, Thread count = absolute default</td>
 *   </tr>
 *   <tr>
 *     <td>Both set</td>
 *     <td>Doesn't matter</td>
 *     <td>Both use your values (if valid)</td>
 *   </tr>
 * </table>
 *
 * <h2>Examples</h2>
 * See {@code org.opensearch.index.ClusterMergeSchedulerConfigsIT} for detailed examples of all configuration scenarios.
 *
 * @see MergeSchedulerConfig
 */
@ExperimentalApi
public class ClusterMergeSchedulerConfig {
    public static final Setting<Integer> CLUSTER_MAX_THREAD_COUNT_SETTING = new Setting<>(
        "cluster.index.merge.scheduler.max_thread_count",
        ClusterMergeSchedulerConfig::getClusterMaxThreadCountDefault,
        (s) -> Setting.parseInt(s, 1, "cluster.default.index.merge.scheduler.max_thread_count"),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Integer> CLUSTER_MAX_MERGE_COUNT_SETTING = new Setting<>(
        "cluster.index.merge.scheduler.max_merge_count",
        ClusterMergeSchedulerConfig::getClusterMaxMergeCountDefault,
        (s) -> Setting.parseInt(s, 1, "cluster.default.index.merge.scheduler.max_merge_count"),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> CLUSTER_AUTO_THROTTLE_SETTING = Setting.boolSetting(
        "cluster.index.merge.scheduler.auto_throttle",
        DEFAULT_AUTO_THROTTLE,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile static String clusterMaxThreadCountDefault;
    private volatile int clusterMaxThreadCount;
    private volatile int clusterMaxMergeCount;
    private volatile boolean clusterAutoThrottleEnabled;
    private final IndicesService indicesService;

    public ClusterMergeSchedulerConfig(IndicesService indicesService) {
        this.indicesService = indicesService;
        ClusterService clusterService = indicesService.clusterService();
        Settings clusterSettings = clusterService.getSettings();

        clusterMaxThreadCount = CLUSTER_MAX_THREAD_COUNT_SETTING.get(clusterSettings);
        clusterMaxMergeCount = CLUSTER_MAX_MERGE_COUNT_SETTING.get(clusterSettings);
        clusterAutoThrottleEnabled = CLUSTER_AUTO_THROTTLE_SETTING.get(clusterSettings);

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                this::setClusterMaxThreadAndMergeCount,
                List.of(CLUSTER_MAX_MERGE_COUNT_SETTING, CLUSTER_MAX_THREAD_COUNT_SETTING),
                this::validateMaxThreadAndMergeCount
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_AUTO_THROTTLE_SETTING, this::onClusterMergeAutoThrottleUpdate);
    }

    private void validateMaxThreadAndMergeCount(Settings settings) {
        // Cluster level validation - assert CLUSTER_DEFAULT_MAX_MERGE_COUNT_SETTING >= CLUSTER_DEFAULT_MAX_THREAD_COUNT_SETTING
        MergeSchedulerConfig.validateMaxThreadAndMergeCount(
            CLUSTER_MAX_THREAD_COUNT_SETTING.get(settings),
            CLUSTER_MAX_MERGE_COUNT_SETTING.get(settings)
        );
    }

    private void setClusterMaxThreadAndMergeCount(Settings settings) {
        // CLUSTER_DEFAULT_MAX_THREAD_COUNT_SETTING and CLUSTER_DEFAULT_MAX_MERGE_COUNT_SETTING
        // settings received here have already been validated using
        // IndicesService.validateMaxThreadAndMergeCount
        clusterMaxThreadCount = CLUSTER_MAX_THREAD_COUNT_SETTING.get(settings);
        clusterMaxMergeCount = CLUSTER_MAX_MERGE_COUNT_SETTING.get(settings);
        for (IndexService indexService : indicesService) {
            indexService.onDefaultMaxMergeOrThreadCountUpdate(clusterMaxThreadCount, clusterMaxMergeCount);
        }
    }

    private void onClusterMergeAutoThrottleUpdate(Boolean value) {
        clusterAutoThrottleEnabled = value;
        for (IndexService indexService : indicesService) {
            indexService.onDefaultAutoThrottleEnabledUpdate(clusterAutoThrottleEnabled);
        }
    }

    public Integer getClusterMaxMergeCount() {
        return this.clusterMaxMergeCount;
    }

    public Integer getClusterMaxThreadCount() {
        return this.clusterMaxThreadCount;
    }

    public Boolean getClusterMergeAutoThrottleEnabled() {
        return this.clusterAutoThrottleEnabled;
    }

    /**
     * Default value for {@code CLUSTER_DEFAULT_MAX_THREAD_COUNT_SETTING}
     */
    public static String getClusterMaxThreadCountDefault(Settings settings) {
        if (NODE_PROCESSORS_SETTING.exists(settings) || clusterMaxThreadCountDefault == null) {
            clusterMaxThreadCountDefault = Integer.toString(
                Math.max(1, Math.min(4, OpenSearchExecutors.allocatedProcessors(settings) / 2))
            );
        }

        return clusterMaxThreadCountDefault;
    }

    /**
     * Default value for {@code CLUSTER_DEFAULT_MAX_MERGE_COUNT_SETTING}
     */
    public static String getClusterMaxMergeCountDefault(Settings settings) {
        return Integer.toString(CLUSTER_MAX_THREAD_COUNT_SETTING.get(settings) + 5);
    }
}

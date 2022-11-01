/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmStats;

/**
 * Defines the settings related to the cancellation of SearchShardTasks.
 *
 * @opensearch.internal
 */
public class SearchShardTaskSettings {
    private static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();

    private static class Defaults {
        private static final double TOTAL_HEAP_PERCENT_THRESHOLD = 0.05;
    }

    /**
     * Defines the heap usage threshold (in percentage) for the sum of heap usages across all search shard tasks
     * before in-flight cancellation is applied.
     */
    private volatile double totalHeapPercentThreshold;
    public static final Setting<Double> SETTING_TOTAL_HEAP_PERCENT_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_shard_task.total_heap_percent_threshold",
        Defaults.TOTAL_HEAP_PERCENT_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchShardTaskSettings(Settings settings, ClusterSettings clusterSettings) {
        totalHeapPercentThreshold = SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_TOTAL_HEAP_PERCENT_THRESHOLD, this::setTotalHeapPercentThreshold);
    }

    public double getTotalHeapPercentThreshold() {
        return totalHeapPercentThreshold;
    }

    public long getTotalHeapBytesThreshold() {
        return (long) (HEAP_SIZE_BYTES * getTotalHeapPercentThreshold());
    }

    private void setTotalHeapPercentThreshold(double totalHeapPercentThreshold) {
        this.totalHeapPercentThreshold = totalHeapPercentThreshold;
    }
}

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
        private static final double TOTAL_HEAP_THRESHOLD = 0.05;
        private static final double HEAP_THRESHOLD = 0.005;
        private static final double HEAP_VARIANCE_THRESHOLD = 2.0;
        private static final long CPU_TIME_THRESHOLD = 15;
        private static final long ELAPSED_TIME_THRESHOLD = 30000;
    }

    /**
     * Defines the heap usage threshold (in percentage) for the sum of heap usages across all search shard tasks
     * before in-flight cancellation is applied.
     */
    private volatile double totalHeapThreshold;
    public static final Setting<Double> SETTING_TOTAL_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_shard_task.total_heap_threshold",
        Defaults.TOTAL_HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage threshold (in percentage) for an individual task before it is considered for cancellation.
     */
    private volatile double heapThreshold;
    public static final Setting<Double> SETTING_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_shard_task.heap_threshold",
        Defaults.HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage variance for an individual task before it is considered for cancellation.
     * A task is considered for cancellation when taskHeapUsage is greater than or equal to heapUsageMovingAverage * variance.
     */
    private volatile double heapVarianceThreshold;
    public static final Setting<Double> SETTING_HEAP_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_shard_task.heap_variance",
        Defaults.HEAP_VARIANCE_THRESHOLD,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the CPU usage threshold (in millis) for an individual task before it is considered for cancellation.
     */
    private volatile long cpuTimeThreshold;
    public static final Setting<Long> SETTING_CPU_TIME_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_shard_task.cpu_time_threshold",
        Defaults.CPU_TIME_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the elapsed time threshold (in millis) for an individual task before it is considered for cancellation.
     */
    private volatile long elapsedTimeThreshold;
    public static final Setting<Long> SETTING_ELAPSED_TIME_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_shard_task.elapsed_time_threshold",
        Defaults.ELAPSED_TIME_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchShardTaskSettings(Settings settings, ClusterSettings clusterSettings) {
        totalHeapThreshold = SETTING_TOTAL_HEAP_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_TOTAL_HEAP_THRESHOLD, this::setTotalHeapThreshold);

        heapThreshold = SETTING_HEAP_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_THRESHOLD, this::setHeapThreshold);

        heapVarianceThreshold = SETTING_HEAP_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_VARIANCE_THRESHOLD, this::setHeapVarianceThreshold);

        cpuTimeThreshold = SETTING_CPU_TIME_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CPU_TIME_THRESHOLD, this::setCpuTimeThreshold);

        elapsedTimeThreshold = SETTING_ELAPSED_TIME_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ELAPSED_TIME_THRESHOLD, this::setElapsedTimeThreshold);
    }

    public double getTotalHeapThreshold() {
        return totalHeapThreshold;
    }

    public long getTotalHeapThresholdBytes() {
        return (long) (HEAP_SIZE_BYTES * getTotalHeapThreshold());
    }

    private void setTotalHeapThreshold(double totalHeapThreshold) {
        this.totalHeapThreshold = totalHeapThreshold;
    }

    public double getHeapThreshold() {
        return heapThreshold;
    }

    public long getHeapThresholdBytes() {
        return (long) (HEAP_SIZE_BYTES * getHeapThreshold());
    }

    private void setHeapThreshold(double heapThreshold) {
        this.heapThreshold = heapThreshold;
    }

    public double getHeapVarianceThreshold() {
        return heapVarianceThreshold;
    }

    private void setHeapVarianceThreshold(double heapVarianceThreshold) {
        this.heapVarianceThreshold = heapVarianceThreshold;
    }

    public long getCpuTimeThreshold() {
        return cpuTimeThreshold;
    }

    private void setCpuTimeThreshold(long cpuTimeThreshold) {
        this.cpuTimeThreshold = cpuTimeThreshold;
    }

    public long getElapsedTimeThreshold() {
        return elapsedTimeThreshold;
    }

    private void setElapsedTimeThreshold(long elapsedTimeThreshold) {
        this.elapsedTimeThreshold = elapsedTimeThreshold;
    }
}

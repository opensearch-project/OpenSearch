/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.apache.logging.log4j.LogManager;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmStats;

import java.util.concurrent.TimeUnit;

/**
 * Defines the settings related to the cancellation of SearchTasks.
 *
 * @opensearch.internal
 */

public class SearchTaskSettings {
    private static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();

    private static class Defaults {
        private static final double TOTAL_HEAP_PERCENT_THRESHOLD = 0.05;
        private static final long CPU_TIME_MILLIS_THRESHOLD = 60000;
        private static final long ELAPSED_TIME_MILLIS_THRESHOLD = 120000;
        private static final double HEAP_PERCENT_THRESHOLD = 0.02;
        private static final double HEAP_VARIANCE_THRESHOLD = 2.0;
        private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;
    }

    /**
     * Defines the heap usage threshold (in percentage) for the sum of heap usages across all search tasks
     * before in-flight cancellation is applied.
     */
    private volatile double totalHeapPercentThreshold;
    public static final Setting<Double> SETTING_TOTAL_HEAP_PERCENT_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task.total_heap_percent_threshold",
        Defaults.TOTAL_HEAP_PERCENT_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the CPU usage threshold (in millis) for an individual search task before it is considered for cancellation.
     */
    private volatile long cpuTimeMillisThreshold;
    public static final Setting<Long> SETTING_CPU_TIME_MILLIS_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_task.cpu_time_millis_threshold",
        Defaults.CPU_TIME_MILLIS_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the elapsed time threshold (in millis) for an individual search task before it is considered for cancellation.
     */
    private volatile long elapsedTimeMillisThreshold;
    public static final Setting<Long> SETTING_ELAPSED_TIME_MILLIS_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_task.elapsed_time_millis_threshold",
        Defaults.ELAPSED_TIME_MILLIS_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage threshold (in percentage) for an individual search task before it is considered for cancellation.
     */
    private volatile double heapPercentThreshold;
    public static final Setting<Double> SETTING_HEAP_PERCENT_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task.heap_percent_threshold",
        Defaults.HEAP_PERCENT_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage variance for an individual search task before it is considered for cancellation.
     * A task is considered for cancellation when taskHeapUsage is greater than or equal to heapUsageMovingAverage * variance.
     */
    private volatile double heapVarianceThreshold;
    public static final Setting<Double> SETTING_HEAP_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task.heap_variance",
        Defaults.HEAP_VARIANCE_THRESHOLD,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the window size to calculate the moving average of heap usage of completed search tasks.
     */
    private volatile int heapMovingAverageWindowSize;
    public static final Setting<Integer> SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "search_backpressure.search_task.heap_moving_average_window_size",
        Defaults.HEAP_MOVING_AVERAGE_WINDOW_SIZE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchTaskSettings(Settings settings, ClusterSettings clusterSettings) {
        this.totalHeapPercentThreshold = SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.get(settings);
        this.cpuTimeMillisThreshold = SETTING_CPU_TIME_MILLIS_THRESHOLD.get(settings);
        this.elapsedTimeMillisThreshold = SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.get(settings);
        this.heapPercentThreshold = SETTING_HEAP_PERCENT_THRESHOLD.get(settings);
        this.heapVarianceThreshold = SETTING_HEAP_VARIANCE_THRESHOLD.get(settings);
        this.heapMovingAverageWindowSize = SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_TOTAL_HEAP_PERCENT_THRESHOLD, this::setTotalHeapPercentThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CPU_TIME_MILLIS_THRESHOLD, this::setCpuTimeMillisThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ELAPSED_TIME_MILLIS_THRESHOLD, this::setElapsedTimeMillisThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_PERCENT_THRESHOLD, this::setHeapPercentThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_VARIANCE_THRESHOLD, this::setHeapVarianceThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE, this::setHeapMovingAverageWindowSize);
    }

    public double getTotalHeapPercentThreshold() {
        return totalHeapPercentThreshold;
    }

    public long getTotalHeapBytesThreshold() {
        return (long) (HEAP_SIZE_BYTES * getTotalHeapPercentThreshold());
    }

    public long getCpuTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(cpuTimeMillisThreshold);
    }

    public long getElapsedTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(elapsedTimeMillisThreshold);
    }

    public long getHeapBytesThreshold() {
        return (long) (HEAP_SIZE_BYTES * heapPercentThreshold);
    }

    public double getHeapVarianceThreshold() {
        return heapVarianceThreshold;
    }

    public int getHeapMovingAverageWindowSize() {
        return heapMovingAverageWindowSize;
    }

    public void setTotalHeapPercentThreshold(double totalHeapPercentThreshold) {
        this.totalHeapPercentThreshold = totalHeapPercentThreshold;
    }

    public void setCpuTimeMillisThreshold(long cpuTimeMillisThreshold) {
        LogManager.getLogger(SearchTaskSettings.class).info("setCpuTimeMillisThreshold " + cpuTimeMillisThreshold);
        this.cpuTimeMillisThreshold = cpuTimeMillisThreshold;
    }

    public void setElapsedTimeMillisThreshold(long elapsedTimeMillisThreshold) {
        this.elapsedTimeMillisThreshold = elapsedTimeMillisThreshold;
    }

    public void setHeapPercentThreshold(double heapPercentThreshold) {
        this.heapPercentThreshold = heapPercentThreshold;
    }

    public void setHeapVarianceThreshold(double heapVarianceThreshold) {
        this.heapVarianceThreshold = heapVarianceThreshold;
    }

    public void setHeapMovingAverageWindowSize(int heapMovingAverageWindowSize) {
        this.heapMovingAverageWindowSize = heapMovingAverageWindowSize;
    }
}

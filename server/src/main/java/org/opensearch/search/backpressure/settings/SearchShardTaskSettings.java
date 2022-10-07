/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Defines the settings related to the cancellation of SearchShardTasks.
 *
 * @opensearch.internal
 */
public class SearchShardTaskSettings {
    private static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();

    private static class Defaults {
        private static final double TOTAL_HEAP_PERCENT_THRESHOLD = 0.05;
        private static final double HEAP_PERCENT_THRESHOLD = 0.005;
        private static final double HEAP_VARIANCE_THRESHOLD = 2.0;
        private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;
        private static final long CPU_TIME_MILLIS_THRESHOLD = 15;
        private static final long ELAPSED_TIME_MILLIS_THRESHOLD = 30000;
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

    /**
     * Defines the heap usage threshold (in percentage) for an individual task before it is considered for cancellation.
     */
    private volatile double heapPercentThreshold;
    public static final Setting<Double> SETTING_HEAP_PERCENT_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_shard_task.heap_percent_threshold",
        Defaults.HEAP_PERCENT_THRESHOLD,
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
     * Defines the window size to calculate the moving average of heap usage of completed tasks.
     */
    private volatile int heapMovingAverageWindowSize;
    public static final Setting<Integer> SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "search_backpressure.search_shard_task.heap_moving_average_window_size",
        Defaults.HEAP_MOVING_AVERAGE_WINDOW_SIZE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the CPU usage threshold (in millis) for an individual task before it is considered for cancellation.
     */
    private volatile long cpuTimeMillisThreshold;
    public static final Setting<Long> SETTING_CPU_TIME_MILLIS_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_shard_task.cpu_time_millis_threshold",
        Defaults.CPU_TIME_MILLIS_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the elapsed time threshold (in millis) for an individual task before it is considered for cancellation.
     */
    private volatile long elapsedTimeMillisThreshold;
    public static final Setting<Long> SETTING_ELAPSED_TIME_MILLIS_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_shard_task.elapsed_time_millis_threshold",
        Defaults.ELAPSED_TIME_MILLIS_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Callback listeners.
     */
    public interface Listener {
        void onHeapMovingAverageWindowSizeChanged(int newWindowSize);
    }

    private final List<Listener> listeners = new ArrayList<>();

    public SearchShardTaskSettings(Settings settings, ClusterSettings clusterSettings) {
        totalHeapPercentThreshold = SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_TOTAL_HEAP_PERCENT_THRESHOLD, this::setTotalHeapPercentThreshold);

        heapPercentThreshold = SETTING_HEAP_PERCENT_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_PERCENT_THRESHOLD, this::setHeapPercentThreshold);

        heapVarianceThreshold = SETTING_HEAP_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_VARIANCE_THRESHOLD, this::setHeapVarianceThreshold);

        heapMovingAverageWindowSize = SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE, this::setHeapMovingAverageWindowSize);

        cpuTimeMillisThreshold = SETTING_CPU_TIME_MILLIS_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CPU_TIME_MILLIS_THRESHOLD, this::setCpuTimeMillisThreshold);

        elapsedTimeMillisThreshold = SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ELAPSED_TIME_MILLIS_THRESHOLD, this::setElapsedTimeMillisThreshold);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
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

    public double getHeapPercentThreshold() {
        return heapPercentThreshold;
    }

    public long getHeapBytesThreshold() {
        return (long) (HEAP_SIZE_BYTES * getHeapPercentThreshold());
    }

    private void setHeapPercentThreshold(double heapPercentThreshold) {
        this.heapPercentThreshold = heapPercentThreshold;
    }

    public double getHeapVarianceThreshold() {
        return heapVarianceThreshold;
    }

    private void setHeapVarianceThreshold(double heapVarianceThreshold) {
        this.heapVarianceThreshold = heapVarianceThreshold;
    }

    public int getHeapMovingAverageWindowSize() {
        return heapMovingAverageWindowSize;
    }

    public void setHeapMovingAverageWindowSize(int heapMovingAverageWindowSize) {
        this.heapMovingAverageWindowSize = heapMovingAverageWindowSize;

        List<Exception> exceptions = new ArrayList<>();
        for (Listener listener : listeners) {
            try {
                listener.onHeapMovingAverageWindowSizeChanged(heapMovingAverageWindowSize);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    public long getCpuTimeMillisThreshold() {
        return cpuTimeMillisThreshold;
    }

    public long getCpuTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(getCpuTimeMillisThreshold());
    }

    private void setCpuTimeMillisThreshold(long cpuTimeMillisThreshold) {
        this.cpuTimeMillisThreshold = cpuTimeMillisThreshold;
    }

    public long getElapsedTimeMillisThreshold() {
        return elapsedTimeMillisThreshold;
    }

    public long getElapsedTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(getElapsedTimeMillisThreshold());
    }

    private void setElapsedTimeMillisThreshold(long elapsedTimeMillisThreshold) {
        this.elapsedTimeMillisThreshold = elapsedTimeMillisThreshold;
    }
}

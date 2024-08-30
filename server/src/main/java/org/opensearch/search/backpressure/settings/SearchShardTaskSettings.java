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
import org.opensearch.search.backpressure.CancellationSettingsListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Defines the settings related to the cancellation of SearchShardTasks.
 *
 * @opensearch.internal
 */
public class SearchShardTaskSettings {
    private final List<CancellationSettingsListener> listeners = new ArrayList<>();
    private final ClusterSettings clusterSettings;

    private static class Defaults {
        private static final double CANCELLATION_RATIO = 0.1;
        private static final double CANCELLATION_RATE = 0.003;
        private static final double CANCELLATION_BURST = 10.0;
        private static final double TOTAL_HEAP_PERCENT_THRESHOLD = 0.05;
        private static final long CPU_TIME_MILLIS_THRESHOLD = 15000;
        private static final long ELAPSED_TIME_MILLIS_THRESHOLD = 30000;
        private static final double HEAP_PERCENT_THRESHOLD = 0.005;
        private static final double HEAP_VARIANCE_THRESHOLD = 2.0;
        private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;
    }

    /**
     * Defines the percentage of SearchShardTasks to cancel relative to the number of successful SearchShardTasks completions.
     * In other words, it is the number of tokens added to the bucket on each successful SearchShardTask completion.
     */
    private volatile double cancellationRatio;
    public static final Setting<Double> SETTING_CANCELLATION_RATIO = Setting.doubleSetting(
        "search_backpressure.search_shard_task.cancellation_ratio",
        SearchBackpressureSettings.SETTING_CANCELLATION_RATIO,
        value -> {
            if (value <= 0.0) {
                throw new IllegalArgumentException("search_backpressure.search_shard_task.cancellation_ratio must be > 0");
            }
            if (value > 1.0) {
                throw new IllegalArgumentException("search_backpressure.search_shard_task.cancellation_ratio must <= 1.0");
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of SearchShardTasks to cancel per unit time (in millis).
     * In other words, it is the number of tokens added to the bucket each millisecond.
     */
    private volatile double cancellationRate;
    public static final Setting<Double> SETTING_CANCELLATION_RATE = Setting.doubleSetting(
        "search_backpressure.search_shard_task.cancellation_rate",
        SearchBackpressureSettings.SETTING_CANCELLATION_RATE,
        value -> {
            if (value <= 0.0) {
                throw new IllegalArgumentException("search_backpressure.search_shard_task.cancellation_rate must be > 0");
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the maximum number of SearchShardTasks that can be cancelled before being rate-limited.
     */
    private volatile double cancellationBurst;
    public static final Setting<Double> SETTING_CANCELLATION_BURST = Setting.doubleSetting(
        "search_backpressure.search_shard_task.cancellation_burst",
        SearchBackpressureSettings.SETTING_CANCELLATION_BURST,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

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
     * Defines the CPU usage threshold (in millis) for an individual search shard task before it is considered for cancellation.
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
     * Defines the elapsed time threshold (in millis) for an individual search shard task before it is considered for cancellation.
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
     * Defines the heap usage threshold (in percentage) for an individual search shard task before it is considered for cancellation.
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
     * Defines the heap usage variance for an individual search shard task before it is considered for cancellation.
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
     * Defines the window size to calculate the moving average of heap usage of completed search shard tasks.
     */
    private volatile int heapMovingAverageWindowSize;
    public static final Setting<Integer> SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "search_backpressure.search_shard_task.heap_moving_average_window_size",
        Defaults.HEAP_MOVING_AVERAGE_WINDOW_SIZE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchShardTaskSettings(Settings settings, ClusterSettings clusterSettings) {
        totalHeapPercentThreshold = SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.get(settings);
        this.cpuTimeMillisThreshold = SETTING_CPU_TIME_MILLIS_THRESHOLD.get(settings);
        this.elapsedTimeMillisThreshold = SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.get(settings);
        this.heapPercentThreshold = SETTING_HEAP_PERCENT_THRESHOLD.get(settings);
        this.heapVarianceThreshold = SETTING_HEAP_VARIANCE_THRESHOLD.get(settings);
        this.heapMovingAverageWindowSize = SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        this.cancellationRatio = SETTING_CANCELLATION_RATIO.get(settings);
        this.cancellationRate = SETTING_CANCELLATION_RATE.get(settings);
        this.cancellationBurst = SETTING_CANCELLATION_BURST.get(settings);
        this.clusterSettings = clusterSettings;

        clusterSettings.addSettingsUpdateConsumer(SETTING_TOTAL_HEAP_PERCENT_THRESHOLD, this::setTotalHeapPercentThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CPU_TIME_MILLIS_THRESHOLD, this::setCpuTimeMillisThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ELAPSED_TIME_MILLIS_THRESHOLD, this::setElapsedTimeMillisThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_PERCENT_THRESHOLD, this::setHeapPercentThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_VARIANCE_THRESHOLD, this::setHeapVarianceThreshold);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE, this::setHeapMovingAverageWindowSize);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATIO, this::setCancellationRatio);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATE, this::setCancellationRate);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_BURST, this::setCancellationBurst);
    }

    public double getTotalHeapPercentThreshold() {
        return totalHeapPercentThreshold;
    }

    public long getCpuTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(cpuTimeMillisThreshold);
    }

    public long getElapsedTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(elapsedTimeMillisThreshold);
    }

    public double getHeapPercentThreshold() {
        return heapPercentThreshold;
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

    public double getCancellationRatio() {
        return cancellationRatio;
    }

    void setCancellationRatio(double cancellationRatio) {
        this.cancellationRatio = cancellationRatio;
        notifyListeners(listener -> listener.onRatioChanged(cancellationRatio));
    }

    public double getCancellationRate() {
        return cancellationRate;
    }

    public double getCancellationRateNanos() {
        return getCancellationRate() / TimeUnit.MILLISECONDS.toNanos(1); // rate per nanoseconds
    }

    void setCancellationRate(double cancellationRate) {
        this.cancellationRate = cancellationRate;
        notifyListeners(listener -> listener.onRateChanged(cancellationRate));
    }

    public double getCancellationBurst() {
        return cancellationBurst;
    }

    void setCancellationBurst(double cancellationBurst) {
        this.cancellationBurst = cancellationBurst;
        notifyListeners(listener -> listener.onBurstChanged(cancellationBurst));
    }

    public void addListener(CancellationSettingsListener listener) {
        listeners.add(listener);
    }

    private void notifyListeners(Consumer<CancellationSettingsListener> consumer) {
        List<Exception> exceptions = new ArrayList<>();

        for (CancellationSettingsListener listener : listeners) {
            try {
                consumer.accept(listener);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }
}

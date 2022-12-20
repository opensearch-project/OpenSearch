/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER;

/**
 * HeapUsageTracker evaluates if the task has consumed too much heap than allowed.
 * It also compares the task's heap usage against a historical moving average of previously completed tasks.
 *
 * @opensearch.internal
 */
public class HeapUsageTracker extends TaskResourceUsageTracker {
    private static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();

    private static class Defaults {
        private static final double HEAP_PERCENT_THRESHOLD_FOR_SEARCH_QUERY = 0.02;
        private static final double HEAP_PERCENT_THRESHOLD = 0.005;
        private static final double HEAP_VARIANCE_THRESHOLD_FOR_SEARCH_QUERY = 2.0;
        private static final double HEAP_VARIANCE_THRESHOLD = 2.0;
        private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE_FOR_SEARCH_QUERY = 100;
        private static final int HEAP_MOVING_AVERAGE_WINDOW_SIZE = 100;
    }

    /**
     * Defines the heap usage threshold (in percentage) for an individual search task before it is considered for cancellation.
     */
    private volatile double heapPercentThresholdForSearchQuery;
    public static final Setting<Double> SETTING_HEAP_PERCENT_THRESHOLD_FOR_SEARCH_QUERY = Setting.doubleSetting(
        "search_backpressure.search_task.heap_percent_threshold_for_search_query",
        Defaults.HEAP_PERCENT_THRESHOLD_FOR_SEARCH_QUERY,
        0.0,
        1.0,
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
     * Defines the heap usage variance for an individual search task before it is considered for cancellation.
     * A task is considered for cancellation when taskHeapUsage is greater than or equal to heapUsageMovingAverage * variance.
     */
    private volatile double heapVarianceThresholdForSearchQuery;
    public static final Setting<Double> SETTING_HEAP_VARIANCE_THRESHOLD_FOR_SEARCH_QUERY = Setting.doubleSetting(
        "search_backpressure.search_task.heap_variance_for_search_query",
        Defaults.HEAP_VARIANCE_THRESHOLD_FOR_SEARCH_QUERY,
        0.0,
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
     * Defines the window size to calculate the moving average of heap usage of completed search tasks.
     */
    private volatile int heapMovingAverageWindowSizeForSearchQuery;
    public static final Setting<Integer> SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE_FOR_SEARCH_QUERY = Setting.intSetting(
        "search_backpressure.search_task.heap_moving_average_window_size_for_search_query",
        Defaults.HEAP_MOVING_AVERAGE_WINDOW_SIZE_FOR_SEARCH_QUERY,
        0,
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

    private final AtomicReference<MovingAverage> movingAverageReferenceForSearchQuery;
    private final AtomicReference<MovingAverage> movingAverageReference;

    public HeapUsageTracker(SearchBackpressureSettings settings) {
        heapPercentThresholdForSearchQuery = SETTING_HEAP_PERCENT_THRESHOLD_FOR_SEARCH_QUERY.get(settings.getSettings());
        settings.getClusterSettings()
            .addSettingsUpdateConsumer(SETTING_HEAP_PERCENT_THRESHOLD_FOR_SEARCH_QUERY, this::setHeapPercentThresholdForSearchQuery);
        heapPercentThreshold = SETTING_HEAP_PERCENT_THRESHOLD.get(settings.getSettings());
        settings.getClusterSettings().addSettingsUpdateConsumer(SETTING_HEAP_PERCENT_THRESHOLD, this::setHeapPercentThreshold);

        heapPercentThresholdForSearchQuery = SETTING_HEAP_VARIANCE_THRESHOLD_FOR_SEARCH_QUERY.get(settings.getSettings());
        settings.getClusterSettings()
            .addSettingsUpdateConsumer(SETTING_HEAP_VARIANCE_THRESHOLD_FOR_SEARCH_QUERY, this::setHeapVarianceThresholdForSearchQuery);
        heapVarianceThreshold = SETTING_HEAP_VARIANCE_THRESHOLD.get(settings.getSettings());
        settings.getClusterSettings().addSettingsUpdateConsumer(SETTING_HEAP_VARIANCE_THRESHOLD, this::setHeapVarianceThreshold);

        heapMovingAverageWindowSizeForSearchQuery = SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE_FOR_SEARCH_QUERY.get(settings.getSettings());
        settings.getClusterSettings()
            .addSettingsUpdateConsumer(
                SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE_FOR_SEARCH_QUERY,
                this::setHeapMovingAverageWindowSizeForSearchQuery
            );
        heapMovingAverageWindowSize = SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE.get(settings.getSettings());
        settings.getClusterSettings()
            .addSettingsUpdateConsumer(SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE, this::setHeapMovingAverageWindowSize);

        this.movingAverageReferenceForSearchQuery = new AtomicReference<>(new MovingAverage(heapMovingAverageWindowSizeForSearchQuery));
        this.movingAverageReference = new AtomicReference<>(new MovingAverage(heapMovingAverageWindowSize));
    }

    @Override
    public String name() {
        return HEAP_USAGE_TRACKER.getName();
    }

    @Override
    public void update(Task task) {
        if (task instanceof SearchTask) {
            movingAverageReferenceForSearchQuery.get().record(task.getTotalResourceStats().getMemoryInBytes());
        } else {
            movingAverageReference.get().record(task.getTotalResourceStats().getMemoryInBytes());
        }
    }

    @Override
    public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
        MovingAverage movingAverage = (task instanceof SearchTask)
            ? movingAverageReferenceForSearchQuery.get()
            : movingAverageReference.get();

        // There haven't been enough measurements.
        if (movingAverage.isReady() == false) {
            return Optional.empty();
        }

        double currentUsage = task.getTotalResourceStats().getMemoryInBytes();
        double averageUsage = movingAverage.getAverage();
        double variance = (task instanceof SearchTask) ? getHeapVarianceThresholdForSearchQuery() : getHeapVarianceThreshold();
        double allowedUsage = averageUsage * variance;
        double threshold = (task instanceof SearchTask) ? getHeapBytesThresholdForSearchQuery() : getHeapBytesThreshold();

        if (currentUsage < threshold || currentUsage < allowedUsage) {
            return Optional.empty();
        }

        return Optional.of(
            new TaskCancellation.Reason(
                "heap usage exceeded [" + new ByteSizeValue((long) currentUsage) + " >= " + new ByteSizeValue((long) allowedUsage) + "]",
                (int) (currentUsage / averageUsage)  // TODO: fine-tune the cancellation score/weight
            )
        );
    }

    public long getHeapBytesThresholdForSearchQuery() {
        return (long) (HEAP_SIZE_BYTES * heapPercentThresholdForSearchQuery);
    }

    public long getHeapBytesThreshold() {
        return (long) (HEAP_SIZE_BYTES * heapPercentThreshold);
    }

    public void setHeapPercentThresholdForSearchQuery(double heapPercentThresholdForSearchQuery) {
        this.heapPercentThresholdForSearchQuery = heapPercentThresholdForSearchQuery;
    }

    public void setHeapPercentThreshold(double heapPercentThreshold) {
        this.heapPercentThreshold = heapPercentThreshold;
    }

    public double getHeapVarianceThresholdForSearchQuery() {
        return heapVarianceThresholdForSearchQuery;
    }

    public double getHeapVarianceThreshold() {
        return heapVarianceThreshold;
    }

    public void setHeapVarianceThresholdForSearchQuery(double heapVarianceThresholdForSearchQuery) {
        this.heapVarianceThresholdForSearchQuery = heapVarianceThresholdForSearchQuery;
    }

    public void setHeapVarianceThreshold(double heapVarianceThreshold) {
        this.heapVarianceThreshold = heapVarianceThreshold;
    }

    public void setHeapMovingAverageWindowSizeForSearchQuery(int heapMovingAverageWindowSizeForSearchQuery) {
        this.heapMovingAverageWindowSizeForSearchQuery = heapMovingAverageWindowSizeForSearchQuery;
        this.movingAverageReferenceForSearchQuery.set(new MovingAverage(heapMovingAverageWindowSizeForSearchQuery));
    }

    public void setHeapMovingAverageWindowSize(int heapMovingAverageWindowSize) {
        this.heapMovingAverageWindowSize = heapMovingAverageWindowSize;
        this.movingAverageReference.set(new MovingAverage(heapMovingAverageWindowSize));
    }

    @Override
    public TaskResourceUsageTracker.Stats searchTaskStats(List<? extends Task> searchTasks) {
        long currentMax = searchTasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).max().orElse(0);
        long currentAvg = (long) searchTasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).average().orElse(0);
        return new Stats(
            getSearchTaskCancellationCount(),
            currentMax,
            currentAvg,
            (long) movingAverageReferenceForSearchQuery.get().getAverage()
        );
    }

    @Override
    public TaskResourceUsageTracker.Stats searchShardTaskStats(List<? extends Task> searchShardTasks) {
        long currentMax = searchShardTasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).max().orElse(0);
        long currentAvg = (long) searchShardTasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).average().orElse(0);
        return new Stats(getSearchShardTaskCancellationCount(), currentMax, currentAvg, (long) movingAverageReference.get().getAverage());
    }

    /**
     * Stats related to HeapUsageTracker.
     */
    public static class Stats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;
        private final long currentMax;
        private final long currentAvg;
        private final long rollingAvg;

        public Stats(long cancellationCount, long currentMax, long currentAvg, long rollingAvg) {
            this.cancellationCount = cancellationCount;
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
            this.rollingAvg = rollingAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("cancellation_count", cancellationCount)
                .humanReadableField("current_max_bytes", "current_max", new ByteSizeValue(currentMax))
                .humanReadableField("current_avg_bytes", "current_avg", new ByteSizeValue(currentAvg))
                .humanReadableField("rolling_avg_bytes", "rolling_avg", new ByteSizeValue(rollingAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
            out.writeVLong(rollingAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return cancellationCount == stats.cancellationCount
                && currentMax == stats.currentMax
                && currentAvg == stats.currentAvg
                && rollingAvg == stats.rollingAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount, currentMax, currentAvg, rollingAvg);
        }
    }
}

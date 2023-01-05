/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.settings.SearchTaskSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER;

/**
 * HeapUsageTracker evaluates if the task has consumed too much heap than allowed.
 * It also compares the task's heap usage against a historical moving average of previously completed tasks.
 *
 * @opensearch.internal
 */
public class HeapUsageTracker extends TaskResourceUsageTracker {
    private static final long HEAP_SIZE_BYTES = JvmStats.jvmStats().getMem().getHeapMax().getBytes();
    private final DoubleSupplier heapVarianceSupplier;
    private final LongSupplier heapBytesThresholdSupplier;
    private final IntSupplier windowSizeSupplier;
    private final AtomicReference<MovingAverage> movingAverageReference;

    public HeapUsageTracker(
        DoubleSupplier heapVarianceSupplier,
        LongSupplier heapBytesThresholdSupplier,
        IntSupplier windowSizeSupplier,
        ClusterSettings clusterSettings
    ) {
        this.heapVarianceSupplier = heapVarianceSupplier;
        this.heapBytesThresholdSupplier = heapBytesThresholdSupplier;
        this.windowSizeSupplier = windowSizeSupplier;
        this.movingAverageReference = new AtomicReference<>(new MovingAverage(windowSizeSupplier.getAsInt()));
        // TODO: find a way to get the type of the setting SearchTaskSettings/SearchShardTaskSettings and then add consumer only for the
        // required setting
        clusterSettings.addSettingsUpdateConsumer(SearchTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE, this::updateWindowSize);
        clusterSettings.addSettingsUpdateConsumer(SearchShardTaskSettings.SETTING_HEAP_MOVING_AVERAGE_WINDOW_SIZE, this::updateWindowSize);
    }

    @Override
    public String name() {
        return HEAP_USAGE_TRACKER.getName();
    }

    @Override
    public void update(Task task) {
        movingAverageReference.get().record(task.getTotalResourceStats().getMemoryInBytes());
    }

    @Override
    public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
        MovingAverage movingAverage = movingAverageReference.get();

        // There haven't been enough measurements.
        if (movingAverage.isReady() == false) {
            return Optional.empty();
        }

        double currentUsage = task.getTotalResourceStats().getMemoryInBytes();
        double averageUsage = movingAverage.getAverage();
        double variance = heapVarianceSupplier.getAsDouble();
        double allowedUsage = averageUsage * variance;
        double threshold = heapBytesThresholdSupplier.getAsLong();

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

    private void updateWindowSize(int heapMovingAverageWindowSize) {
        this.movingAverageReference.set(new MovingAverage(heapMovingAverageWindowSize));
    }

    @Override
    public TaskResourceUsageTracker.Stats stats(List<? extends Task> tasks) {
        long currentMax = tasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).max().orElse(0);
        long currentAvg = (long) tasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).average().orElse(0);
        return new Stats(getCancellations(), currentMax, currentAvg, (long) movingAverageReference.get().getAverage());
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * HeapUsageTracker evaluates if the task has consumed too much heap than allowed.
 * It also compares the task's heap usage against a historical moving average of previously completed tasks.
 */
public class HeapUsageTracker extends ResourceUsageTracker {
    public static final String NAME = "heap_usage_tracker";

    private final LongSupplier searchTaskHeapThresholdBytesSupplier;
    private final DoubleSupplier searchTaskHeapVarianceThresholdSupplier;
    private final MovingAverage movingAverage = new MovingAverage(100);

    public HeapUsageTracker(LongSupplier searchTaskHeapThresholdBytesSupplier, DoubleSupplier searchTaskHeapVarianceThresholdSupplier) {
        this.searchTaskHeapThresholdBytesSupplier = searchTaskHeapThresholdBytesSupplier;
        this.searchTaskHeapVarianceThresholdSupplier = searchTaskHeapVarianceThresholdSupplier;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void update(Task task) {
        movingAverage.record(task.getTotalResourceStats().getMemoryInBytes());
    }

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        // There haven't been enough measurements.
        if (movingAverage.isReady() == false) {
            return Optional.empty();
        }

        double taskHeapUsage = task.getTotalResourceStats().getMemoryInBytes();
        double averageHeapUsage = movingAverage.getAverage();
        double allowedHeapUsage = averageHeapUsage * searchTaskHeapVarianceThresholdSupplier.getAsDouble();

        if (taskHeapUsage < searchTaskHeapThresholdBytesSupplier.getAsLong() || taskHeapUsage < allowedHeapUsage) {
            return Optional.empty();
        }

        return Optional.of(new TaskCancellation.Reason(this, "heap usage exceeded", (int) (taskHeapUsage / averageHeapUsage)));
    }

    @Override
    public ResourceUsageTracker.Stats currentStats(List<Task> activeTasks) {
        long currentMax = activeTasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).max().orElse(0);
        long currentAvg = (long) activeTasks.stream().mapToLong(t -> t.getTotalResourceStats().getMemoryInBytes()).average().orElse(0);
        return new Stats(currentMax, currentAvg, (long) movingAverage.getAverage());
    }

    /**
     * Stats for HeapUsageTracker as seen in "_node/stats/search_backpressure" API.
     */
    public static class Stats implements ResourceUsageTracker.Stats {
        private final long currentMax;
        private final long currentAvg;
        private final long rollingAvg;

        public Stats(long currentMax, long currentAvg, long rollingAvg) {
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
            this.rollingAvg = rollingAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .humanReadableField("current_max_bytes", "current_max", new ByteSizeValue(currentMax))
                .humanReadableField("current_avg_bytes", "current_avg", new ByteSizeValue(currentAvg))
                .humanReadableField("rolling_avg_bytes", "rolling_avg", new ByteSizeValue(rollingAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
            out.writeVLong(rollingAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return currentMax == stats.currentMax && currentAvg == stats.currentAvg && rollingAvg == stats.rollingAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentMax, currentAvg, rollingAvg);
        }
    }
}

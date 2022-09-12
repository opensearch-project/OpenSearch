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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * CpuUsageTracker evaluates if the task has consumed too many CPU cycles than allowed.
 */
public class CpuUsageTracker extends ResourceUsageTracker {
    public static final String NAME = "cpu_usage_tracker";

    private final LongSupplier cpuTimeNanosThresholdSupplier;

    public CpuUsageTracker(LongSupplier cpuTimeNanosThresholdSupplier) {
        this.cpuTimeNanosThresholdSupplier = cpuTimeNanosThresholdSupplier;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void update(Task task) {
        // nothing to do
    }

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        if (task.getTotalResourceStats().getCpuTimeInNanos() < cpuTimeNanosThresholdSupplier.getAsLong()) {
            return Optional.empty();
        }

        return Optional.of(new TaskCancellation.Reason(this, "cpu usage exceeded", 1));
    }

    @Override
    public Stats currentStats(List<Task> activeTasks) {
        long currentMax = activeTasks.stream().mapToLong(t -> t.getTotalResourceStats().getCpuTimeInNanos()).max().orElse(0);
        long currentAvg = (long) activeTasks.stream().mapToLong(t -> t.getTotalResourceStats().getCpuTimeInNanos()).average().orElse(0);
        return new Stats(currentMax, currentAvg);
    }

    /**
     * Stats for CpuUsageTracker as seen in "_node/stats/search_backpressure" API.
     */
    public static class Stats implements ResourceUsageTracker.Stats {
        private final long currentMax;
        private final long currentAvg;

        public Stats(long currentMax, long currentAvg) {
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .humanReadableField("current_max_nanos", "current_max", new TimeValue(currentMax))
                .humanReadableField("current_avg_nanos", "current_avg", new TimeValue(currentAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return currentMax == stats.currentMax && currentAvg == stats.currentAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentMax, currentAvg);
        }
    }
}

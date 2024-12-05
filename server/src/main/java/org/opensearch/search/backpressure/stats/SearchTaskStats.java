/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.Version;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Stats related to cancelled SearchTasks.
 */

public class SearchTaskStats implements ToXContentObject, Writeable {
    private final long cancellationCount;
    private final long limitReachedCount;
    private final long completionCount;
    private final Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats;

    public SearchTaskStats(
        long cancellationCount,
        long limitReachedCount,
        long completionCount,
        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats
    ) {
        this.cancellationCount = cancellationCount;
        this.limitReachedCount = limitReachedCount;
        this.completionCount = completionCount;
        this.resourceUsageTrackerStats = resourceUsageTrackerStats;
    }

    public SearchTaskStats(StreamInput in) throws IOException {
        this.cancellationCount = in.readVLong();
        this.limitReachedCount = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.completionCount = in.readVLong();
        } else {
            this.completionCount = -1;
        }

        MapBuilder<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> builder = new MapBuilder<>();
        builder.put(TaskResourceUsageTrackerType.CPU_USAGE_TRACKER, in.readOptionalWriteable(CpuUsageTracker.Stats::new));
        builder.put(TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER, in.readOptionalWriteable(HeapUsageTracker.Stats::new));
        builder.put(TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER, in.readOptionalWriteable(ElapsedTimeTracker.Stats::new));
        this.resourceUsageTrackerStats = builder.immutableMap();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("resource_tracker_stats");
        for (Map.Entry<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> entry : resourceUsageTrackerStats.entrySet()) {
            builder.field(entry.getKey().getName(), entry.getValue());
        }
        builder.endObject();
        if (completionCount != -1) {
            builder.field("completion_count", completionCount);
        }

        builder.startObject("cancellation_stats")
            .field("cancellation_count", cancellationCount)
            .field("cancellation_limit_reached_count", limitReachedCount)
            .endObject();

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(cancellationCount);
        out.writeVLong(limitReachedCount);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeVLong(completionCount);
        }

        out.writeOptionalWriteable(resourceUsageTrackerStats.get(TaskResourceUsageTrackerType.CPU_USAGE_TRACKER));
        out.writeOptionalWriteable(resourceUsageTrackerStats.get(TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER));
        out.writeOptionalWriteable(resourceUsageTrackerStats.get(TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchTaskStats that = (SearchTaskStats) o;
        return cancellationCount == that.cancellationCount
            && limitReachedCount == that.limitReachedCount
            && completionCount == that.completionCount
            && resourceUsageTrackerStats.equals(that.resourceUsageTrackerStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cancellationCount, limitReachedCount, resourceUsageTrackerStats, completionCount);
    }
}

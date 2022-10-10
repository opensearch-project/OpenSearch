/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Stats related to cancelled search shard tasks.
 */
public class SearchShardTaskStats implements ToXContentObject, Writeable {
    private final long cancellationCount;
    private final long limitReachedCount;
    private final CancelledTaskStats lastCancelledTaskStats;
    private final Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats;

    public SearchShardTaskStats(
        long cancellationCount,
        long limitReachedCount,
        CancelledTaskStats lastCancelledTaskStats,
        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats
    ) {
        this.cancellationCount = cancellationCount;
        this.limitReachedCount = limitReachedCount;
        this.lastCancelledTaskStats = lastCancelledTaskStats;
        this.resourceUsageTrackerStats = resourceUsageTrackerStats;
    }

    public SearchShardTaskStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readOptionalWriteable(CancelledTaskStats::new), readResourceUsageTrackerStats(in));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("resource_tracker_stats");
        for (Map.Entry<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> entry : resourceUsageTrackerStats.entrySet()) {
            builder.field(entry.getKey().getName(), entry.getValue());
        }
        builder.endObject();

        builder.startObject("cancellation_stats")
            .field("cancellation_count", cancellationCount)
            .field("cancellation_limit_reached_count", limitReachedCount)
            .field("last_cancelled_task", lastCancelledTaskStats)
            .endObject();

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(cancellationCount);
        out.writeVLong(limitReachedCount);
        out.writeOptionalWriteable(lastCancelledTaskStats);
        out.writeMap(resourceUsageTrackerStats, (o, type) -> o.writeString(type.getName()), (o, stats) -> stats.writeTo(o));
    }

    private static Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> readResourceUsageTrackerStats(StreamInput in)
        throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return Collections.emptyMap();
        }

        MapBuilder<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> builder = new MapBuilder<>();

        for (int i = 0; i < size; i++) {
            TaskResourceUsageTrackerType type = TaskResourceUsageTrackerType.fromName(in.readString());
            TaskResourceUsageTracker.Stats stats;

            switch (type) {
                case CPU_USAGE_TRACKER:
                    stats = new CpuUsageTracker.Stats(in);
                    break;
                case HEAP_USAGE_TRACKER:
                    stats = new HeapUsageTracker.Stats(in);
                    break;
                case ELAPSED_TIME_TRACKER:
                    stats = new ElapsedTimeTracker.Stats(in);
                    break;
                default:
                    throw new IllegalArgumentException("invalid TaskResourceUsageTrackerType: " + type);
            }

            builder.put(type, stats);
        }

        return builder.immutableMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardTaskStats that = (SearchShardTaskStats) o;
        return cancellationCount == that.cancellationCount
            && limitReachedCount == that.limitReachedCount
            && Objects.equals(lastCancelledTaskStats, that.lastCancelledTaskStats)
            && resourceUsageTrackerStats.equals(that.resourceUsageTrackerStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cancellationCount, limitReachedCount, lastCancelledTaskStats, resourceUsageTrackerStats);
    }
}

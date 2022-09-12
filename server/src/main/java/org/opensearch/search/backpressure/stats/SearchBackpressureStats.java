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
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Stats related to search backpressure.
 */
public class SearchBackpressureStats implements ToXContentFragment, Writeable {
    private final Map<String, ResourceUsageTracker.Stats> searchShardTaskCurrentStats;
    private final CancellationStats searchShardTaskCancellationStats;
    private final boolean enabled;
    private final boolean enforced;

    public SearchBackpressureStats(
        Map<String, ResourceUsageTracker.Stats> searchShardTaskCurrentStats,
        CancellationStats searchShardTaskCancellationStats,
        boolean enabled,
        boolean enforced
    ) {
        this.searchShardTaskCurrentStats = searchShardTaskCurrentStats;
        this.searchShardTaskCancellationStats = searchShardTaskCancellationStats;
        this.enabled = enabled;
        this.enforced = enforced;
    }

    public SearchBackpressureStats(StreamInput in) throws IOException {
        this(readStats(in), new CancellationStats(in), in.readBoolean(), in.readBoolean());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject("search_backpressure")
            .startObject("current_stats")
            .field("search_shard_task", searchShardTaskCurrentStats)
            .endObject()
            .startObject("cancellation_stats")
            .field("search_shard_task", searchShardTaskCancellationStats)
            .endObject()
            .field("enabled", enabled)
            .field("enforced", enforced)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(searchShardTaskCurrentStats, StreamOutput::writeString, (o, stats) -> stats.writeTo(o));
        searchShardTaskCancellationStats.writeTo(out);
        out.writeBoolean(enabled);
        out.writeBoolean(enforced);
    }

    private static Map<String, ResourceUsageTracker.Stats> readStats(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return Collections.emptyMap();
        }

        MapBuilder<String, ResourceUsageTracker.Stats> builder = new MapBuilder<>();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            switch (key) {
                case CpuUsageTracker.NAME:
                    builder.put(key, new CpuUsageTracker.Stats(in));
                    break;
                case HeapUsageTracker.NAME:
                    builder.put(key, new HeapUsageTracker.Stats(in));
                    break;
                case ElapsedTimeTracker.NAME:
                    builder.put(key, new ElapsedTimeTracker.Stats(in));
                    break;
                default:
                    throw new IllegalArgumentException("invalid stats type = " + key);
            }
        }

        return builder.immutableMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchBackpressureStats that = (SearchBackpressureStats) o;
        return enabled == that.enabled
            && enforced == that.enforced
            && searchShardTaskCurrentStats.equals(that.searchShardTaskCurrentStats)
            && searchShardTaskCancellationStats.equals(that.searchShardTaskCancellationStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchShardTaskCurrentStats, searchShardTaskCancellationStats, enabled, enforced);
    }
}

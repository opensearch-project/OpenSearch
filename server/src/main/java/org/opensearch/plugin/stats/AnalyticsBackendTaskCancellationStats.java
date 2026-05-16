/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Task cancellation counters from the analytics backend execution layer.
 *
 * <p>Contains 4 counters tracking search tasks and search shard tasks that
 * continue executing in the analytics backend after cancellation.
 */
public class AnalyticsBackendTaskCancellationStats implements Writeable, ToXContentFragment {

    private final long searchTaskCurrent;
    private final long searchTaskTotal;
    private final long searchShardTaskCurrent;
    private final long searchShardTaskTotal;

    /**
     * Construct from individual counter values.
     *
     * @param searchTaskCurrent      current count of search tasks executing post-cancellation
     * @param searchTaskTotal        total count of search tasks that executed post-cancellation
     * @param searchShardTaskCurrent current count of search shard tasks executing post-cancellation
     * @param searchShardTaskTotal   total count of search shard tasks that executed post-cancellation
     */
    public AnalyticsBackendTaskCancellationStats(
        long searchTaskCurrent,
        long searchTaskTotal,
        long searchShardTaskCurrent,
        long searchShardTaskTotal
    ) {
        this.searchTaskCurrent = searchTaskCurrent;
        this.searchTaskTotal = searchTaskTotal;
        this.searchShardTaskCurrent = searchShardTaskCurrent;
        this.searchShardTaskTotal = searchShardTaskTotal;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public AnalyticsBackendTaskCancellationStats(StreamInput in) throws IOException {
        this.searchTaskCurrent = in.readVLong();
        this.searchTaskTotal = in.readVLong();
        this.searchShardTaskCurrent = in.readVLong();
        this.searchShardTaskTotal = in.readVLong();
    }

    public long getSearchTaskCurrent() {
        return searchTaskCurrent;
    }

    public long getSearchTaskTotal() {
        return searchTaskTotal;
    }

    public long getSearchShardTaskCurrent() {
        return searchShardTaskCurrent;
    }

    public long getSearchShardTaskTotal() {
        return searchShardTaskTotal;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(searchTaskCurrent);
        out.writeVLong(searchTaskTotal);
        out.writeVLong(searchShardTaskCurrent);
        out.writeVLong(searchShardTaskTotal);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("analytics_search_task");
        builder.field("current_count_post_cancel", searchTaskCurrent);
        builder.field("total_count_post_cancel", searchTaskTotal);
        builder.endObject();
        builder.startObject("analytics_search_shard_task");
        builder.field("current_count_post_cancel", searchShardTaskCurrent);
        builder.field("total_count_post_cancel", searchShardTaskTotal);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsBackendTaskCancellationStats that = (AnalyticsBackendTaskCancellationStats) o;
        return searchTaskCurrent == that.searchTaskCurrent
            && searchTaskTotal == that.searchTaskTotal
            && searchShardTaskCurrent == that.searchShardTaskCurrent
            && searchShardTaskTotal == that.searchShardTaskTotal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskCurrent, searchTaskTotal, searchShardTaskCurrent, searchShardTaskTotal);
    }
}

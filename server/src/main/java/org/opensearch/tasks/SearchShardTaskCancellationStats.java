/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds monitoring service stats specific to search shard task.
 */
public class SearchShardTaskCancellationStats implements ToXContentObject, Writeable {

    private final long currentLongRunningCancelledTaskCount;
    private final long totalLongRunningCancelledTaskCount;

    public SearchShardTaskCancellationStats(long currentTaskCount, long totalTaskCount) {
        this.currentLongRunningCancelledTaskCount = currentTaskCount;
        this.totalLongRunningCancelledTaskCount = totalTaskCount;
    }

    public SearchShardTaskCancellationStats(StreamInput in) throws IOException {
        this.currentLongRunningCancelledTaskCount = in.readVLong();
        this.totalLongRunningCancelledTaskCount = in.readVLong();
    }

    // package private for testing
    protected long getCurrentLongRunningCancelledTaskCount() {
        return this.currentLongRunningCancelledTaskCount;
    }

    // package private for testing
    protected long getTotalLongRunningCancelledTaskCount() {
        return this.totalLongRunningCancelledTaskCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("current_count_post_cancel", currentLongRunningCancelledTaskCount);
        builder.field("total_count_post_cancel", totalLongRunningCancelledTaskCount);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(currentLongRunningCancelledTaskCount);
        out.writeVLong(totalLongRunningCancelledTaskCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardTaskCancellationStats that = (SearchShardTaskCancellationStats) o;
        return currentLongRunningCancelledTaskCount == that.currentLongRunningCancelledTaskCount
            && totalLongRunningCancelledTaskCount == that.totalLongRunningCancelledTaskCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentLongRunningCancelledTaskCount, totalLongRunningCancelledTaskCount);
    }
}

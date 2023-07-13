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
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds stats related to task cancellation.
 */
public class TaskCancellationStats implements ToXContentFragment, Writeable {

    private final SearchShardTaskCancellationStats searchShardTaskCancellationStats;

    public TaskCancellationStats(SearchShardTaskCancellationStats searchShardTaskCancellationStats) {
        this.searchShardTaskCancellationStats = searchShardTaskCancellationStats;
    }

    public TaskCancellationStats(StreamInput in) throws IOException {
        searchShardTaskCancellationStats = new SearchShardTaskCancellationStats(in);
    }

    // package private for testing
    protected SearchShardTaskCancellationStats getSearchShardTaskCancellationStats() {
        return this.searchShardTaskCancellationStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("task_cancellation");
        builder.field("search_shard_task", searchShardTaskCancellationStats);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchShardTaskCancellationStats.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskCancellationStats that = (TaskCancellationStats) o;
        return Objects.equals(searchShardTaskCancellationStats, that.searchShardTaskCancellationStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchShardTaskCancellationStats);
    }
}

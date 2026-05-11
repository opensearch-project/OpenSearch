/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFusionNativeNodeStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds stats related to task cancellation.
 */
public class TaskCancellationStats implements ToXContentFragment, Writeable {

    private final SearchTaskCancellationStats searchTaskCancellationStats;
    private final SearchShardTaskCancellationStats searchShardTaskCancellationStats;
    @Nullable
    private final DataFusionNativeNodeStats nativeStats;

    /**
     * Backward-compatible constructor without native stats.
     */
    public TaskCancellationStats(
        SearchTaskCancellationStats searchTaskCancellationStats,
        SearchShardTaskCancellationStats searchShardTaskCancellationStats
    ) {
        this(searchTaskCancellationStats, searchShardTaskCancellationStats, null);
    }

    /**
     * Constructor with optional native task cancellation stats.
     *
     * @param searchTaskCancellationStats      search task cancellation stats
     * @param searchShardTaskCancellationStats search shard task cancellation stats
     * @param nativeStats                      native task cancellation stats from DataFusion, or null
     */
    public TaskCancellationStats(
        SearchTaskCancellationStats searchTaskCancellationStats,
        SearchShardTaskCancellationStats searchShardTaskCancellationStats,
        @Nullable DataFusionNativeNodeStats nativeStats
    ) {
        this.searchTaskCancellationStats = searchTaskCancellationStats;
        this.searchShardTaskCancellationStats = searchShardTaskCancellationStats;
        this.nativeStats = nativeStats;
    }

    public TaskCancellationStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            searchTaskCancellationStats = new SearchTaskCancellationStats(in);
        } else {
            searchTaskCancellationStats = new SearchTaskCancellationStats(0, 0);
        }
        searchShardTaskCancellationStats = new SearchShardTaskCancellationStats(in);
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            if (in.readBoolean()) {
                nativeStats = new DataFusionNativeNodeStats(in);
            } else {
                nativeStats = null;
            }
        } else {
            nativeStats = null;
        }
    }

    // package private for testing
    protected SearchShardTaskCancellationStats getSearchShardTaskCancellationStats() {
        return this.searchShardTaskCancellationStats;
    }

    // package private for testing
    protected SearchTaskCancellationStats getSearchTaskCancellationStats() {
        return this.searchTaskCancellationStats;
    }

    // package private for testing
    @Nullable
    protected DataFusionNativeNodeStats getNativeStats() {
        return this.nativeStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("task_cancellation");
        builder.field("search_task", searchTaskCancellationStats);
        builder.field("search_shard_task", searchShardTaskCancellationStats);
        if (nativeStats != null) {
            nativeStats.toXContent(builder, params);
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            searchTaskCancellationStats.writeTo(out);
        }
        searchShardTaskCancellationStats.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeBoolean(nativeStats != null);
            if (nativeStats != null) {
                nativeStats.writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskCancellationStats that = (TaskCancellationStats) o;
        return Objects.equals(searchTaskCancellationStats, that.searchTaskCancellationStats)
            && Objects.equals(searchShardTaskCancellationStats, that.searchShardTaskCancellationStats)
            && Objects.equals(nativeStats, that.nativeStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskCancellationStats, searchShardTaskCancellationStats, nativeStats);
    }
}

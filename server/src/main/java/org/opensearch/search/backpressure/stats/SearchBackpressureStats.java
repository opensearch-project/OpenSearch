/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats related to search backpressure.
 */
public class SearchBackpressureStats implements ToXContentFragment, Writeable {
    private final SearchShardTaskStats searchShardTaskStats;
    private final boolean enabled;
    private final boolean enforced;

    public SearchBackpressureStats(SearchShardTaskStats searchShardTaskStats, boolean enabled, boolean enforced) {
        this.searchShardTaskStats = searchShardTaskStats;
        this.enabled = enabled;
        this.enforced = enforced;
    }

    public SearchBackpressureStats(StreamInput in) throws IOException {
        this(new SearchShardTaskStats(in), in.readBoolean(), in.readBoolean());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject("search_backpressure")
            .field("search_shard_task", searchShardTaskStats)
            .field("enabled", enabled)
            .field("enforced", enforced)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchShardTaskStats.writeTo(out);
        out.writeBoolean(enabled);
        out.writeBoolean(enforced);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchBackpressureStats that = (SearchBackpressureStats) o;
        return enabled == that.enabled && enforced == that.enforced && searchShardTaskStats.equals(that.searchShardTaskStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchShardTaskStats, enabled, enforced);
    }
}

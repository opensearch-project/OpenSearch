/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Node level remote store stats
 * @opensearch.internal
 */
public class RemoteStoreNodeStats implements Writeable, ToXContentFragment {

    public static final String STATS_NAME = "remote_store";
    public static final String LAST_SUCCESSFUL_FETCH_OF_PINNED_TIMESTAMPS = "last_successful_fetch_of_pinned_timestamps";

    /**
     * Time stamp for the last successful fetch of pinned timestamps by the {@linkplain  RemoteStorePinnedTimestampService}
     */
    private final long lastSuccessfulFetchOfPinnedTimestamps;

    public RemoteStoreNodeStats() {
        this.lastSuccessfulFetchOfPinnedTimestamps = RemoteStorePinnedTimestampService.getPinnedTimestamps().v1();
    }

    public long getLastSuccessfulFetchOfPinnedTimestamps() {
        return this.lastSuccessfulFetchOfPinnedTimestamps;
    }

    public RemoteStoreNodeStats(StreamInput in) throws IOException {
        this.lastSuccessfulFetchOfPinnedTimestamps = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.lastSuccessfulFetchOfPinnedTimestamps);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(STATS_NAME);
        builder.field(LAST_SUCCESSFUL_FETCH_OF_PINNED_TIMESTAMPS, this.lastSuccessfulFetchOfPinnedTimestamps);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "RemoteStoreNodeStats{ lastSuccessfulFetchOfPinnedTimestamps=" + lastSuccessfulFetchOfPinnedTimestamps + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != RemoteStoreNodeStats.class) {
            return false;
        }
        RemoteStoreNodeStats other = (RemoteStoreNodeStats) o;
        return this.lastSuccessfulFetchOfPinnedTimestamps == other.lastSuccessfulFetchOfPinnedTimestamps;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastSuccessfulFetchOfPinnedTimestamps);
    }
}
